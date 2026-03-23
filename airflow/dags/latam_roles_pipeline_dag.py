import os
from calendar import monthrange
from datetime import date, datetime
from typing import Any, Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from pipeline import (
    load_position_groups,
    normalize_position_id,
    result_exists_in_s3,
    run_pipeline,
    write_result_to_s3,
)


def _build_month_date_range(year_raw: Any, month_raw: Any):
    try:
        year = int(year_raw)
        month = int(month_raw)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Invalid year/month. Use numeric values like year=2026 month=2."
        ) from exc

    if month < 1 or month > 12:
        raise RuntimeError("Invalid month. Expected 1..12.")
    if year < 2000 or year > 2100:
        raise RuntimeError("Invalid year. Expected range 2000..2100.")

    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    return start.isoformat(), end.isoformat(), year, month


def _resolve_period(year_raw: Any, month_raw: Any):
    today = datetime.now(tz=pendulum.timezone("UTC")).date()
    if year_raw and month_raw:
        return _build_month_date_range(year_raw, month_raw)

    if year_raw or month_raw:
        raise RuntimeError("Provide both year and month, or provide neither.")

    # Automatic mode: previous full month.
    if today.month == 1:
        year = today.year - 1
        month = 12
    else:
        year = today.year
        month = today.month - 1
    return _build_month_date_range(year, month)


def _load_roles_from_config(position_name: str | None) -> List[Dict[str, Any]]:
    positions_config_path = os.environ.get(
        "POSITIONS_CONFIG", "/opt/airflow/project/positions.json"
    )
    roles = load_position_groups(positions_config_path)
    if not roles:
        raise RuntimeError(f"No positions found in {positions_config_path}")

    if not position_name:
        return roles

    normalized = position_name.strip().lower()
    selected = []
    for item in roles:
        aliases = item.get("role_aliases") or [item.get("name")]
        aliases_normalized = [str(a).strip().lower() for a in aliases if a]
        if normalized in aliases_normalized:
            selected.append(item)

    if not selected:
        raise RuntimeError(
            f"Position '{position_name}' not found in positions config aliases."
        )
    return selected


def _run_gemini_with_retries(
    *,
    name: str,
    position_id: str,
    start_date: str,
    end_date: str,
    extra_terms: str | None,
    role_aliases: List[str],
    retries: int = 2,
):
    attempts = retries + 1
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return run_pipeline(
                position=name,
                start_date=start_date,
                end_date=end_date,
                extra_terms=extra_terms,
                role_aliases=role_aliases,
                position_id=position_id,
                write_to_s3=False,
                mode="ai_only",
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(
                f"Gemini attempt {attempt}/{attempts} failed "
                f"for position_id={position_id}: {exc}"
            )
    raise RuntimeError(
        f"Gemini run failed after {attempts} attempts for position_id={position_id}: {last_error}"
    )


@dag(
    dag_id="latam_roles_pipeline",
    description="Simple master + mapped-role pipeline for LATAM role estimates",
    schedule="0 3 1 * *",  # day 1 of each month at 03:00 UTC
    start_date=datetime(2025, 1, 1, tzinfo=pendulum.timezone("UTC")),
    catchup=False,
    tags=["latam", "jobs", "gemini", "mapped"],
)
def latam_roles_pipeline():
    @task
    def validate_and_prepare() -> Dict[str, Any]:
        """
        Validation task:
        - reads dag_run.conf (position_name/year/month optional)
        - validates month/year rules
        - reads positions config and returns array of roles to process
        """
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", {}) or {}

        position_name = conf.get("position_name")
        year_raw = conf.get("year")
        month_raw = conf.get("month")

        start_date, end_date, year, month = _resolve_period(year_raw, month_raw)
        today = datetime.now(tz=pendulum.timezone("UTC")).date()

        if year == today.year and month == today.month:
            raise RuntimeError(
                "Current month runs are blocked. Run after month closes "
                "(typically day 1 for previous month)."
            )
        if date.fromisoformat(start_date) > today:
            raise RuntimeError(
                f"Future period is not allowed: start_date={start_date}, today={today.isoformat()}."
            )

        roles = _load_roles_from_config(position_name)
        if not roles:
            raise RuntimeError("No roles selected for processing.")

        run_meta = {
            "start_date": start_date,
            "end_date": end_date,
            "year": year,
            "month": month,
        }
        return {"run_meta": run_meta, "roles": roles}

    @task
    def extract_roles(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        return payload["roles"]

    @task
    def extract_run_meta(payload: Dict[str, Any]) -> Dict[str, Any]:
        return payload["run_meta"]

    @task(max_active_tis_per_dag=1)
    def process_role(
        role_item: Dict[str, Any], run_meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        One mapped task per role item.
        Fails this mapped task on Gemini or S3 errors, with contextual logs.
        """
        position_id = role_item.get("id") or normalize_position_id(
            role_item.get("name", "unknown")
        )
        name = role_item.get("name")
        role_aliases = role_item.get("role_aliases") or [name]
        extra_terms = role_item.get("extra_terms")
        start_date = run_meta["start_date"]
        end_date = run_meta["end_date"]

        if not name:
            raise RuntimeError(
                f"[validation] Missing role name in config. "
                f"position_id={position_id}, start_date={start_date}, end_date={end_date}"
            )

        if result_exists_in_s3(
            position_id=position_id,
            start_date=start_date,
            end_date=end_date,
        ):
            raise RuntimeError(
                f"[precheck] Data already exists in S3 for this period. "
                f"position_id={position_id}, name={name}, start_date={start_date}, end_date={end_date}"
            )

        try:
            result = _run_gemini_with_retries(
                name=name,
                position_id=position_id,
                start_date=start_date,
                end_date=end_date,
                extra_terms=extra_terms,
                role_aliases=role_aliases,
                retries=2,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"[gemini] Failed Gemini processing. "
                f"position_id={position_id}, name={name}, aliases={role_aliases}, "
                f"start_date={start_date}, end_date={end_date}, error={exc}"
            ) from exc

        try:
            s3_key = write_result_to_s3(
                result=result,
                position_id=position_id,
                start_date=start_date,
                end_date=end_date,
                fail_on_error=True,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"[s3] Failed S3 write. "
                f"position_id={position_id}, name={name}, bucket={os.environ.get('OUTPUT_BUCKET')}, "
                f"start_date={start_date}, end_date={end_date}, error={exc}"
            ) from exc

        result.setdefault("storage", {})
        result["storage"]["s3_bucket"] = os.environ.get("OUTPUT_BUCKET")
        result["storage"]["s3_key"] = s3_key

        return {
            "status": "success",
            "stage": "done",
            "position_id": position_id,
            "name": name,
            "result": result,
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify_and_finalize(
        role_results: List[Dict[str, Any]], run_meta: Dict[str, Any]
    ) -> None:
        """
        Notification/summary task:
        - prints success/failure per role
        - fails DAG at the end if any failed
        """
        results = role_results or []
        successes = [r for r in results if isinstance(r, dict) and r.get("status") == "success"]
        failures = [r for r in results if not isinstance(r, dict) or r.get("status") != "success"]

        total_positions = sum(
            int(((r.get("result") or {}).get("total_positions", 0) or 0))
            for r in successes
        )

        print(
            "Run summary:\n"
            f"- Requested month: {run_meta.get('year')}-{str(run_meta.get('month')).zfill(2)}\n"
            f"- Period: {run_meta.get('start_date')} to {run_meta.get('end_date')}\n"
            f"- Success count: {len(successes)}\n"
            f"- Failure count: {len(failures)}\n"
            f"- Total positions found (sum): {total_positions}"
        )

        for item in successes:
            print(
                f"SUCCESS position_id={item.get('position_id')} "
                f"total_positions={(item.get('result') or {}).get('total_positions')}"
            )

        for item in failures:
            print(
                f"FAILURE position_id={item.get('position_id')} "
                f"stage={item.get('stage')} error={item.get('error')}"
            )

        if failures:
            raise RuntimeError(
                f"Run finished with failures ({len(failures)}). "
                "All mapped roles were attempted."
            )

    payload = validate_and_prepare()
    roles = extract_roles(payload)
    run_meta = extract_run_meta(payload)
    role_results = process_role.partial(run_meta=run_meta).expand(role_item=roles)
    notify_and_finalize(role_results, run_meta)


dag = latam_roles_pipeline()
