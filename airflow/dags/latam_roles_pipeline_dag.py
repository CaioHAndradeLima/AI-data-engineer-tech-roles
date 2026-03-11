import json
import os
from calendar import monthrange
from datetime import date
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline import result_exists_in_s3, run_pipeline, write_result_to_s3


def _get_required_params_from_conf(**context):
  """
  Read required parameters from dag_run.conf.
  Expected keys (all required):
    - position_name (e.g. "Data Engineer")
    - year (YYYY)
    - month (1..12)
  """
  dag_run = context.get("dag_run")
  conf = getattr(dag_run, "conf", {}) or {}

  position_name = conf.get("position_name")
  year = conf.get("year")
  month = conf.get("month")

  missing = [k for k, v in [
    ("position_name", position_name),
    ("year", year),
    ("month", month),
  ] if not v]

  if missing:
    raise RuntimeError(
      f"Missing required dag_run.conf params: {', '.join(missing)}. "
      "Provide them when triggering the DAG."
    )

  return position_name, year, month


def _normalize_position_id(position_name: str) -> str:
  return position_name.strip().lower().replace(" ", "_")


def _build_month_date_range(year_raw, month_raw):
  try:
    year = int(year_raw)
    month = int(month_raw)
  except Exception as exc:  # noqa: BLE001
    raise RuntimeError("Invalid year/month. Use numeric values like year=2026 month=2.") from exc

  if month < 1 or month > 12:
    raise RuntimeError("Invalid month. Expected 1..12.")
  if year < 2000 or year > 2100:
    raise RuntimeError("Invalid year. Expected range 2000..2100.")

  start = date(year, month, 1)
  last_day = monthrange(year, month)[1]
  end = date(year, month, last_day)
  return start.isoformat(), end.isoformat(), year, month


def _load_position_from_config(position_name: str):
  """
  Load position metadata from positions.json by name (case-insensitive).
  If no exact match is found, fallback to provided name without extra terms.
  """
  # Prefer explicit env var, then container project mount path, then local path.
  positions_config_path = os.environ.get(
    "POSITIONS_CONFIG", "/opt/airflow/project/positions.json"
  )
  with open(positions_config_path, "r", encoding="utf-8") as f:
    config = json.load(f)

  positions = config.get("positions", [])
  if not positions:
    raise RuntimeError(f"No positions found in {positions_config_path}")

  normalized = position_name.strip().lower()
  for item in positions:
    if (item.get("name") or "").strip().lower() == normalized:
      return item

  return {
    "id": _normalize_position_id(position_name),
    "name": position_name,
    "extra_terms": None,
  }


def request_gemini_estimate(**context):
  """
  Step 1:
    Validate the request and call Gemini (no S3 write in this step).
  """
  position_name, year_raw, month_raw = _get_required_params_from_conf(**context)
  start_date, end_date, year, month = _build_month_date_range(year_raw, month_raw)

  today = datetime.now(tz=pendulum.timezone("UTC")).date()
  if year == today.year and month == today.month:
    raise RuntimeError(
      "Current month runs are blocked. Run only after month closes (typically on day 1 for previous month)."
    )

  start_obj = date.fromisoformat(start_date)
  if start_obj > today:
    raise RuntimeError(
      f"Future period is not allowed: start_date={start_date} is after today={today.isoformat()}."
    )

  position = _load_position_from_config(position_name)
  position_id = position.get("id") or _normalize_position_id(position_name)

  name = position.get("name")
  extra_terms = position.get("extra_terms")
  if not name:
    raise RuntimeError(f"Position '{position_id}' missing 'name' in config.")

  already_exists = result_exists_in_s3(
    position_id=position_id,
    start_date=start_date,
    end_date=end_date,
  )
  if already_exists:
    raise RuntimeError(
      "Data already exists in S3 for this period. "
      f"position={position_id}, start_date={start_date}, end_date={end_date}"
    )

  result = run_pipeline(
    position=name,
    start_date=start_date,
    end_date=end_date,
    extra_terms=extra_terms,
    position_id=position_id,
    write_to_s3=False,
    mode="ai_only",
  )

  payload = {
    "position_id": position_id,
    "name": name,
    "start_date": start_date,
    "end_date": end_date,
    "year": year,
    "month": month,
    "result": result,
  }

  return payload


def write_info_to_s3(**context):
  """
  Step 2:
    Persist estimate JSON to S3. This step is strict and fails DAG on any S3 error.
  """
  ti = context["ti"]
  payload = ti.xcom_pull(
    task_ids="request_gemini_estimate", key="return_value"
  ) or {}

  result = payload.get("result")
  position_id = payload.get("position_id")
  start_date = payload.get("start_date")
  end_date = payload.get("end_date")

  if not isinstance(result, dict):
    raise RuntimeError("Missing Gemini result from previous task.")
  if not position_id or not start_date or not end_date:
    raise RuntimeError("Missing metadata required for S3 write.")

  s3_key = write_result_to_s3(
    result=result,
    position_id=position_id,
    start_date=start_date,
    end_date=end_date,
    fail_on_error=True,
  )
  if not s3_key:
    raise RuntimeError("S3 write returned no object key.")

  result.setdefault("storage", {})
  result["storage"]["s3_bucket"] = os.environ.get("OUTPUT_BUCKET")
  result["storage"]["s3_key"] = s3_key
  payload["result"] = result

  return payload


def slack_notification_stub(**context):
  """
  Step 3:
    Placeholder for Slack notification. For now, just logs the final summary.
  """
  ti = context["ti"]
  payload = ti.xcom_pull(task_ids="write_info_to_s3", key="return_value") or {}
  result = payload.get("result") or {}

  total = int(result.get("total_positions", 0) or 0)
  position_id = payload.get("position_id", "unknown")
  name = payload.get("name", "unknown")
  start_date = payload.get("start_date", "unknown")
  end_date = payload.get("end_date", "unknown")
  year = payload.get("year", "unknown")
  month = payload.get("month", "unknown")
  storage = result.get("storage") or {}

  dag_id = context["dag"].dag_id
  run_id = context["run_id"]

  print(
    f":white_check_mark: Airflow DAG `{dag_id}` succeeded.\n"
    f"- Run ID: `{run_id}`\n"
    f"- Position: `{position_id}` ({name})\n"
    f"- Requested month: {year}-{str(month).zfill(2)}\n"
    f"- Period: {start_date} to {end_date}\n"
    f"- Total positions found: *{total}*\n"
    f"- S3: s3://{storage.get('s3_bucket', 'unknown')}/{storage.get('s3_key', 'unknown')}"
  )


def slack_failure_notification(context):
  dag_id = context["dag"].dag_id
  run_id = context["run_id"]
  task_id = context.get("task_instance").task_id
  try:
    position_name, year, month = _get_required_params_from_conf(**context)
  except Exception:
    position_name = year = month = "unknown"

  print(
    f":x: Airflow DAG `{dag_id}` failed.\n"
    f"- Run ID: `{run_id}`\n"
    f"- Task: `{task_id}`\n"
    f"- Position: `{position_name}`\n"
    f"- Requested month: {year}-{month}"
  )


default_args = {
  "owner": "data-engineering",
  "depends_on_past": False,
  "retries": 0,
  "on_failure_callback": slack_failure_notification,
}

with DAG(
  dag_id="latam_roles_pipeline",
  default_args=default_args,
  description="On-demand LATAM roles AI estimate pipeline (Gemini) writing to S3",
  schedule_interval=None,  # run on demand via trigger only
  start_date=datetime(2025, 1, 1, tzinfo=pendulum.timezone("UTC")),
  catchup=False,
  tags=["latam", "jobs", "gemini"],
) as dag:
  request_gemini = PythonOperator(
    task_id="request_gemini_estimate",
    python_callable=request_gemini_estimate,
    provide_context=True,
  )

  write_s3 = PythonOperator(
    task_id="write_info_to_s3",
    python_callable=write_info_to_s3,
    provide_context=True,
  )

  slack_stub = PythonOperator(
    task_id="slack_notification_stub",
    python_callable=slack_notification_stub,
    provide_context=True,
  )

  request_gemini >> write_s3 >> slack_stub
