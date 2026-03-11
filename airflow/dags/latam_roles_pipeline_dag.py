import json
import os
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline import run_pipeline, write_result_to_s3


def _get_required_params_from_conf(**context):
  """
  Read required parameters from dag_run.conf.
  Expected keys (all required):
    - start_date (YYYY-MM-DD)
    - end_date (YYYY-MM-DD)
    - position_id (must exist in positions.json)
  """
  dag_run = context.get("dag_run")
  conf = getattr(dag_run, "conf", {}) or {}

  start_date = conf.get("start_date")
  end_date = conf.get("end_date")
  position_id = conf.get("position_id")

  missing = [k for k, v in [
    ("start_date", start_date),
    ("end_date", end_date),
    ("position_id", position_id),
  ] if not v]

  if missing:
    raise RuntimeError(
      f"Missing required dag_run.conf params: {', '.join(missing)}. "
      "Provide them when triggering the DAG."
    )

  return start_date, end_date, position_id


def _load_position_from_config(position_id: str):
  """
  Load a single position definition from positions.json by id.
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

  for item in positions:
    if item.get("id") == position_id:
      return item

  raise RuntimeError(
    f"Position id '{position_id}' not found in {positions_config_path}"
  )


def request_gemini_estimate(**context):
  """
  Step 1:
    Request an AI estimate from Gemini (no S3 write in this step).
  """
  start_date, end_date, position_id = _get_required_params_from_conf(**context)
  position = _load_position_from_config(position_id)

  name = position.get("name")
  extra_terms = position.get("extra_terms")
  if not name:
    raise RuntimeError(f"Position '{position_id}' missing 'name' in config.")

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
  storage = result.get("storage") or {}

  dag_id = context["dag"].dag_id
  run_id = context["run_id"]

  print(
    f":white_check_mark: Airflow DAG `{dag_id}` succeeded.\n"
    f"- Run ID: `{run_id}`\n"
    f"- Position: `{position_id}` ({name})\n"
    f"- Period: {start_date} to {end_date}\n"
    f"- Total positions found: *{total}*\n"
    f"- S3: s3://{storage.get('s3_bucket', 'unknown')}/{storage.get('s3_key', 'unknown')}"
  )


def slack_failure_notification(context):
  dag_id = context["dag"].dag_id
  run_id = context["run_id"]
  task_id = context.get("task_instance").task_id
  try:
    start_date, end_date, position_id = _get_required_params_from_conf(**context)
  except Exception:
    start_date = end_date = position_id = "unknown"

  print(
    f":x: Airflow DAG `{dag_id}` failed.\n"
    f"- Run ID: `{run_id}`\n"
    f"- Task: `{task_id}`\n"
    f"- Position: `{position_id}`\n"
    f"- Period: {start_date} to {end_date}"
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
