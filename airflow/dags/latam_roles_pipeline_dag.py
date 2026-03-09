import json
import os
from datetime import datetime

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline import run_pipeline


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
  positions_config_path = os.environ.get("POSITIONS_CONFIG", "positions.json")
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


def run_latam_roles_pipeline(**context):
  """
  Step 1–3 combined for now:
    1) Read start/end dates and position_id from dag_run.conf.
    2) Load the position metadata from positions.json.
    3) Run the AI-only pipeline for that position and write to S3.
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
    write_to_s3=True,
    mode="ai_only",
  )

  total = int(result.get("total_positions", 0) or 0)
  keywords = result.get("keywords") or {}

  results_summary = {
    "position_id": position_id,
    "name": name,
    "start_date": start_date,
    "end_date": end_date,
    "total_positions": total,
    "keywords": keywords,
    "storage": result.get("storage"),
  }

  # Push summary to XCom for Slack notifications or downstream analytics steps.
  ti = context["ti"]
  ti.xcom_push(key="results_summary", value=results_summary)


def _post_to_slack(message: str) -> None:
  webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
  if not webhook_url:
    # Slack not configured; just log.
    print("SLACK_WEBHOOK_URL not set; skipping Slack notification.")
    print(message)
    return

  payload = {"text": message}
  resp = requests.post(webhook_url, json=payload, timeout=10)
  try:
    resp.raise_for_status()
  except Exception as exc:  # noqa: BLE001
    print(f"Failed to send Slack notification: {exc} - response={resp.text}")


def slack_success_notification(**context):
  ti = context["ti"]
  results_summary = ti.xcom_pull(
    key="results_summary", task_ids="run_latam_roles_pipeline"
  )
  if not isinstance(results_summary, dict):
    results_summary = {}

  total = int(results_summary.get("total_positions", 0) or 0)
  position_id = results_summary.get("position_id", "unknown")
  name = results_summary.get("name", "unknown")

  dag_id = context["dag"].dag_id
  run_id = context["run_id"]
  start_date, end_date, _ = _get_required_params_from_conf(**context)

  message = (
    f":white_check_mark: Airflow DAG `{dag_id}` succeeded.\n"
    f"- Run ID: `{run_id}`\n"
    f"- Position: `{position_id}` ({name})\n"
    f"- Period: {start_date} to {end_date}\n"
    f"- Total positions found: *{total}*"
  )
  _post_to_slack(message)


def slack_failure_notification(context):
  dag_id = context["dag"].dag_id
  run_id = context["run_id"]
  task_id = context.get("task_instance").task_id
  try:
    start_date, end_date, position_id = _get_required_params_from_conf(**context)
  except Exception:
    start_date = end_date = position_id = "unknown"

  message = (
    f":x: Airflow DAG `{dag_id}` failed.\n"
    f"- Run ID: `{run_id}`\n"
    f"- Task: `{task_id}`\n"
    f"- Position: `{position_id}`\n"
    f"- Period: {start_date} to {end_date}"
  )
  _post_to_slack(message)


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
  run_task = PythonOperator(
    task_id="run_latam_roles_pipeline",
    python_callable=run_latam_roles_pipeline,
    provide_context=True,
  )

  slack_success = PythonOperator(
    task_id="slack_success_notification",
    python_callable=slack_success_notification,
    provide_context=True,
  )

  run_task >> slack_success

