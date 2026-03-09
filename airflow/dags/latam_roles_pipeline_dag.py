import json
import os
from datetime import datetime

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline import run_pipeline


def _compute_dates_from_conf_or_previous_month(**context):
  """
  Determine start/end date for the run.
  Priority:
  1) dag_run.conf["start_date"] / ["end_date"] if provided (YYYY-MM-DD)
  2) Previous full calendar month based on execution date.
  """
  dag_run = context.get("dag_run")
  if dag_run and dag_run.conf:
    start = dag_run.conf.get("start_date")
    end = dag_run.conf.get("end_date")
    if start and end:
      return start, end

  # Default: previous full calendar month
  exec_date = context["logical_date"]  # pendulum DateTime
  first_of_current_month = exec_date.replace(day=1)
  last_month_end = first_of_current_month - pendulum.duration(days=1)
  last_month_start = last_month_end.replace(day=1)

  return last_month_start.to_date_string(), last_month_end.to_date_string()


def run_latam_roles_pipeline(**context):
  start_date, end_date = _compute_dates_from_conf_or_previous_month(**context)

  positions_config_path = os.environ.get("POSITIONS_CONFIG", "positions.json")
  with open(positions_config_path, "r", encoding="utf-8") as f:
    config = json.load(f)

  positions = config.get("positions", [])
  if not positions:
    raise RuntimeError(f"No positions found in {positions_config_path}")

  results_summary = {"positions": [], "total_positions": 0}

  for item in positions:
    position_id = item.get("id")
    name = item.get("name")
    extra_terms = item.get("extra_terms")
    if not position_id or not name:
      continue

    result = run_pipeline(
      position=name,
      start_date=start_date,
      end_date=end_date,
      extra_terms=extra_terms,
      position_id=position_id,
      write_to_s3=True,
    )

    total = int(result.get("total_positions", 0) or 0)
    results_summary["positions"].append(
      {
        "id": position_id,
        "name": name,
        "total_positions": total,
        "storage": result.get("storage"),
      }
    )
    results_summary["total_positions"] += total

  # Push summary to XCom for Slack notifications
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

  total = 0
  if isinstance(results_summary, dict):
    total = int(results_summary.get("total_positions", 0) or 0)

  dag_id = context["dag"].dag_id
  run_id = context["run_id"]
  start_date, end_date = _compute_dates_from_conf_or_previous_month(**context)

  message = (
    f":white_check_mark: Airflow DAG `{dag_id}` succeeded.\n"
    f"- Run ID: `{run_id}`\n"
    f"- Period: {start_date} to {end_date}\n"
    f"- Total positions found: *{total}*"
  )
  _post_to_slack(message)


def slack_failure_notification(context):
  dag_id = context["dag"].dag_id
  run_id = context["run_id"]
  task_id = context.get("task_instance").task_id
  start_date, end_date = _compute_dates_from_conf_or_previous_month(**context)

  message = (
    f":x: Airflow DAG `{dag_id}` failed.\n"
    f"- Run ID: `{run_id}`\n"
    f"- Task: `{task_id}`\n"
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
  description="Collect LATAM roles data via Google + Gemini, monthly, and store in S3",
  schedule_interval="0 3 1 * *",  # 03:00 on the 1st of each month
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

