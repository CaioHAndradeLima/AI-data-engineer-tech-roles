import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
POSITIONS_PATH = PROJECT_ROOT / "positions.json"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env.dev"


@dataclass
class RoleRecord:
    position_id: str
    role_name: str
    start_date: str
    end_date: str
    total_positions: int
    keywords: Dict[str, int]
    keyword_details: Dict[str, Dict[str, Any]]
    note: str
    s3_key: str

    @property
    def month_label(self) -> str:
        return self.start_date[:7]


def load_runtime_env() -> None:
    env_path = os.getenv("STREAMLIT_ENV_FILE", str(DEFAULT_ENV_PATH))
    if Path(env_path).exists():
        load_dotenv(env_path, override=False)

    # Streamlit Community Cloud secrets support.
    try:
        secret_mappings = {
            "AWS_ACCESS_KEY_ID": st.secrets.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": st.secrets.get("AWS_SECRET_ACCESS_KEY"),
            "AWS_SESSION_TOKEN": st.secrets.get("AWS_SESSION_TOKEN"),
            "AWS_REGION": st.secrets.get("AWS_REGION"),
            "OUTPUT_BUCKET": st.secrets.get("OUTPUT_BUCKET"),
        }
    except Exception:  # noqa: BLE001
        secret_mappings = {}

    for key, value in secret_mappings.items():
        if value and not os.getenv(key):
            os.environ[key] = str(value)


def load_position_groups() -> List[Dict[str, Any]]:
    with open(POSITIONS_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    groups = raw.get("position_groups", [])
    normalized: List[Dict[str, Any]] = []
    for item in groups:
        roles = item.get("role") or []
        if isinstance(roles, str):
            roles = [roles]
        roles = [r.strip() for r in roles if isinstance(r, str) and r.strip()]
        if not roles:
            continue
        normalized.append(
            {
                "id": item.get("id") or roles[0].strip().lower().replace(" ", "_"),
                "name": roles[0],
                "role_aliases": roles,
                "extra_terms": item.get("extra_terms", ""),
            }
        )
    return normalized


def get_s3_client():
    load_runtime_env()
    return boto3.client("s3", region_name=os.getenv("AWS_REGION"))


def list_role_objects(bucket: str, position_id: str) -> List[Dict[str, str]]:
    s3 = get_s3_client()
    prefix = f"roles/{position_id}/"
    paginator = s3.get_paginator("list_objects_v2")

    objects: List[Dict[str, str]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith("result.json"):
                continue
            objects.append(
                {
                    "key": key,
                    "last_modified": item["LastModified"].isoformat(),
                }
            )
    return objects


def _parse_record(position_name: str, position_id: str, key: str, payload: Dict[str, Any]) -> RoleRecord:
    parts = key.split("/")
    start_date = parts[3] if len(parts) > 3 else ""
    end_date = parts[4] if len(parts) > 4 else ""
    raw_keywords = payload.get("keywords") or {}
    keyword_details: Dict[str, Dict[str, Any]] = {}
    keyword_counts: Dict[str, int] = {}

    for keyword, raw_value in raw_keywords.items():
        if isinstance(raw_value, dict):
            roles_found = int(raw_value.get("roles_found", raw_value.get("count", 0)) or 0)
            detail = {
                "roles_found": roles_found,
                "salary_bands": {
                    "0_6000": int(((raw_value.get("salary_bands") or {}).get("0_6000", 0)) or 0),
                    "6000_9000": int(((raw_value.get("salary_bands") or {}).get("6000_9000", 0)) or 0),
                    "9000_15000": int(((raw_value.get("salary_bands") or {}).get("9000_15000", 0)) or 0),
                },
                "average_salary": raw_value.get("average_salary"),
                "stock_options_roles": int(raw_value.get("stock_options_roles", 0) or 0),
            }
        else:
            roles_found = int(raw_value or 0)
            detail = {
                "roles_found": roles_found,
                "salary_bands": {
                    "0_6000": 0,
                    "6000_9000": 0,
                    "9000_15000": 0,
                },
                "average_salary": None,
                "stock_options_roles": 0,
            }

        keyword_counts[str(keyword)] = roles_found
        keyword_details[str(keyword)] = detail

    return RoleRecord(
        position_id=position_id,
        role_name=position_name,
        start_date=start_date,
        end_date=end_date,
        total_positions=int(payload.get("total_positions", 0) or 0),
        keywords=keyword_counts,
        keyword_details=keyword_details,
        note=str(payload.get("note", "")),
        s3_key=key,
    )


def load_role_history(bucket: str, position_id: str, position_name: str) -> List[RoleRecord]:
    s3 = get_s3_client()
    records: List[RoleRecord] = []

    for item in list_role_objects(bucket, position_id):
        key = item["key"]
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            payload = json.loads(response["Body"].read().decode("utf-8"))
            records.append(_parse_record(position_name, position_id, key, payload))
        except (ClientError, BotoCoreError, json.JSONDecodeError, UnicodeDecodeError):
            continue

    records.sort(key=lambda record: record.start_date)
    return records


def build_positions_dataframe(records: List[RoleRecord]) -> pd.DataFrame:
    rows = [
        {
            "month": record.month_label,
            "start_date": record.start_date,
            "end_date": record.end_date,
            "total_positions": record.total_positions,
        }
        for record in records
    ]
    return pd.DataFrame(rows)


def build_keywords_dataframe(records: List[RoleRecord]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in records:
        for keyword, count in record.keywords.items():
            rows.append(
                {
                    "month": record.month_label,
                    "keyword": keyword,
                    "count": count,
                    "share": (count / record.total_positions) if record.total_positions else 0,
                }
            )
    return pd.DataFrame(rows)


def build_keyword_salary_dataframe(records: List[RoleRecord]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in records:
        for keyword, detail in record.keyword_details.items():
            salary_bands = detail.get("salary_bands") or {}
            rows.append(
                {
                    "month": record.month_label,
                    "start_date": record.start_date,
                    "end_date": record.end_date,
                    "keyword": keyword,
                    "roles_found": int(detail.get("roles_found", 0) or 0),
                    "average_salary": (
                        float(detail.get("average_salary"))
                        if detail.get("average_salary") not in (None, "")
                        else None
                    ),
                    "stock_options_roles": int(detail.get("stock_options_roles", 0) or 0),
                    "tier1_roles": int(salary_bands.get("0_6000", 0) or 0),
                    "tier2_roles": int(salary_bands.get("6000_9000", 0) or 0),
                    "tier3_roles": int(salary_bands.get("9000_15000", 0) or 0),
                }
            )
    return pd.DataFrame(rows)


def build_salary_over_time_dataframe(records: List[RoleRecord]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in records:
        weighted_total = 0.0
        weighted_roles = 0
        stock_total = 0

        for detail in record.keyword_details.values():
            avg_salary = detail.get("average_salary")
            roles_found = int(detail.get("roles_found", 0) or 0)
            stock_total += int(detail.get("stock_options_roles", 0) or 0)
            if avg_salary in (None, "") or roles_found <= 0:
                continue
            weighted_total += float(avg_salary) * roles_found
            weighted_roles += roles_found

        rows.append(
            {
                "month": record.month_label,
                "start_date": record.start_date,
                "end_date": record.end_date,
                "average_salary": round(weighted_total / weighted_roles, 2)
                if weighted_roles
                else None,
                "stock_options_roles": stock_total,
            }
        )

    return pd.DataFrame(rows)


def top_keywords_for_record(record: RoleRecord, limit: int = 10) -> pd.DataFrame:
    rows = [
        {"keyword": keyword, "count": count}
        for keyword, count in sorted(
            record.keywords.items(), key=lambda item: item[1], reverse=True
        )[:limit]
    ]
    return pd.DataFrame(rows)


def average_keyword_share(df_keywords: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if df_keywords.empty:
        return pd.DataFrame(columns=["keyword", "avg_share"])

    grouped = (
        df_keywords.groupby("keyword", as_index=False)["share"]
        .mean()
        .rename(columns={"share": "avg_share"})
        .sort_values("avg_share", ascending=False)
        .head(limit)
    )
    return grouped


def load_role_bundle(position_id: str, position_name: str) -> Dict[str, Any]:
    load_runtime_env()
    bucket = os.getenv("OUTPUT_BUCKET")
    if not bucket:
        raise RuntimeError("OUTPUT_BUCKET is not configured. Update .env.dev or env vars.")

    records = load_role_history(bucket=bucket, position_id=position_id, position_name=position_name)
    return {
        "bucket": bucket,
        "records": records,
        "positions_df": build_positions_dataframe(records),
        "keywords_df": build_keywords_dataframe(records),
        "keyword_salary_df": build_keyword_salary_dataframe(records),
        "salary_df": build_salary_over_time_dataframe(records),
    }
