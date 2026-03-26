import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
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
    note: str
    s3_key: str

    @property
    def month_label(self) -> str:
        return self.start_date[:7]


def load_runtime_env() -> None:
    env_path = os.getenv("STREAMLIT_ENV_FILE", str(DEFAULT_ENV_PATH))
    if Path(env_path).exists():
        load_dotenv(env_path, override=False)


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
    return RoleRecord(
        position_id=position_id,
        role_name=position_name,
        start_date=start_date,
        end_date=end_date,
        total_positions=int(payload.get("total_positions", 0) or 0),
        keywords={
            str(keyword): int(value or 0)
            for keyword, value in (payload.get("keywords") or {}).items()
        },
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
    }

