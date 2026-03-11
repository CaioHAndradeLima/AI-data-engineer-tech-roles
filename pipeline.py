import argparse
import json
import os
from datetime import date
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types
import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


GOOGLE_CSE_URL = "https://www.googleapis.com/customsearch/v1"

GENAI_CLIENT: Optional[genai.Client] = None


def normalize_position_id(position_name: str) -> str:
    return position_name.strip().lower().replace(" ", "_")


def load_position_groups(positions_config_path: str) -> List[Dict[str, Any]]:
    """
    Load position groups from config file.

    Supported formats:
    1) New:
       {"position_groups":[{"id":"data_engineer","role":["Data Engineer","Senior Data Engineer"]}]}
    2) Legacy:
       {"positions":[{"id":"data_engineer","name":"Data Engineer","extra_terms":"..."}]}
    """
    with open(positions_config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    groups = config.get("position_groups")
    if isinstance(groups, list) and groups:
        normalized: List[Dict[str, Any]] = []
        for item in groups:
            roles = item.get("role") or []
            if isinstance(roles, str):
                roles = [roles]
            roles = [r.strip() for r in roles if isinstance(r, str) and r.strip()]
            if not roles:
                continue

            group_id = item.get("id") or normalize_position_id(roles[0])
            normalized.append(
                {
                    "id": group_id,
                    "name": roles[0],
                    "role_aliases": roles,
                    "extra_terms": item.get("extra_terms"),
                }
            )
        return normalized

    legacy_positions = config.get("positions", [])
    normalized_legacy: List[Dict[str, Any]] = []
    for item in legacy_positions:
        position_id = item.get("id")
        name = item.get("name")
        if not position_id or not name:
            continue
        normalized_legacy.append(
            {
                "id": position_id,
                "name": name,
                "role_aliases": [name],
                "extra_terms": item.get("extra_terms"),
            }
        )
    return normalized_legacy


def build_result_s3_key(position_id: str, start_date: str, end_date: str) -> str:
    """
    Build a deterministic S3 key for a monthly run.
    Layout: roles/<position>/<year>/<start_date>/<end_date>/result.json
    """
    year = start_date[:4]
    return f"roles/{position_id}/{year}/{start_date}/{end_date}/result.json"


def result_exists_in_s3(
    position_id: str,
    start_date: str,
    end_date: str,
    bucket: Optional[str] = None,
) -> bool:
    """
    Check whether the monthly result object already exists in S3.
    """
    target_bucket = bucket or os.getenv("OUTPUT_BUCKET")
    if not target_bucket:
        raise RuntimeError("OUTPUT_BUCKET is not configured.")

    session = boto3.Session()
    s3 = session.client("s3")
    key = build_result_s3_key(position_id, start_date, end_date)

    try:
        s3.head_object(Bucket=target_bucket, Key=key)
        return True
    except ClientError as exc:
        error_code = (exc.response or {}).get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def load_config() -> None:
    """
    Load environment variables from .env if present and configure Gemini.
    """
    global GENAI_CLIENT

    load_dotenv()

    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is not set.")

    # Initialize a Google GenAI client (Gemini API).
    GENAI_CLIENT = genai.Client(api_key=gemini_api_key)


def google_search(query: str, max_results: int = 10) -> List[Dict[str, Any]]:
    """
    Call Google Custom Search API and return a list of result items.
    """
    api_key = os.getenv("GOOGLE_CSE_API_KEY")
    cse_id = os.getenv("GOOGLE_CSE_ID")

    if not api_key or not cse_id:
        raise RuntimeError(
            "GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID must be set to use Google search."
        )

    params = {
        "key": api_key,
        "cx": cse_id,
        "q": query,
        "num": max(1, min(max_results, 10)),  # API limit per call
    }

    items: List[Dict[str, Any]] = []
    fetched = 0

    while fetched < max_results:
        remaining = max_results - fetched
        params["num"] = max(1, min(remaining, 10))
        params["start"] = fetched + 1

        resp = requests.get(GOOGLE_CSE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("items", [])
        if not batch:
            break

        items.extend(batch)
        fetched += len(batch)

        if len(batch) < params["num"]:
            break

    return items


def build_latam_query(position: str, extra_terms: Optional[str] = None) -> str:
    """
    Build a Google search query focused on LATAM roles for the given position.
    """
    base = f'"{position}" job LATAM'
    if extra_terms:
        base += f" {extra_terms}"
    return base


def ask_gemini_to_count_positions(
    position: str,
    start_date: str,
    end_date: str,
    search_results: List[Dict[str, Any]],
    model_name: str = "gemini-3-flash-preview",
) -> Dict[str, Any]:
    """
    Send search results and parameters to Gemini and ask for a JSON summary.
    """
    if GENAI_CLIENT is None:
        raise RuntimeError("Gemini client is not initialized. Call load_config() first.")

    # Keep only the most relevant fields to reduce token usage.
    condensed_results = [
        {
            "title": item.get("title"),
            "link": item.get("link"),
            "snippet": item.get("snippet"),
        }
        for item in search_results
    ]

    system_instruction = (
        "You are an analyst that extracts job information from web search results. "
        "You receive a list of Google search results about job postings. "
        "You must identify job postings that match the requested position and "
        "are located in LATAM (Latin America). You must also consider only "
        "postings that are within the given date range."
    )

    user_instruction = f"""
Position: {position}
Region: LATAM (Latin America)
Start date: {start_date}
End date: {end_date}

Search results (JSON list of objects with title, link, snippet):
{json.dumps(condensed_results, ensure_ascii=False, indent=2)}

Task:
1. Identify which results look like actual job postings.
2. Among those, keep only postings that:
   - Are for the requested position (or very close variants),
   - Are based in LATAM,
   - Appear to be active or posted within the given date range.
3. Return ONLY a JSON object with this structure (no extra text):
{{
  "total_positions": <int>,
  "positions": [
    {{
      "title": "<job title>",
      "company": "<company or source if known>",
      "location": "<location if known>",
      "url": "<job url>",
      "source": "<'google_result' or similar>",
      "notes": "<short reasoning or summary>",
      "confidence": "<high|medium|low>"
    }}
  ]
}}
"""

    prompt = system_instruction + "\n\n" + user_instruction

    config = types.GenerateContentConfig(response_mime_type="application/json")
    response = GENAI_CLIENT.models.generate_content(
        model=model_name,
        contents=prompt,
        config=config,
    )

    raw_text = response.text or ""

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        # Fallback: try to locate JSON substring.
        start = raw_text.find("{")
        end = raw_text.rfind("}") + 1
        if start == -1 or end <= start:
            raise ValueError(f"Gemini response is not valid JSON: {raw_text}")
        parsed = json.loads(raw_text[start:end])

    if not isinstance(parsed, dict) or "total_positions" not in parsed:
        raise ValueError(f"Gemini JSON missing 'total_positions': {parsed}")

    return parsed


def ask_gemini_to_estimate_positions(
    position: str,
    start_date: str,
    end_date: str,
    role_aliases: Optional[List[str]] = None,
    model_name: str = "gemini-3-flash-preview",
) -> Dict[str, Any]:
    """
    Ask Gemini directly to estimate the number of positions, without
    using an external search API. This is an AI-only, approximate view.
    """
    if GENAI_CLIENT is None:
        raise RuntimeError("Gemini client is not initialized. Call load_config() first.")

    system_instruction = (
        "You are a labor-market analyst. "
        "Given a role, region and date range, you estimate how many job "
        "openings likely existed in that period, based on your knowledge "
        "of global and regional hiring patterns. "
        "Your answer is an estimate, not an exact measurement."
    )

    aliases = role_aliases or [position]
    aliases_text = ", ".join(aliases)

    user_instruction = f"""
Role: {position}
Role aliases (same family, include all in estimate): {aliases_text}
Region: LATAM (Latin America)
Start date: {start_date}
End date: {end_date}

Task:
1. Provide your best-guess estimate of how many open positions existed for
   this role in this region and period. The number must be a single integer.
2. Consider all role aliases above as equivalent targets of the same role family.
3. Based on your understanding of typical job descriptions for this role in
   LATAM, build an approximate frequency table of the most relevant tools,
   technologies, or skills (e.g. Python, SQL, Snowflake, Terraform, dbt,
   Airflow, etc.) that would appear in those postings.
4. Return ONLY a JSON object with this structure (no extra text):
{{
  "total_positions": <int>,  // your estimated count of positions
  "keywords": {{
    "<keyword>": <int>,      // approximate number of positions where this keyword appears
    "...": <int>
  }}
}}
"""

    prompt = system_instruction + "\n\n" + user_instruction

    config = types.GenerateContentConfig(response_mime_type="application/json")
    response = GENAI_CLIENT.models.generate_content(
        model=model_name,
        contents=prompt,
        config=config,
    )

    raw_text = response.text or ""

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("{")
        end = raw_text.rfind("}") + 1
        if start == -1 or end <= start:
            raise ValueError(f"Gemini response is not valid JSON: {raw_text}")
        parsed = json.loads(raw_text[start:end])

    if not isinstance(parsed, dict) or "total_positions" not in parsed:
        raise ValueError(f"Gemini JSON missing 'total_positions': {parsed}")

    parsed.setdefault(
        "note",
        "AI-only estimate generated directly by Gemini, "
        "not based on live job board scraping.",
    )
    return parsed


def write_result_to_s3(
    result: Dict[str, Any],
    position_id: str,
    start_date: str,
    end_date: str,
    fail_on_error: bool = False,
) -> Optional[str]:
    """
    Optionally write the result JSON to S3 if OUTPUT_BUCKET is configured.
    Returns the S3 key if written, or None otherwise.
    """
    bucket = os.getenv("OUTPUT_BUCKET")
    if not bucket:
        if fail_on_error:
            raise RuntimeError("OUTPUT_BUCKET is not configured.")
        return None

    session = boto3.Session()
    s3 = session.client("s3")

    key = build_result_s3_key(position_id, start_date, end_date)

    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(result, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as exc:
        if fail_on_error:
            raise RuntimeError(f"Failed to write result to S3: {exc}") from exc
        # Non-strict mode: keep backward-compatible behavior.
        print(f"Failed to write result to S3: {exc}")
        return None

    return key


def run_pipeline(
    position: str,
    start_date: str,
    end_date: str,
    max_results: int = 20,
    extra_terms: Optional[str] = None,
    role_aliases: Optional[List[str]] = None,
    position_id: Optional[str] = None,
    write_to_s3: bool = True,
    mode: str = "ai_only",
) -> Dict[str, Any]:
    """
    Main function to:
    - Either:
      - Build a Google query, fetch search results and ask Gemini to
        summarize and count positions (mode='search'), or
      - Ask Gemini directly for an AI-only estimate (mode='ai_only').
    - Optionally write results to S3
    """
    load_config()

    if mode == "search":
        query = build_latam_query(position, extra_terms=extra_terms)
        search_results = google_search(query, max_results=max_results)

        if not search_results:
            result: Dict[str, Any] = {
                "total_positions": 0,
                "positions": [],
                "note": "No search results from Google Custom Search API.",
            }
        else:
            result = ask_gemini_to_count_positions(
                position=position,
                start_date=start_date,
                end_date=end_date,
                search_results=search_results,
            )
    else:
        # Default: AI-only mode, no external search.
        result = ask_gemini_to_estimate_positions(
            position=position,
            start_date=start_date,
            end_date=end_date,
            role_aliases=role_aliases,
        )

    if write_to_s3 and position_id:
        s3_key = write_result_to_s3(
            result=result,
            position_id=position_id,
            start_date=start_date,
            end_date=end_date,
        )
        if s3_key:
            result.setdefault("storage", {})
            result["storage"]["s3_bucket"] = os.getenv("OUTPUT_BUCKET")
            result["storage"]["s3_key"] = s3_key

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count LATAM job positions for a given role and period using Google + Gemini."
    )
    parser.add_argument(
        "--position",
        help="Single job position to search for (e.g. 'Data Engineer').",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=20,
        help="Maximum number of Google results to fetch.",
    )
    parser.add_argument(
        "--extra-terms",
        help="Optional extra terms to add to the Google query.",
    )
    parser.add_argument(
        "--mode",
        choices=["ai_only", "search"],
        default="ai_only",
        help="Data source mode: 'ai_only' (Gemini estimate only) or "
        "'search' (Google Custom Search + Gemini). Defaults to 'ai_only'.",
    )
    parser.add_argument(
        "--positions-config",
        default="positions.json",
        help="Path to JSON file containing a list of positions. "
        "If provided and --position is not set, the pipeline will run for all positions in the file.",
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Disable writing results to S3 even if OUTPUT_BUCKET is configured.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    write_to_s3 = not args.no_s3

    if args.position:
        # Single-position mode.
        result = run_pipeline(
            position=args.position,
            start_date=args.start_date,
            end_date=args.end_date,
            max_results=args.max_results,
            extra_terms=args.extra_terms,
            position_id=args.position.replace(" ", "_").lower(),
            write_to_s3=write_to_s3,
            mode=args.mode,
        )

        total = result.get("total_positions", 0)
        print(
            f"Found {total} positions for '{args.position}' in LATAM "
            f"between {args.start_date} and {args.end_date}"
        )
        print("Raw result:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        # Multi-position/group mode: read from positions config.
        groups = load_position_groups(args.positions_config)
        if not groups:
            raise RuntimeError(
                f"No positions found in config file {args.positions_config}"
            )

        summary: Dict[str, Any] = {"positions": [], "total_positions": 0}

        for item in groups:
            position_id = item.get("id")
            name = item.get("name")
            extra_terms = item.get("extra_terms")
            if not position_id or not name:
                continue

            print(f"Running pipeline for position '{name}' ({position_id})...")
            result = run_pipeline(
                position=name,
                start_date=args.start_date,
                end_date=args.end_date,
                max_results=args.max_results,
                extra_terms=extra_terms or args.extra_terms,
                role_aliases=item.get("role_aliases"),
                position_id=position_id,
                write_to_s3=write_to_s3,
                mode=args.mode,
            )

            total = result.get("total_positions", 0)
            summary["positions"].append(
                {
                    "id": position_id,
                    "name": name,
                    "total_positions": total,
                    "storage": result.get("storage"),
                }
            )
            summary["total_positions"] += int(total or 0)

        print("Summary across all positions:")
        print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
