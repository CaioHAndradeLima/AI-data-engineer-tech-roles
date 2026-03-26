from __future__ import annotations

import base64
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from data_loader import (
    average_keyword_share,
    load_position_groups,
    load_role_bundle,
    load_runtime_env,
    top_keywords_for_record,
)


load_runtime_env()

BACKGROUND_IMAGE = Path(__file__).resolve().parent / "assets-dark.jpg"
BACKGROUND_B64 = (
    base64.b64encode(BACKGROUND_IMAGE.read_bytes()).decode("utf-8")
    if BACKGROUND_IMAGE.exists()
    else ""
)

st.set_page_config(
    page_title="LATAM Roles Observatory",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

CSS_TEMPLATE = """
    <style>
    :root {
        --ink: #f5f7fb;
        --muted: #d7deea;
        --glass-fill: rgba(255, 255, 255, 0.15);
        --glass-fill-soft: rgba(255, 255, 255, 0.10);
        --glass-border: rgba(255, 255, 255, 0.58);
        --glass-shadow: rgba(31, 38, 135, 0.18);
        --mint: #90f3c4;
    }
    .stApp {
        background:
            linear-gradient(135deg, rgba(18, 20, 24, 0.52) 0%, rgba(24, 26, 31, 0.48) 46%, rgba(28, 30, 35, 0.50) 100%),
            url("data:image/jpeg;base64,__BACKGROUND_B64__");
        background-size: cover;
        background-position: center center;
        background-attachment: fixed;
        color: var(--ink);
    }
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2rem;
    }
    .ambient-orb {
        position: fixed;
        border-radius: 999px;
        filter: blur(90px);
        opacity: 0.14;
        z-index: -1;
        pointer-events: none;
    }
    .orb-a {
        width: 320px;
        height: 320px;
        top: 80px;
        left: 280px;
        background: rgba(255, 255, 255, 0.08);
    }
    .orb-b {
        width: 380px;
        height: 380px;
        top: 220px;
        right: 140px;
        background: rgba(255, 255, 255, 0.06);
    }
    .orb-c {
        width: 280px;
        height: 280px;
        bottom: 40px;
        left: 45%;
        background: rgba(255, 255, 255, 0.04);
    }
    [data-testid="stSidebar"] {
        background:
            linear-gradient(180deg, rgba(16, 18, 23, 0.64) 0%, rgba(20, 22, 27, 0.58) 100%);
        backdrop-filter: blur(20px) saturate(160%);
        -webkit-backdrop-filter: blur(20px) saturate(160%);
        border-right: 1px solid rgba(255, 255, 255, 0.14);
        box-shadow: inset -1px 0 0 rgba(255, 255, 255, 0.04);
    }
    [data-testid="stSidebar"] * {
        color: var(--ink);
    }
    [data-testid="stSidebar"] [data-baseweb="radio"] > div {
        gap: 0.4rem;
    }
    [data-testid="stSidebar"] [data-baseweb="radio"] label {
        background: rgba(255, 255, 255, 0.04);
        border: 1px solid transparent;
        border-radius: 14px;
        padding: 0.45rem 0.6rem;
        transition: all 160ms ease;
    }
    [data-testid="stSidebar"] [data-baseweb="radio"] label:hover {
        border-color: rgba(142, 197, 255, 0.28);
        background: rgba(255, 255, 255, 0.08);
    }
    [data-testid="stMetric"],
    [data-testid="stPlotlyChart"],
    [data-testid="stDataFrame"],
    .summary-card {
        position: relative;
        overflow: hidden;
        isolation: isolate;
        background: var(--glass-fill);
        backdrop-filter: blur(2px) saturate(180%);
        -webkit-backdrop-filter: blur(2px) saturate(180%);
        border: 1px solid var(--glass-border);
        box-shadow:
            0 8px 32px var(--glass-shadow),
            inset 0 4px 20px rgba(255, 255, 255, 0.22);
    }
    [data-testid="stMetric"]::after,
    [data-testid="stPlotlyChart"]::after,
    [data-testid="stDataFrame"]::after,
    .summary-card::after {
        content: "";
        position: absolute;
        inset: 0;
        background: var(--glass-fill-soft);
        border-radius: inherit;
        backdrop-filter: blur(1px);
        -webkit-backdrop-filter: blur(1px);
        box-shadow:
            inset -10px -8px 0 -11px rgba(255, 255, 255, 0.95),
            inset 0 -9px 0 -8px rgba(255, 255, 255, 0.95);
        opacity: 0.6;
        z-index: -1;
        filter: blur(1px) drop-shadow(10px 4px 6px rgba(0, 0, 0, 0.55)) brightness(115%);
        pointer-events: none;
    }
    [data-testid="stMetric"] {
        border-radius: 2rem;
        padding: 1rem 1.05rem;
    }
    [data-testid="stMetricLabel"] {
        color: var(--muted);
    }
    [data-testid="stMetricValue"] {
        color: var(--ink);
    }
    [data-testid="stMetricDelta"] {
        color: var(--mint);
    }
    [data-testid="stPlotlyChart"] {
        border-radius: 2rem;
        padding: 0.85rem;
    }
    [data-testid="stDataFrame"] {
        border-radius: 2rem;
        padding: 0.35rem;
    }
    h1, h2, h3 {
        color: var(--ink);
        letter-spacing: -0.03em;
    }
    p, li, label, .stCaption, .stMarkdown, .stText {
        color: var(--muted);
    }
    .section-title {
        color: var(--ink);
        font-size: 1.15rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        margin: 1rem 0 0.7rem 0.15rem;
    }
    .title {
        color: var(--ink);
        font-size: 2.8rem;
        font-weight: 800;
        line-height: 1.05;
        margin: 0 0 1.2rem 0.15rem;
        text-shadow: 0 2px 14px rgba(0, 0, 0, 0.24);
    }
    .summary-card {
        border-radius: 2rem;
        padding: 1rem 1.1rem;
        color: var(--ink);
    }
    .summary-card code {
        background: rgba(8, 12, 22, 0.8);
        color: var(--mint);
        border-radius: 8px;
        padding: 0.12rem 0.35rem;
    }
    </style>
    """

st.markdown(
    CSS_TEMPLATE.replace("__BACKGROUND_B64__", BACKGROUND_B64),
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="ambient-orb orb-a"></div>
    <div class="ambient-orb orb-b"></div>
    <div class="ambient-orb orb-c"></div>
    """,
    unsafe_allow_html=True,
)


def render_empty_state(message: str) -> None:
    st.warning(message)
    st.stop()


positions = load_position_groups()
if not positions:
    render_empty_state("No roles found in positions.json.")

with st.sidebar:
    st.title("Role List")
    st.caption("Choose a role family to inspect monthly trend data and technology signals.")
    role_labels = {item["name"]: item for item in positions}
    selected_name = st.radio("Role family", options=list(role_labels.keys()), label_visibility="collapsed")
    selected_role = role_labels[selected_name]

    st.divider()
    st.markdown("**Aliases included**")
    for alias in selected_role["role_aliases"]:
        st.write(f"- {alias}")

    if selected_role.get("extra_terms"):
        st.divider()
        st.markdown("**Query context**")
        st.caption(selected_role["extra_terms"])

st.markdown(f'<h1 class="title">{selected_role["name"]}</h1>', unsafe_allow_html=True)

try:
    bundle = load_role_bundle(selected_role["id"], selected_role["name"])
except Exception as exc:  # noqa: BLE001
    render_empty_state(f"Unable to load role data: {exc}")

records = bundle["records"]
positions_df: pd.DataFrame = bundle["positions_df"]
keywords_df: pd.DataFrame = bundle["keywords_df"]

if not records:
    render_empty_state(
        f"No S3 results found yet for `{selected_role['id']}` in bucket `{bundle['bucket']}`."
    )

latest_record = records[-1]
latest_keywords_df = top_keywords_for_record(latest_record, limit=12)
avg_share_df = average_keyword_share(keywords_df, limit=12)

metric_1, metric_2, metric_3 = st.columns(3)
metric_1.metric(
    "Positions Found In Latest Month",
    f"{latest_record.total_positions:,}",
    latest_record.month_label,
)
metric_2.metric(
    "Tracked Months",
    len(records),
)
metric_3.metric(
    "Top Technology",
    latest_keywords_df.iloc[0]["keyword"] if not latest_keywords_df.empty else "N/A",
    f"{int(latest_keywords_df.iloc[0]['count'])} mentions" if not latest_keywords_df.empty else None,
)

col_left, col_right = st.columns([1.15, 0.85], gap="large")

with col_left:
    st.markdown('<div class="section-title">Positions Over Time</div>', unsafe_allow_html=True)
    positions_chart = px.line(
        positions_df,
        x="month",
        y="total_positions",
        markers=True,
        line_shape="spline",
        color_discrete_sequence=["#14b8a6"],
    )
    positions_chart.update_layout(
        height=360,
        margin=dict(l=20, r=20, t=10, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.045)",
        xaxis_title="Month",
        yaxis_title="Estimated Open Positions",
        font=dict(color="#eef2ff"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.08)", zerolinecolor="rgba(255,255,255,0.08)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.14)", zerolinecolor="rgba(255,255,255,0.08)"),
    )
    st.plotly_chart(positions_chart, use_container_width=True)

    st.markdown('<div class="section-title">Technology Presence In Latest Month</div>', unsafe_allow_html=True)
    if latest_keywords_df.empty:
        st.info("No keyword data available for the latest month.")
    else:
        tech_chart = px.bar(
            latest_keywords_df.sort_values("count"),
            x="count",
            y="keyword",
            orientation="h",
            color="count",
            color_continuous_scale=["#fed7aa", "#fb923c", "#c2410c"],
        )
        tech_chart.update_layout(
            height=420,
            margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.045)",
            xaxis_title="Estimated Mentions",
            yaxis_title="Technology",
            coloraxis_showscale=False,
            font=dict(color="#eef2ff"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.08)", zerolinecolor="rgba(255,255,255,0.08)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.05)"),
        )
        st.plotly_chart(tech_chart, use_container_width=True)

with col_right:
    st.markdown('<div class="section-title">Latest Month Summary</div>', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="summary-card">
          <strong>Period</strong>: <code>{latest_record.start_date}</code> to <code>{latest_record.end_date}</code><br/>
          <strong>S3 Key</strong>: <code>{latest_record.s3_key}</code><br/>
          <strong>Bucket</strong>: <code>{bundle["bucket"]}</code>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(latest_record.note or "No additional note available.")

    st.markdown('<div class="section-title">Average Requirement Coverage</div>', unsafe_allow_html=True)
    if avg_share_df.empty:
        st.info("No keyword coverage data available yet.")
    else:
        avg_share_df["avg_share_pct"] = (avg_share_df["avg_share"] * 100).round(1)
        radar_chart = px.bar_polar(
            avg_share_df,
            r="avg_share_pct",
            theta="keyword",
            color="avg_share_pct",
            color_continuous_scale=["#c7d2fe", "#3b82f6", "#1d4ed8"],
        )
        radar_chart.update_layout(
            height=420,
            margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            polar=dict(
                bgcolor="rgba(255,255,255,0.045)",
                radialaxis=dict(
                    showticklabels=True,
                    ticksuffix="%",
                    gridcolor="rgba(255,255,255,0.12)",
                    linecolor="rgba(255,255,255,0.08)",
                    tickfont=dict(color="#dbe4f3"),
                ),
                angularaxis=dict(
                    gridcolor="rgba(255,255,255,0.08)",
                    linecolor="rgba(255,255,255,0.08)",
                    tickfont=dict(color="#dbe4f3"),
                ),
            ),
            coloraxis_showscale=False,
            font=dict(color="#eef2ff"),
        )
        st.plotly_chart(radar_chart, use_container_width=True)

    st.markdown('<div class="section-title">Monthly Raw Data</div>', unsafe_allow_html=True)
    st.dataframe(
        positions_df.sort_values("month", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

st.markdown('<div class="section-title">Technology Trend By Month</div>', unsafe_allow_html=True)
if keywords_df.empty:
    st.info("No historical keyword data found.")
else:
    top_keywords = (
        keywords_df.groupby("keyword", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
        .head(8)["keyword"]
        .tolist()
    )
    trend_df = keywords_df[keywords_df["keyword"].isin(top_keywords)].copy()
    trend_df["share_pct"] = (trend_df["share"] * 100).round(1)
    trend_chart = px.line(
        trend_df,
        x="month",
        y="share_pct",
        color="keyword",
        markers=True,
        line_shape="spline",
        color_discrete_sequence=[
            "#0f766e",
            "#2563eb",
            "#b45309",
            "#dc2626",
            "#7c3aed",
            "#059669",
            "#ea580c",
            "#1d4ed8",
        ],
    )
    trend_chart.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=10, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.045)",
        xaxis_title="Month",
        yaxis_title="Share Of Positions Mentioning Technology (%)",
        legend_title="Technology",
        font=dict(color="#eef2ff"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.08)", zerolinecolor="rgba(255,255,255,0.08)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.12)", zerolinecolor="rgba(255,255,255,0.08)"),
    )
    st.plotly_chart(trend_chart, use_container_width=True)

st.caption(
    f"Data source: S3 bucket `{bundle['bucket']}`. Role families come from `{Path('positions.json')}`."
)
