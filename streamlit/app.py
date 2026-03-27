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
from theme import build_css


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

st.markdown(build_css(BACKGROUND_B64), unsafe_allow_html=True)

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


def matches_role_search(item: dict, query: str) -> bool:
    if not query:
        return True

    normalized = query.strip().lower()
    haystacks = [item["name"], *(item.get("role_aliases") or [])]
    return any(normalized in str(value).lower() for value in haystacks)


positions = load_position_groups()
if not positions:
    render_empty_state("No roles found in positions.json.")

with st.sidebar:
    search_value = st.text_input(
        "Search roles",
        value=st.session_state.get("role_search", ""),
        placeholder="Search role or alias",
        label_visibility="collapsed",
        key="role_search",
    )

    filtered_positions = [item for item in positions if matches_role_search(item, search_value)]
    if not filtered_positions:
        st.caption("No roles match your search.")
        st.stop()

    role_labels = {item["name"]: item for item in filtered_positions}
    default_role = st.session_state.get("selected_role_name", filtered_positions[0]["name"])
    if default_role not in role_labels:
        default_role = filtered_positions[0]["name"]

    selected_name = default_role
    with st.container():
        st.markdown('<div class="role-list-hook"></div>', unsafe_allow_html=True)
        for role_name in role_labels:
            if st.button(
                role_name,
                key=f"role_picker_{role_labels[role_name]['id']}",
                use_container_width=True,
                type="secondary",
            ):
                st.session_state["selected_role_name"] = role_name
                st.rerun()
    selected_role = role_labels[selected_name]

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
    st.markdown('<div class="section-title">Aliases Included</div>', unsafe_allow_html=True)
    aliases_html = "".join(f"<li>{alias}</li>" for alias in selected_role["role_aliases"])
    extra_context = (
        f'<div style="margin-top:0.8rem;color:var(--muted);"><strong>Query context</strong><br/>{selected_role.get("extra_terms")}</div>'
        if selected_role.get("extra_terms")
        else ""
    )
    st.markdown(
        f"""
        <div class="summary-card">
          <ul style="margin:0; padding-left:1.1rem; line-height:1.9;">
            {aliases_html}
          </ul>
          {extra_context}
        </div>
        """,
        unsafe_allow_html=True,
    )

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
