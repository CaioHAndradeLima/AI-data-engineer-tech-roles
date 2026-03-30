from __future__ import annotations

import base64
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

from data_loader import (
    average_keyword_share,
    build_keyword_salary_dataframe,
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


@st.fragment
def render_keyword_salary_explorer(keyword_salary_df: pd.DataFrame) -> None:
    st.markdown('<div class="section-title">Keyword Salary Explorer</div>', unsafe_allow_html=True)
    if keyword_salary_df.empty:
        st.info("No keyword salary history available yet.")
        return

    keyword_options = sorted(keyword_salary_df["keyword"].dropna().unique().tolist())
    default_keyword = keyword_options[0] if keyword_options else None
    selected_keyword = st.selectbox(
        "Pick a keyword",
        options=keyword_options,
        index=keyword_options.index(default_keyword) if default_keyword else 0,
        key="salary_keyword_selector",
    )
    selected_keyword_df = keyword_salary_df[keyword_salary_df["keyword"] == selected_keyword].copy()

    keyword_col_left, keyword_col_right = st.columns([1.0, 1.15], gap="large")

    with keyword_col_left:
        keyword_salary_chart = px.line(
            selected_keyword_df.dropna(subset=["average_salary"]),
            x="month",
            y="average_salary",
            markers=True,
            line_shape="spline",
            color_discrete_sequence=["#22c55e"],
        )
        keyword_salary_chart.update_layout(
            height=340,
            margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.045)",
            xaxis_title="Month",
            yaxis_title=f"{selected_keyword} Avg Salary (USD / month)",
            font=dict(color="#eef2ff"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.08)", zerolinecolor="rgba(255,255,255,0.08)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.14)", zerolinecolor="rgba(255,255,255,0.08)"),
        )
        st.plotly_chart(keyword_salary_chart, use_container_width=True)

    with keyword_col_right:
        tier_df = selected_keyword_df.melt(
            id_vars=["month"],
            value_vars=["tier1_roles", "tier2_roles", "tier3_roles", "stock_options_roles"],
            var_name="metric",
            value_name="count",
        )
        tier_labels = {
            "tier1_roles": "Tier 1 (0-6000)",
            "tier2_roles": "Tier 2 (6000-9000)",
            "tier3_roles": "Tier 3 (9000-15000)",
            "stock_options_roles": "Stock Options",
        }
        tier_df["metric"] = tier_df["metric"].map(tier_labels)
        tier_chart = px.line(
            tier_df,
            x="month",
            y="count",
            color="metric",
            markers=True,
            line_shape="spline",
            color_discrete_map={
                "Tier 1 (0-6000)": "#fbbf24",
                "Tier 2 (6000-9000)": "#fb923c",
                "Tier 3 (9000-15000)": "#ef4444",
                "Stock Options": "#38bdf8",
            },
        )
        tier_chart.update_layout(
            height=340,
            margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.045)",
            xaxis_title="Month",
            yaxis_title="Estimated Roles By Salary Tier / Stock Options",
            legend_title="Metric",
            font=dict(color="#eef2ff"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.08)", zerolinecolor="rgba(255,255,255,0.08)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.12)", zerolinecolor="rgba(255,255,255,0.08)"),
        )
        st.plotly_chart(tier_chart, use_container_width=True)


def matches_role_search(item: dict, query: str) -> bool:
    if not query:
        return True

    normalized = query.strip().lower()
    haystacks = [item["name"], *(item.get("role_aliases") or [])]
    return any(normalized in str(value).lower() for value in haystacks)


positions = load_position_groups()
if not positions:
    render_empty_state("No roles found in positions.json.")

all_role_labels = {item["name"]: item for item in positions}
selected_name = st.session_state.get("selected_role_name", positions[0]["name"])
if selected_name not in all_role_labels:
    selected_name = positions[0]["name"]

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

    with st.container():
        st.markdown('<div class="role-list-hook"></div>', unsafe_allow_html=True)
        for role_name in [item["name"] for item in filtered_positions]:
            if st.button(
                role_name,
                key=f"role_picker_{all_role_labels[role_name]['id']}",
                use_container_width=True,
                type="secondary",
            ):
                st.session_state["selected_role_name"] = role_name
                st.rerun()

selected_role = all_role_labels[selected_name]

components.html(
    """
    <script>
    const root = window.parent.document;
    function bindInstantSearch() {
      const sidebar = root.querySelector('[data-testid="stSidebar"]');
      if (!sidebar) return;
      const input = sidebar.querySelector('input[placeholder="Search role or alias"]');
      if (!input || input.dataset.instantSearchBound === "1") return;
      input.dataset.instantSearchBound = "1";
      let timer = null;
      input.addEventListener("input", () => {
        window.clearTimeout(timer);
        timer = window.setTimeout(() => {
          input.dispatchEvent(new KeyboardEvent("keydown", {key: "Enter", code: "Enter", keyCode: 13, which: 13, bubbles: true}));
          input.dispatchEvent(new KeyboardEvent("keyup", {key: "Enter", code: "Enter", keyCode: 13, which: 13, bubbles: true}));
          input.dispatchEvent(new Event("change", {bubbles: true}));
        }, 90);
      });
    }
    bindInstantSearch();
    new MutationObserver(bindInstantSearch).observe(root.body, {childList: true, subtree: true});
    </script>
    """,
    height=0,
)

st.markdown(f'<h1 class="title">{selected_role["name"]}</h1>', unsafe_allow_html=True)

try:
    bundle = load_role_bundle(selected_role["id"], selected_role["name"])
except Exception as exc:  # noqa: BLE001
    render_empty_state(f"Unable to load role data: {exc}")

records = bundle["records"]
positions_df: pd.DataFrame = bundle["positions_df"]
keywords_df: pd.DataFrame = bundle["keywords_df"]
keyword_salary_df: pd.DataFrame = bundle["keyword_salary_df"]
salary_df: pd.DataFrame = bundle["salary_df"]

if not records:
    render_empty_state(
        f"No S3 results found yet for `{selected_role['id']}` in bucket `{bundle['bucket']}`."
    )

latest_record = records[-1]
latest_period_label = datetime.fromisoformat(latest_record.start_date).strftime("%B %Y")
latest_keywords_df = top_keywords_for_record(latest_record, limit=12)
avg_share_df = average_keyword_share(keywords_df, limit=12)
latest_salary_row = (
    salary_df.sort_values("month").iloc[-1].to_dict()
    if not salary_df.empty
    else {}
)
latest_average_salary = latest_salary_row.get("average_salary")
latest_stock_roles = int(latest_salary_row.get("stock_options_roles", 0) or 0)

metric_1, metric_2, metric_3, metric_4 = st.columns(4)
metric_1.metric(
    f"Positions Found In {latest_period_label}",
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
metric_4.metric(
    "Average Salary",
    f"USD {latest_average_salary:,.0f}" if latest_average_salary else "N/A",
    latest_record.month_label if latest_average_salary else None,
)

col_left, col_right = st.columns([1.15, 0.85], gap="large")

with col_left:
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

    st.markdown(
        f'<div class="section-title">Technology Presence In {latest_period_label}</div>',
        unsafe_allow_html=True,
    )
    if latest_keywords_df.empty:
        st.info(f"No keyword data available for {latest_period_label}.")
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

    st.markdown(
        f'<div class="section-title">{latest_period_label} Summary</div>',
        unsafe_allow_html=True,
    )
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

    st.markdown(
        f'<div class="section-title">{latest_period_label} Salary Snapshot</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div class="summary-card">
          <strong>Average Salary</strong>: {"<code>USD {:,.0f}</code>".format(latest_average_salary) if latest_average_salary else "<code>N/A</code>"}<br/>
          <strong>Stock Option Roles</strong>: <code>{latest_stock_roles}</code><br/>
          <strong>Salary Source</strong>: <code>Gemini estimate by keyword</code>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="section-title">Monthly Raw Data</div>', unsafe_allow_html=True)
    st.dataframe(
        positions_df.sort_values("month", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

st.markdown('<div class="section-title">Average Salary Over Time</div>', unsafe_allow_html=True)
if salary_df.empty or salary_df["average_salary"].dropna().empty:
    st.info("No salary data available yet.")
else:
    salary_chart = px.line(
        salary_df.dropna(subset=["average_salary"]),
        x="month",
        y="average_salary",
        markers=True,
        line_shape="spline",
        color_discrete_sequence=["#f59e0b"],
    )
    salary_chart.update_layout(
        height=360,
        margin=dict(l=20, r=20, t=10, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.045)",
        xaxis_title="Month",
        yaxis_title="Average Salary (USD / month)",
        font=dict(color="#eef2ff"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.08)", zerolinecolor="rgba(255,255,255,0.08)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.14)", zerolinecolor="rgba(255,255,255,0.08)"),
    )
    st.plotly_chart(salary_chart, use_container_width=True)

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
    trend_df["point_label"] = trend_df.apply(
        lambda row: f"{row['share_pct']}% · {int(row['count'])}",
        axis=1,
    )
    trend_chart = px.line(
        trend_df,
        x="month",
        y="share_pct",
        color="keyword",
        markers=True,
        text="point_label",
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
    trend_chart.update_traces(
        textposition="top center",
        hovertemplate=(
            "<b>%{fullData.name}</b><br>"
            "Month: %{x}<br>"
            "Share: %{y:.1f}%<br>"
            "Roles found: %{customdata[0]}<extra></extra>"
        ),
        customdata=trend_df[["count"]].to_numpy(),
    )
    st.plotly_chart(trend_chart, use_container_width=True)

render_keyword_salary_explorer(keyword_salary_df)

st.caption(
    f"Data source: S3 bucket `{bundle['bucket']}`. Role families come from `{Path('positions.json')}`."
)
