from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ThemeConfig:
    ink: str = "#ffffff"
    muted: str = "#eef2f7"
    glass_fill: str = "rgba(255, 255, 255, 0.15)"
    glass_fill_soft: str = "rgba(255, 255, 255, 0.10)"
    glass_border: str = "rgba(255, 255, 255, 0.58)"
    glass_shadow: str = "rgba(31, 38, 135, 0.18)"
    mint: str = "#90f3c4"

    page_gradient: str = (
        "linear-gradient(135deg, rgba(17, 19, 23, 0.42) 0%, "
        "rgba(25, 28, 34, 0.36) 46%, rgba(31, 35, 42, 0.42) 100%)"
    )
    page_base: str = "linear-gradient(135deg, #111317 0%, #191c22 46%, #1f232a 100%)"
    wallpaper_blur_px: int = 18
    wallpaper_brightness: float = 0.48
    wallpaper_saturate: float = 0.86
    wallpaper_scale: float = 1.05

    ambient_blur_px: int = 120
    ambient_opacity: float = 0.05
    orb_a: str = "rgba(255, 255, 255, 0.08)"
    orb_b: str = "rgba(255, 255, 255, 0.06)"
    orb_c: str = "rgba(255, 255, 255, 0.04)"

    sidebar_bg: str = (
        "linear-gradient(180deg, rgba(16, 18, 23, 0.64) 0%, "
        "rgba(20, 22, 27, 0.58) 100%)"
    )
    sidebar_blur_px: int = 20
    sidebar_saturate: int = 160

    card_blur_px: int = 2
    card_saturate: int = 180
    card_radius: str = "2rem"
    card_shadow: str = "0 8px 32px rgba(31, 38, 135, 0.18), inset 0 4px 20px rgba(255, 255, 255, 0.22)"
    card_highlight_shadow: str = (
        "inset -10px -8px 0 -11px rgba(255, 255, 255, 0.95), "
        "inset 0 -9px 0 -8px rgba(255, 255, 255, 0.95)"
    )


DEFAULT_THEME = ThemeConfig()


def build_css(background_b64: str, theme: ThemeConfig = DEFAULT_THEME) -> str:
    return f"""
    <style>
    :root {{
        --ink: {theme.ink};
        --muted: {theme.muted};
        --glass-fill: {theme.glass_fill};
        --glass-fill-soft: {theme.glass_fill_soft};
        --glass-border: {theme.glass_border};
        --glass-shadow: {theme.glass_shadow};
        --mint: {theme.mint};
    }}
    .stApp {{
        background: {theme.page_base};
        color: var(--ink);
        position: relative;
        overflow: hidden;
        isolation: isolate;
    }}
    .stApp > * {{
        position: relative;
        z-index: 2;
    }}
    .stApp::before {{
        content: "";
        position: fixed;
        inset: -40px;
        background-image: url("data:image/jpeg;base64,{background_b64}");
        background-size: cover;
        background-position: center center;
        background-repeat: no-repeat;
        filter: blur({theme.wallpaper_blur_px}px) brightness({theme.wallpaper_brightness}) saturate({theme.wallpaper_saturate});
        transform: scale({theme.wallpaper_scale});
        pointer-events: none;
        z-index: -2;
    }}
    .stApp::after {{
        content: "";
        position: fixed;
        inset: 0;
        background: {theme.page_gradient};
        pointer-events: none;
        z-index: -1;
    }}
    .block-container {{
        padding-top: 1.4rem;
        padding-bottom: 2rem;
        position: relative;
        z-index: 3;
    }}
    .ambient-orb {{
        position: fixed;
        border-radius: 999px;
        filter: blur({theme.ambient_blur_px}px);
        opacity: {theme.ambient_opacity};
        z-index: -3;
        pointer-events: none;
    }}
    .orb-a {{
        width: 320px;
        height: 320px;
        top: 80px;
        left: 280px;
        background: {theme.orb_a};
    }}
    .orb-b {{
        width: 380px;
        height: 380px;
        top: 220px;
        right: 140px;
        background: {theme.orb_b};
    }}
    .orb-c {{
        width: 280px;
        height: 280px;
        bottom: 40px;
        left: 45%;
        background: {theme.orb_c};
    }}
    [data-testid="stSidebar"] {{
        background: {theme.sidebar_bg};
        backdrop-filter: blur({theme.sidebar_blur_px}px) saturate({theme.sidebar_saturate}%);
        -webkit-backdrop-filter: blur({theme.sidebar_blur_px}px) saturate({theme.sidebar_saturate}%);
        border-right: 1px solid rgba(255, 255, 255, 0.14);
        box-shadow: inset -1px 0 0 rgba(255, 255, 255, 0.04);
        position: relative;
        z-index: 2;
    }}
    [data-testid="stSidebar"] .block-container {{
        padding-top: 1.2rem;
    }}
    [data-testid="stSidebar"] * {{
        color: var(--ink);
    }}
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-baseweb="input"]) {{
        margin-bottom: 0.7rem;
    }}
    [data-testid="stSidebar"] [data-testid="stTextInput"] small,
    [data-testid="stSidebar"] [data-testid="stTextInput"] p,
    [data-testid="stSidebar"] [data-testid="InputInstructions"] {{
        display: none !important;
    }}
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has(.role-list-hook) {{
        background: rgba(255, 255, 255, 0.11);
        border: 1px solid rgba(255, 255, 255, 0.28);
        border-radius: 28px;
        padding: 0.72rem 0.6rem;
        backdrop-filter: blur(22px) saturate(160%);
        -webkit-backdrop-filter: blur(22px) saturate(160%);
        box-shadow:
            0 12px 28px rgba(0, 0, 0, 0.22),
            inset 0 1px 0 rgba(255, 255, 255, 0.24),
            inset 0 -1px 0 rgba(255, 255, 255, 0.08);
        width: 100%;
        box-sizing: border-box;
    }}
    [data-testid="stSidebar"] .role-list-hook {{
        display: none;
    }}
    [data-testid="stSidebar"] .element-container:has(.role-list-hook) {{
        display: none !important;
        margin: 0 !important;
        padding: 0 !important;
        height: 0 !important;
    }}
    [data-testid="stSidebar"] .stMarkdown:has(.role-list-hook) {{
        display: none !important;
        margin: 0 !important;
        padding: 0 !important;
        height: 0 !important;
    }}
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has(.role-list-hook) .stButton {{
        width: 100%;
    }}
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has(.role-list-hook) .stButton + .stButton {{
        margin-top: 0.18rem;
    }}
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has(.role-list-hook) .stButton > button {{
        width: 100%;
        min-height: auto;
        justify-content: flex-start;
        border: 0;
        border-radius: 0;
        background: transparent;
        color: var(--ink);
        font-size: 1.02rem;
        font-weight: 600;
        letter-spacing: -0.02em;
        padding: 0.26rem 0.32rem;
        box-shadow: none;
        text-align: left;
    }}
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has(.role-list-hook) .stButton > button:hover,
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has(.role-list-hook) .stButton > button:focus,
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has(.role-list-hook) .stButton > button:active {{
        border: 0;
        background: transparent;
        color: var(--ink);
        box-shadow: none;
        outline: none;
    }}
    [data-testid="stSidebar"] [data-baseweb="input"] > div {{
        background: rgba(255, 255, 255, 0.09);
        border: 1px solid rgba(255, 255, 255, 0.16);
        border-radius: 999px;
        backdrop-filter: blur(18px) saturate(145%);
        -webkit-backdrop-filter: blur(18px) saturate(145%);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05) !important;
        outline: none !important;
    }}
    [data-testid="stSidebar"] [data-testid="stTextInput"],
    [data-testid="stSidebar"] [data-testid="stTextInput"] > div,
    [data-testid="stSidebar"] [data-baseweb="input"],
    [data-testid="stSidebar"] [data-baseweb="input"] > div:focus-within,
    [data-testid="stSidebar"] [data-baseweb="input"]:focus-within,
    [data-testid="stSidebar"] [data-baseweb="input"]:has(input:focus),
    [data-testid="stSidebar"] [data-baseweb="input"] > div:has(input:focus) {{
        outline: none !important;
        border-color: rgba(255, 255, 255, 0.16) !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05) !important;
    }}
    [data-testid="stSidebar"] [data-baseweb="input"] > div::before,
    [data-testid="stSidebar"] [data-baseweb="input"] > div::after {{
        border: 0 !important;
        box-shadow: none !important;
        outline: none !important;
    }}
    [data-testid="stSidebar"] input {{
        color: var(--ink) !important;
        font-size: 0.98rem !important;
        outline: none !important;
        box-shadow: none !important;
    }}
    [data-testid="stSidebar"] input:focus,
    [data-testid="stSidebar"] input:focus-visible {{
        outline: none !important;
        box-shadow: none !important;
        border: 0 !important;
    }}
    [data-testid="stSidebar"] input::placeholder {{
        color: rgba(255, 255, 255, 0.58) !important;
    }}
    [data-testid="stMetric"],
    [data-testid="stPlotlyChart"],
    [data-testid="stDataFrame"],
    .summary-card {{
        position: relative;
        overflow: hidden;
        isolation: isolate;
        background: var(--glass-fill);
        backdrop-filter: blur({theme.card_blur_px}px) saturate({theme.card_saturate}%);
        -webkit-backdrop-filter: blur({theme.card_blur_px}px) saturate({theme.card_saturate}%);
        border: 1px solid var(--glass-border);
        box-shadow: {theme.card_shadow};
    }}
    [data-testid="stMetric"]::after,
    [data-testid="stPlotlyChart"]::after,
    [data-testid="stDataFrame"]::after,
    .summary-card::after {{
        content: "";
        position: absolute;
        inset: 0;
        background: var(--glass-fill-soft);
        border-radius: inherit;
        backdrop-filter: blur(1px);
        -webkit-backdrop-filter: blur(1px);
        box-shadow: {theme.card_highlight_shadow};
        opacity: 0.6;
        z-index: -1;
        filter: blur(1px) drop-shadow(10px 4px 6px rgba(0, 0, 0, 0.55)) brightness(115%);
        pointer-events: none;
    }}
    [data-testid="stMetric"] {{
        border-radius: {theme.card_radius};
        padding: 1rem 1.05rem;
    }}
    [data-testid="stMetricLabel"] {{
        color: #f7fafc;
        opacity: 0.95;
    }}
    [data-testid="stMetricValue"] {{
        color: var(--ink);
        text-shadow: 0 1px 8px rgba(0, 0, 0, 0.18);
    }}
    [data-testid="stMetricDelta"] {{
        color: var(--mint);
    }}
    [data-testid="stPlotlyChart"] {{
        border-radius: {theme.card_radius};
        padding: 0.85rem;
    }}
    [data-testid="stDataFrame"] {{
        border-radius: {theme.card_radius};
        padding: 0.35rem;
    }}
    h1, h2, h3 {{
        color: var(--ink);
        letter-spacing: -0.03em;
    }}
    p, li, label, .stCaption, .stMarkdown, .stText {{
        color: var(--muted);
    }}
    .section-title {{
        color: var(--ink);
        font-size: 1.15rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        margin: 1rem 0 0.7rem 0.15rem;
    }}
    .title {{
        color: var(--ink);
        font-size: 2.8rem;
        font-weight: 800;
        line-height: 1.05;
        margin: 0 0 1.2rem 0.15rem;
        text-shadow: 0 2px 14px rgba(0, 0, 0, 0.24);
    }}
    .summary-card {{
        border-radius: {theme.card_radius};
        padding: 1rem 1.1rem;
        color: var(--ink);
        text-shadow: 0 1px 6px rgba(0, 0, 0, 0.14);
    }}
    .summary-card code {{
        background: rgba(8, 12, 22, 0.92);
        color: var(--mint);
        border-radius: 8px;
        padding: 0.12rem 0.35rem;
    }}

    /*
     * Touch / phone layout only (CSS media queries — no JS, no hidden Streamlit header).
     * Keeps native sidebar control visible & tappable; follows common Streamlit forum patterns.
     */
    @media (hover: none) and (pointer: coarse),
           ((max-width: 1024px) and (hover: none)) {{
        .stApp {{
            overflow-x: clip;
            overflow-y: auto;
            min-height: 100vh;
        }}
        [data-testid="stHeader"] {{
            background: linear-gradient(
                180deg,
                rgba(16, 18, 23, 0.96) 0%,
                rgba(20, 22, 28, 0.92) 100%
            ) !important;
            backdrop-filter: blur(14px) saturate(140%);
            -webkit-backdrop-filter: blur(14px) saturate(140%);
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            position: sticky;
            top: 0;
            z-index: 999998;
        }}
        [data-testid="stSidebarCollapsedControl"] button,
        [data-testid="stSidebarCollapsedControl"] [role="button"] {{
            min-height: 48px !important;
            min-width: 48px !important;
        }}
        [data-testid="stMain"] h1.title,
        section.main h1.title {{
            position: fixed;
            top: max(0px, env(safe-area-inset-top));
            left: max(3.5rem, env(safe-area-inset-left));
            right: max(3.5rem, env(safe-area-inset-right));
            margin: 0 !important;
            padding: 0 !important;
            height: 3.25rem;
            line-height: 3.25rem;
            font-size: 1.05rem !important;
            font-weight: 750;
            text-align: center;
            z-index: 999999;
            pointer-events: none;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            color: #f8fafc;
            text-shadow: 0 1px 8px rgba(0, 0, 0, 0.35);
        }}
        .block-container {{
            padding-left: 1rem !important;
            padding-right: 1rem !important;
            padding-top: calc(4.25rem + env(safe-area-inset-top)) !important;
            max-width: 100% !important;
        }}
        .section-title {{
            font-size: 1.02rem !important;
            margin: 0.75rem 0 0.55rem 0.1rem !important;
        }}
        [data-testid="stPlotlyChart"] {{
            padding: 0.5rem 0.35rem !important;
            min-height: 0;
        }}
        [data-testid="stPlotlyChart"] > div,
        [data-testid="stPlotlyChart"] .plotly-graph-div {{
            max-width: 100% !important;
        }}
        [data-testid="stHorizontalBlock"] {{
            flex-wrap: wrap !important;
            gap: 0.65rem !important;
        }}
        [data-testid="stHorizontalBlock"]:has([data-testid="stPlotlyChart"]) > [data-testid="column"],
        [data-testid="stHorizontalBlock"]:has([data-testid="stDataFrame"]) > [data-testid="column"] {{
            flex: 1 1 100% !important;
            min-width: 100% !important;
            width: 100% !important;
        }}
        [data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) > [data-testid="column"] {{
            flex: 1 1 calc(50% - 0.35rem) !important;
            min-width: calc(50% - 0.35rem) !important;
            max-width: calc(50% - 0.35rem) !important;
        }}
        [data-testid="stMetric"] {{
            padding: 0.75rem 0.65rem !important;
        }}
        [data-testid="stMetricLabel"] {{
            font-size: 0.78rem !important;
            white-space: normal !important;
        }}
        [data-testid="stMetricValue"] {{
            font-size: 1.15rem !important;
        }}
        [data-testid="stSidebar"] {{
            z-index: 1000001;
        }}
        .js-plotly-plot .plotly .scatterlayer .textpoint {{
            font-size: 9px !important;
        }}
    }}
    </style>
    """
