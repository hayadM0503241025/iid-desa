import streamlit as st
import pandas as pd
import numpy as np
import os
import base64
import networkx as nx
from community import community_louvain
import plotly.graph_objects as go
import plotly.io as pio
import plotly.express as px

# =========================================================
# 1. KONFIGURASI TEMA & UI DDP (WHITE SIDEBAR STANDARD)
# =========================================================
LOGO_PATH = os.path.join("assets", "logo-banner2.png")
HEADER_PATH = next(
    (
        p
        for p in [
            os.path.join("assets", "header.png"),
            os.path.join("assets", "header.jpg"),
            os.path.join("assets", "header.jpeg"),
        ]
        if os.path.exists(p)
    ),
    None,
)
FRAME_PATH = os.path.join("assets", "frame.png") if os.path.exists(os.path.join("assets", "frame.png")) else None

# Jika file logo ada di assets, pakai itu. Jika tidak, pakai icon default sementara.
if os.path.exists(LOGO_PATH):
    page_icon = LOGO_PATH
else:
    page_icon = "SNA"


def get_image_data_uri(path):
    if not path or not os.path.exists(path):
        return None
    ext = os.path.splitext(path)[1].lower()
    mime = "image/png" if ext == ".png" else "image/jpeg" if ext in {".jpg", ".jpeg"} else "application/octet-stream"
    try:
        with open(path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
        return f"data:{mime};base64,{encoded}"
    except Exception:
        return None


def get_logo_data_uri(path):
    if not os.path.exists(path):
        return None
    return get_image_data_uri(path)

st.set_page_config(
    page_title="DDP Dashboard SNA",
    page_icon=page_icon,
    layout="wide"
)

DDP_BLUE = "#111827"
DDP_RED = "#B91C1C"
LIGHT_BG = "#EEF2F7"

# Skala warna kontras tinggi yang tetap layak untuk publikasi.
SATELLITE_COLORS = [[0, "#B91C1C"], [0.5, "#F59E0B"], [1, "#2563EB"]]
CONTRAST_COLORS = [
    "#1F77B4", "#D62728", "#2CA02C", "#9467BD", "#8C564B",
    "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF", "#FF7F0E",
    "#4C78A8", "#F58518", "#54A24B", "#B279A2", "#72B7B2",
    "#9D755D", "#EECA3B", "#E45756"
]
PLOTLY_DRAW_CONFIG = {
    "scrollZoom": True,
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToAdd": ["drawrect", "drawline", "drawopenpath", "drawclosedpath", "drawcircle", "eraseshape"],
}
BINARY_COLOR_MAP = {"YA": "#2563EB", "TIDAK": DDP_RED}
BANSOS_TARGETING_COLORS = {
    "Rendah - Penerima": "#0f766e",
    "Rendah - Belum Menerima": "#b91c1c",
    "Sedang - Penerima": "#14b8a6",
    "Sedang - Belum Menerima": "#f59e0b",
    "Tinggi - Penerima": "#2563eb",
    "Tinggi - Belum Menerima": "#64748b",
    "Sangat Tinggi - Penerima": "#7c3aed",
    "Sangat Tinggi - Belum Menerima": "#94a3b8",
    "Tidak Valid": "#cbd5e1",
}
BPS_CATEGORY_COLORS = {
    "Rendah": "#b91c1c",
    "Sedang": "#d97706",
    "Tinggi": "#0f766e",
    "Sangat Tinggi": "#2563eb",
}
BPS_CATEGORY_ORDER = ("Sangat Tinggi", "Tinggi", "Sedang", "Rendah")
BPS_FALLBACK_COLOR = "#94a3b8"
PLOT_TEXT_COLOR = "#111827"
PLOT_GRID_COLOR = "#E2E8F0"
PUBLICATION_TEMPLATE = "ddp_clarity"
PUBLICATION_FONT = '"Source Sans Pro", "Segoe UI", Arial, sans-serif'
PUBLICATION_CONTINUOUS_SCALE = [[0.0, "#B91C1C"], [0.5, "#F59E0B"], [1.0, "#0F766E"]]
NETWORK_EDGE_NEUTRAL = "rgba(71, 85, 105, 0.24)"
NETWORK_EDGE_FAINT = "rgba(148, 163, 184, 0.28)"
NETWORK_NODE_LINE = "#111827"
HEADER_DATA_URI = get_image_data_uri(HEADER_PATH)
FRAME_DATA_URI = get_image_data_uri(FRAME_PATH)
KPI_FRAME_STYLE = (
    f"""
        border: none;
        border-radius: 0;
        padding: 18px 12px 14px 12px;
        background-image: url('{FRAME_DATA_URI}');
        background-size: 100% 100%;
        background-repeat: no-repeat;
        background-position: center;
    """
    if FRAME_DATA_URI
    else """
        border: 1px solid rgba(255,255,255,0.16);
        border-radius: 18px;
        padding: 18px 14px;
    """
)


def render_global_header():
    if not HEADER_DATA_URI:
        return
    st.markdown(
        f"""
        <div class="global-header-wrap">
            <img src="{HEADER_DATA_URI}" class="global-header-img" alt="Dashboard Header"/>
        </div>
        """,
        unsafe_allow_html=True,
    )


def subbab_dropdown(title, expanded=False):
    return st.expander(title, expanded=expanded)


pio.templates["ddp_clarity"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(color=PLOT_TEXT_COLOR, size=13, family=PUBLICATION_FONT),
        title=dict(font=dict(color=PLOT_TEXT_COLOR, size=18, family=PUBLICATION_FONT), x=0.02, xanchor="left"),
        legend=dict(
            bgcolor="rgba(255,255,255,0.92)",
            bordercolor="#E2E8F0",
            borderwidth=1,
            font=dict(color=PLOT_TEXT_COLOR),
        ),
        xaxis=dict(
            color=PLOT_TEXT_COLOR,
            gridcolor=PLOT_GRID_COLOR,
            zerolinecolor="#94a3b8",
            linecolor="#334155",
            ticks="outside",
        ),
        yaxis=dict(
            color=PLOT_TEXT_COLOR,
            gridcolor=PLOT_GRID_COLOR,
            zerolinecolor="#94a3b8",
            linecolor="#334155",
            ticks="outside",
        ),
        coloraxis=dict(
            colorbar=dict(
                tickfont=dict(color=PLOT_TEXT_COLOR),
                title=dict(font=dict(color=PLOT_TEXT_COLOR)),
            )
        ),
    )
)
pio.templates.default = PUBLICATION_TEMPLATE
pio.templates["plotly_white"] = pio.templates[PUBLICATION_TEMPLATE]


def style_publication_figure(
    fig,
    title=None,
    height=None,
    xaxis_title=None,
    yaxis_title=None,
    showlegend=None,
    legend_title=None,
    margin=None,
):
    layout_kwargs = {
        "template": PUBLICATION_TEMPLATE,
        "paper_bgcolor": "#FFFFFF",
        "plot_bgcolor": "#FFFFFF",
        "font": dict(color=PLOT_TEXT_COLOR, family=PUBLICATION_FONT, size=13),
        "hoverlabel": dict(bgcolor="#FFFFFF", font_size=12, font_family=PUBLICATION_FONT),
        "margin": margin or dict(l=48, r=24, t=72, b=48),
    }
    if title is not None:
        layout_kwargs["title"] = dict(text=title, x=0.02, xanchor="left")
    if height is not None:
        layout_kwargs["height"] = height
    if showlegend is not None:
        layout_kwargs["showlegend"] = showlegend
    if legend_title is not None:
        layout_kwargs["legend_title_text"] = legend_title
    fig.update_layout(**layout_kwargs)
    fig.update_xaxes(
        title_text=xaxis_title,
        showline=True,
        linewidth=1,
        linecolor="#CBD5E1",
        gridcolor=PLOT_GRID_COLOR,
        zerolinecolor="#CBD5E1",
        ticks="outside",
    )
    fig.update_yaxes(
        title_text=yaxis_title,
        showline=True,
        linewidth=1,
        linecolor="#CBD5E1",
        gridcolor=PLOT_GRID_COLOR,
        zerolinecolor="#CBD5E1",
        ticks="outside",
    )
    return fig


def style_network_figure(fig, title, height=540, showlegend=False):
    style_publication_figure(
        fig,
        title=title,
        height=height,
        showlegend=showlegend,
        margin=dict(l=16, r=16, t=72, b=16),
    )
    fig.update_xaxes(visible=False, showgrid=False, zeroline=False)
    fig.update_yaxes(visible=False, showgrid=False, zeroline=False)
    return fig


def ordered_existing_categories(values, preferred_order, invalid_label="Tidak Valid"):
    observed = pd.Series(values).dropna().astype(str).str.strip()
    observed = observed[(observed != "") & (observed != invalid_label)]
    present = set(observed.tolist())
    ordered = [cat for cat in preferred_order if cat in present]
    extras = sorted(present.difference(ordered))
    return ordered + extras

st.markdown(f"""
    <style>
    :root {{
        --surface: rgba(255, 255, 255, 0.88);
        --surface-strong: rgba(255, 255, 255, 0.96);
        --stroke: rgba(15, 23, 42, 0.12);
        --shadow: 0 18px 45px rgba(15, 23, 42, 0.10);
        --text-main: {DDP_BLUE};
        --accent: #2563EB;
    }}
    .stApp {{
        font-family: "SF Pro Display", "SF Pro Text", "Helvetica Neue", "Segoe UI", sans-serif;
        background:
            radial-gradient(1200px 460px at 5% -10%, rgba(37, 99, 235, 0.13) 0%, rgba(37, 99, 235, 0) 62%),
            radial-gradient(900px 420px at 98% 0%, rgba(15, 23, 42, 0.08) 0%, rgba(15, 23, 42, 0) 65%),
            linear-gradient(180deg, {LIGHT_BG} 0%, #F8FAFC 60%, #FFFFFF 100%);
    }}
    .main .block-container {{
        max-width: 1400px;
        padding-top: 1.1rem;
        padding-bottom: 1.6rem;
    }}
    .global-header-wrap {{
        width: 100%;
        border-radius: 0;
        overflow: hidden;
        margin: 0.1rem 0 1.0rem 0;
        border: 1px solid rgba(15, 23, 42, 0.10);
        box-shadow: 0 16px 40px rgba(15, 23, 42, 0.16);
        background: rgba(255,255,255,0.82);
    }}
    .global-header-img {{
        width: 100%;
        display: block;
        object-fit: cover;
    }}
    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #0F172A 0%, #111827 100%) !important;
        border-right: 1px solid rgba(255,255,255,0.10);
    }}
    section[data-testid="stSidebar"] * {{
        color: #E5E7EB !important;
    }}
    section[data-testid="stSidebar"] [data-baseweb="select"] > div,
    section[data-testid="stSidebar"] [data-baseweb="input"] > div,
    section[data-testid="stSidebar"] .stSlider {{
        background: rgba(255,255,255,0.06) !important;
        border: 1px solid rgba(255,255,255,0.12) !important;
        border-radius: 12px !important;
    }}
    .sidebar-logo-shell {{
        width: 62px;
        height: 62px;
        border-radius: 16px;
        border: 1px solid rgba(255,255,255,0.42);
        background: linear-gradient(145deg, rgba(255,255,255,0.14) 0%, rgba(255,255,255,0.06) 100%);
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.24), 0 8px 20px rgba(2,6,23,0.34);
    }}
    .sidebar-logo-disc {{
        width: 48px;
        height: 48px;
        border-radius: 999px;
        background: #FFFFFF;
        display: flex;
        align-items: center;
        justify-content: center;
        border: 1px solid #E5E7EB;
        overflow: hidden;
    }}
    .sidebar-logo-img {{
        width: 36px;
        height: 36px;
        object-fit: contain;
        display: block;
    }}
    .sidebar-logo-fallback {{
        color: #111827 !important;
        font-size: 0.8rem;
        font-weight: 700;
        letter-spacing: 0.3px;
    }}
    section[data-testid="stSidebar"] div[data-testid="stFileUploader"] {{
        margin-top: 0.15rem;
    }}
    section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {{
        background: rgba(148,163,184,0.14) !important;
        border: 1px solid rgba(255,255,255,0.28) !important;
        border-radius: 14px !important;
        padding: 0.75rem 0.7rem !important;
    }}
    section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"]:hover {{
        background: rgba(148,163,184,0.20) !important;
        border-color: rgba(255,255,255,0.42) !important;
    }}
    section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] span,
    section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] small {{
        color: #E5E7EB !important;
    }}
    section[data-testid="stSidebar"] [data-testid="stFileUploaderFileName"] {{
        color: #F8FAFC !important;
    }}
    section[data-testid="stSidebar"] [data-testid="stFileUploader"] button {{
        background: rgba(255,255,255,0.10) !important;
        color: #F8FAFC !important;
        border: 1px solid rgba(255,255,255,0.26) !important;
        border-radius: 10px !important;
    }}
    .kpi-card {{
        {KPI_FRAME_STYLE}
        min-height: 132px;
        border-radius: 0;
        color: #F8FAFC;
        text-align: center;
        box-shadow: var(--shadow);
        margin-bottom: 14px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
    }}
    .kpi-card h3 {{
        margin: 0 0 6px 0;
        font-size: 1.7rem;
        line-height: 1.05;
        letter-spacing: 0.3px;
        font-weight: 700;
    }}
    .kpi-card p {{
        margin: 0;
        font-size: 0.82rem;
        letter-spacing: 0.4px;
        opacity: 0.96;
    }}
    .bg-ddp-blue {{
        background-color: #1E293B;
    }}
    .bg-ddp-red {{
        background-color: #991B1B;
    }}
    .main-header {{
        color: var(--text-main);
        font-family: "SF Pro Display", "Helvetica Neue", sans-serif;
        font-weight: 700;
        letter-spacing: 0.2px;
        border-bottom: 1px solid rgba(15, 23, 42, 0.16);
        padding-bottom: 10px;
        margin-bottom: 14px;
    }}
    .soft-card {{
        background: var(--surface);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid var(--stroke);
        border-radius: 16px;
        padding: 14px 16px;
        margin-bottom: 14px;
        box-shadow: var(--shadow);
    }}
    .premium-hero {{
        background:
            linear-gradient(130deg, rgba(15, 23, 42, 0.95) 0%, rgba(30, 41, 59, 0.92) 70%),
            radial-gradient(circle at 85% 20%, rgba(37, 99, 235, 0.35) 0%, rgba(37, 99, 235, 0) 52%);
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 20px;
        color: #F8FAFC;
        padding: 16px 20px;
        margin-bottom: 14px;
        box-shadow: 0 16px 40px rgba(2, 6, 23, 0.30);
    }}
    .premium-hero b {{
        color: #FFFFFF;
    }}
    @media (max-width: 768px) {{
        .kpi-card {{ min-height: 116px; border-radius: 0; }}
        .main-header {{ font-size: 1.2rem; }}
        .stats-container {{ font-size: 12px; }}
    }}
    .stats-container {{
        background: var(--surface-strong);
        padding: 22px;
        border-radius: 14px;
        border: 1px solid var(--stroke);
        font-family: "SF Mono", "JetBrains Mono", "Consolas", monospace;
        color: #1F2937;
        line-height: 1.5;
        font-size: 14px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
    }}
    .explanation-pillar {{
        padding: 20px;
        border-radius: 14px;
        border: 1px solid var(--stroke);
        background: var(--surface-strong);
        min-height: 180px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
        margin-bottom: 15px;
    }}
    div[data-testid="stPlotlyChart"] {{
        background: var(--surface-strong);
        border: 1px solid var(--stroke);
        border-radius: 16px;
        padding: 8px 10px;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06) !important;
    }}
    .streamlit-expanderHeader {{
        background: var(--surface-strong);
        border-radius: 14px;
        border: 1px solid var(--stroke);
        padding: 0.2rem 0.4rem;
        font-weight: 700;
        color: var(--text-main);
    }}
    .stTabs [data-baseweb="tab-list"] {{
        gap: 8px;
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 999px;
        background: rgba(15, 23, 42, 0.06);
        border: 1px solid rgba(15, 23, 42, 0.12);
        padding: 7px 14px;
    }}
    .stTabs [aria-selected="true"] {{
        background: #0F172A !important;
        color: #F8FAFC !important;
        border-color: #0F172A !important;
    }}
    div[data-testid="stMetric"] {{
        background: var(--surface);
        border: 1px solid var(--stroke);
        border-radius: 14px;
        padding: 10px 12px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
    }}
    </style>
    """, unsafe_allow_html=True)

BUILDER_GRAPH_COLS = ("ipm_mikro", "dusun", "organisasi_num")
EDGE_REKAP_COLS = (
    "f_a_dari_rekap_kk",
    "f_b_dari_rekap_kk",
    "f_c_dari_rekap_kk",
    "f_d_dari_rekap_kk",
    "f_e_dari_rekap_kk",
)
IKR_DIMENSION_DISPLAY = {
    "f_a_dari_rekap_kk": "Sandang, Pangan, dan Papan",
    "f_b_dari_rekap_kk": "Pendidikan",
    "f_c_dari_rekap_kk": "Sosial, Hukum, dan HAM",
    "f_d_dari_rekap_kk": "Kesehatan dan Pekerjaan",
    "f_e_dari_rekap_kk": "Lingkungan dan Infrastruktur",
}
IKR_DIMENSION_MAP = (
    ("Sandang, Pangan, dan Papan", "f_a_dari_rekap_kk"),
    ("Pendidikan", "f_b_dari_rekap_kk"),
    ("Sosial, Hukum, dan HAM", "f_c_dari_rekap_kk"),
    ("Kesehatan dan Pekerjaan", "f_d_dari_rekap_kk"),
    ("Lingkungan dan Infrastruktur", "f_e_dari_rekap_kk"),
)
IKR_OVERALL_METRIC = ("IKR Agregat (Indeks Kesejahteraan Rakyat)", "f_ikr_dari_rekap_kk")
IKR_OVERALL_LABEL = "IKR Agregat"
PSEUDO_DIMENSION_COLS = tuple(IKR_DIMENSION_DISPLAY.values())


def format_dimension_source_label(col_name):
    if col_name == IKR_OVERALL_METRIC[1]:
        return "Skor IKR agregat"
    if col_name in IKR_DIMENSION_DISPLAY:
        return f"Skor dimensi {IKR_DIMENSION_DISPLAY[col_name]}"
    return str(col_name)


DRILLDOWN_DIMENSIONS = {
    "A": {
        "label": "Sandang, Pangan, dan Papan",
        "aggregate_col": "f_a_dari_rekap_kk",
        "variables": [
            {
                "code": "A1",
                "label": "Sandang",
                "description": "Seberapa sering keluarga membeli pakaian baru (indikator gaya hidup).",
                "candidates": ["a1_sandang", "a1", "f_a1", "f_a_1", "sandang"],
            },
            {
                "code": "A2",
                "label": "Pangan",
                "description": "Frekuensi makan dan gizi menu harian (indikator ketahanan pangan mikro).",
                "candidates": ["a2_pangan", "a2", "f_a2", "f_a_2", "pangan"],
            },
            {
                "code": "A3",
                "label": "Papan",
                "description": "Kualitas lantai, dinding, atap, dan sanitasi rumah (indikator aset fisik).",
                "candidates": ["a3_papan", "a3", "f_a3", "f_a_3", "papan"],
            },
        ],
    },
    "B": {
        "label": "Pendidikan",
        "aggregate_col": "f_b_dari_rekap_kk",
        "variables": [
            {
                "code": "B1",
                "label": "Lama Sekolah",
                "description": "Capaian ijazah KK (indikator modal intelektual).",
                "candidates": ["b1_lama_sekolah", "b1", "f_b1", "f_b_1", "lama_sekolah"],
            },
            {
                "code": "B2",
                "label": "Partisipasi Sekolah",
                "description": "Status sekolah anggota keluarga (indikator keberlanjutan pendidikan).",
                "candidates": ["b2_partisipasi", "b2", "f_b2", "f_b_2", "partisipasi_sekolah"],
            },
        ],
    },
    "C": {
        "label": "Sosial, Hukum, dan HAM",
        "aggregate_col": "f_c_dari_rekap_kk",
        "variables": [
            {
                "code": "C1",
                "label": "Kehidupan Sosial",
                "description": "Akses bansos dan partisipasi organisasi (indikator inklusi kebijakan).",
                "candidates": ["c1_kehidupan_sosial", "c1", "f_c1", "f_c_1", "kehidupan_sosial"],
            },
            {
                "code": "C2",
                "label": "Hukum dan HAM",
                "description": "Pengalaman kriminalitas dan bantuan hukum (indikator keamanan).",
                "candidates": ["c2_hukum_ham", "c2", "f_c2", "f_c_2", "hukum_ham"],
            },
        ],
    },
    "D": {
        "label": "Kesehatan dan Pekerjaan",
        "aggregate_col": "f_d_dari_rekap_kk",
        "variables": [
            {
                "code": "D1",
                "label": "Kesehatan",
                "description": "Riwayat penyakit berat dan disabilitas (indikator kerentanan fisik).",
                "candidates": ["d1_kesehatan", "d1", "f_d1", "f_d_1", "kesehatan"],
            },
            {
                "code": "D2",
                "label": "Pekerjaan",
                "description": "Status bekerja dan keterampilan (indikator produktivitas ekonomi).",
                "candidates": ["d2_pekerjaan", "d2", "f_d2", "f_d_2", "pekerjaan"],
            },
            {
                "code": "D3",
                "label": "Jaminan Sosial",
                "description": "Kepesertaan BPJS/JKN (indikator jaring pengaman).",
                "candidates": ["d3_jaminan_sosial", "d3", "f_d3", "f_d_3", "jaminan_sosial", "bpjs"],
            },
        ],
    },
    "E": {
        "label": "Lingkungan dan Infrastruktur",
        "aggregate_col": "f_e_dari_rekap_kk",
        "variables": [
            {
                "code": "E1",
                "label": "Lingkungan",
                "description": "Sumber air bersih dan pengelolaan sampah (indikator sanitasi lingkungan).",
                "candidates": ["e1_lingkungan", "e1", "f_e1", "f_e_1", "lingkungan"],
            },
            {
                "code": "E2",
                "label": "Infrastruktur",
                "description": "Akses listrik, ponsel, dan transportasi (indikator konektivitas digital).",
                "candidates": ["e2_infrastruktur", "e2", "f_e2", "f_e_2", "infrastruktur"],
            },
        ],
    },
}
def _normalize_text(val):
    return str(val).strip().lower() if pd.notnull(val) else ""


def to_binary_presence(val):
    v = _normalize_text(val)
    if v in {"0", "0.0", "tidak", "tidak ada", "none", "nan", ""}:
        return 0
    return 1


def to_binary_phone(val):
    v = _normalize_text(val)
    if v in {"ya", "yes", "1", "1.0", "true"}:
        return 1
    if v in {"tidak", "no", "0", "0.0", "false", "tidak ada", "none", "nan", ""}:
        return 0
    return 0


def _safe_float_metric(val, default=0.0):
    try:
        fval = float(val)
        return fval if np.isfinite(fval) else float(default)
    except Exception:
        return float(default)


def rgba_from_hex(color, alpha=0.28):
    color_text = str(color or "").strip()
    if color_text.startswith("rgba") or color_text.startswith("rgb"):
        return color_text
    if color_text.startswith("#") and len(color_text) in {4, 7}:
        if len(color_text) == 4:
            r = int(color_text[1] * 2, 16)
            g = int(color_text[2] * 2, 16)
            b = int(color_text[3] * 2, 16)
        else:
            r = int(color_text[1:3], 16)
            g = int(color_text[3:5], 16)
            b = int(color_text[5:7], 16)
        return f"rgba({r},{g},{b},{float(alpha):.3f})"
    return color_text or f"rgba(71,85,105,{float(alpha):.3f})"


def normalize_layout_positions(pos_dict, outer_quantile=0.88, clip_radius=1.35):
    if not pos_dict:
        return {}
    keys = list(pos_dict.keys())
    arr = np.array([pos_dict[k] for k in keys], dtype=float)
    if arr.ndim != 2 or arr.shape[1] != 2:
        return {k: np.array([0.0, 0.0]) for k in keys}
    center = np.nanmedian(arr, axis=0)
    arr = np.nan_to_num(arr - center, nan=0.0, posinf=0.0, neginf=0.0)
    radii = np.sqrt((arr ** 2).sum(axis=1))
    valid_radii = radii[np.isfinite(radii) & (radii > 1e-9)]
    scale_ref = float(np.quantile(valid_radii, outer_quantile)) if len(valid_radii) else 1.0
    scale_ref = max(scale_ref, 1e-9)
    arr = arr / scale_ref
    radii = np.sqrt((arr ** 2).sum(axis=1))
    too_far = radii > clip_radius
    if np.any(too_far):
        arr[too_far] = arr[too_far] / radii[too_far, None] * clip_radius
    return {k: arr[idx] for idx, k in enumerate(keys)}


def build_clustered_network_layout(graph_obj, partition=None, layout_spread=2.0, seed=42):
    if graph_obj is None or graph_obj.number_of_nodes() == 0:
        return {}
    nodes = list(graph_obj.nodes())
    if partition is None:
        partition = {n: graph_obj.nodes[n].get("cluster", 0) for n in nodes}
    cluster_nodes = {}
    for n in nodes:
        try:
            cid = int(partition.get(n, graph_obj.nodes[n].get("cluster", 0)))
        except (TypeError, ValueError):
            cid = 0
        cluster_nodes.setdefault(cid, []).append(n)

    cluster_ids = sorted(cluster_nodes)
    max_cluster_size = max(len(v) for v in cluster_nodes.values())
    spread = float(np.clip(layout_spread, 1.0, 3.4))

    cluster_graph = nx.Graph()
    for cid, c_nodes in cluster_nodes.items():
        cluster_graph.add_node(cid, size=len(c_nodes))
    for u, v, d in graph_obj.edges(data=True):
        cu = int(partition.get(u, graph_obj.nodes[u].get("cluster", 0)))
        cv = int(partition.get(v, graph_obj.nodes[v].get("cluster", 0)))
        if cu == cv:
            continue
        weight = _safe_float_metric(d.get("weight"), default=0.0)
        if cluster_graph.has_edge(cu, cv):
            cluster_graph[cu][cv]["weight"] += weight
        else:
            cluster_graph.add_edge(cu, cv, weight=weight)

    if len(cluster_ids) == 1:
        cluster_centers = {cluster_ids[0]: np.array([0.0, 0.0])}
    elif len(cluster_ids) == 2:
        cluster_centers = {
            cluster_ids[0]: np.array([-1.0, 0.0]),
            cluster_ids[1]: np.array([1.0, 0.0]),
        }
    else:
        cluster_centers = nx.circular_layout(cluster_graph, scale=1.0)

    pos_final = {}
    center_radius = 2.45 + (1.05 * spread)
    for idx, cid in enumerate(cluster_ids):
        c_nodes = cluster_nodes[cid]
        sub_g = graph_obj.subgraph(c_nodes).copy()
        if len(c_nodes) == 1:
            local_pos = {c_nodes[0]: np.array([0.0, 0.0])}
        elif len(c_nodes) <= 12:
            local_pos = nx.circular_layout(sub_g, scale=1.0)
        else:
            local_k = float(np.clip(5.0 / np.sqrt(len(c_nodes)), 0.38, 1.15))
            local_pos = nx.spring_layout(
                sub_g,
                seed=int(seed) + idx + 13,
                weight="weight",
                k=local_k,
                iterations=360,
                scale=1.0,
            )
        local_pos = normalize_layout_positions(local_pos, outer_quantile=0.86, clip_radius=1.42)
        size_ratio = np.sqrt(len(c_nodes) / max(max_cluster_size, 1))
        local_radius = (0.75 + (1.35 * size_ratio)) * (0.92 + (0.08 * spread))
        center = np.array(cluster_centers[cid], dtype=float) * center_radius
        for n in c_nodes:
            pos_final[n] = center + (np.array(local_pos.get(n, [0.0, 0.0]), dtype=float) * local_radius)

    return pos_final


def select_representative_edges(graph_obj, max_edges=900, per_node=1):
    if graph_obj is None or graph_obj.number_of_edges() == 0:
        return []
    all_edges = [
        (u, v, d, _safe_float_metric(d.get("weight"), default=0.0))
        for u, v, d in graph_obj.edges(data=True)
    ]
    limit = int(max(0, min(max_edges, len(all_edges))))
    if len(all_edges) <= limit:
        return [(u, v, d) for u, v, d, _ in sorted(all_edges, key=lambda x: x[3], reverse=True)]

    edge_lookup = {frozenset((u, v)): (u, v, d, w) for u, v, d, w in all_edges}
    selected_keys = set()
    if per_node > 0:
        for n in graph_obj.nodes():
            incident = [
                (frozenset((u, v)), w)
                for u, v, _, w in all_edges
                if u == n or v == n
            ]
            incident = sorted(incident, key=lambda x: x[1], reverse=True)[:per_node]
            for key, _ in incident:
                selected_keys.add(key)

    strongest = sorted(all_edges, key=lambda x: x[3], reverse=True)
    for u, v, _, _ in strongest:
        if len(selected_keys) >= limit:
            break
        selected_keys.add(frozenset((u, v)))

    selected = [edge_lookup[key] for key in selected_keys if key in edge_lookup]
    selected = sorted(selected, key=lambda x: x[3], reverse=True)[:limit]
    return [(u, v, d) for u, v, d, _ in selected]


def network_marker_size(n_nodes, base=8.0):
    n_nodes = int(max(n_nodes, 1))
    if n_nodes >= 700:
        return max(5.2, base - 3.0)
    if n_nodes >= 450:
        return max(5.8, base - 2.2)
    if n_nodes >= 250:
        return max(6.5, base - 1.3)
    return base


def centrality_marker_sizes(values, n_nodes):
    val_arr = np.asarray(values, dtype=float)
    if val_arr.size == 0:
        return []
    valid = val_arr[np.isfinite(val_arr)]
    if valid.size == 0:
        return [7.0 for _ in val_arr]
    lo = float(np.quantile(valid, 0.05))
    hi = float(np.quantile(valid, 0.95))
    if hi <= lo:
        lo = float(np.nanmin(valid))
        hi = float(np.nanmax(valid))
    denom = max(hi - lo, 1e-9)
    norm = np.clip((val_arr - lo) / denom, 0.0, 1.0)
    min_size = network_marker_size(n_nodes, base=7.2)
    max_size = min_size + (8.5 if n_nodes >= 350 else 12.0)
    return (min_size + ((max_size - min_size) * np.sqrt(norm))).tolist()


def add_network_edge_traces(
    fig,
    edge_items,
    pos,
    edge_min,
    edge_span,
    color_fn=None,
    base_width=0.32,
    width_scale=0.95,
    hover=False,
):
    for u, v, d in edge_items:
        if u not in pos or v not in pos:
            continue
        w = _safe_float_metric(d.get("weight"), default=0.0)
        w_norm = float(np.clip((w - edge_min) / max(edge_span, 1e-9), 0.0, 1.0))
        if color_fn:
            try:
                edge_color = color_fn(u, v, d, w_norm)
            except TypeError:
                edge_color = color_fn(u, v)
        else:
            edge_color = NETWORK_EDGE_NEUTRAL
        edge_width = base_width + (width_scale * w_norm)
        fig.add_trace(
            go.Scatter(
                x=[pos[u][0], pos[v][0], None],
                y=[pos[u][1], pos[v][1], None],
                mode="lines",
                line=dict(width=edge_width, color=edge_color),
                hovertemplate=f"Interaksi: {w:.4f}<extra></extra>" if hover else None,
                hoverinfo="text" if hover else "none",
                showlegend=False,
            )
        )


def resolve_basis_column(df_in, preferred_col):
    if preferred_col in df_in.columns:
        return preferred_col
    numeric_candidates = []
    for c in df_in.columns:
        if c in {"family_id", "cluster", "bansos_num", "digital_num", "organisasi_num"}:
            continue
        s = pd.to_numeric(df_in[c], errors="coerce")
        if s.notna().sum() >= max(3, int(0.2 * len(df_in))):
            numeric_candidates.append(c)
    priority = ["f_ikr_dari_rekap_kk", "ipm_mikro", "indeks_pengeluaran", "indeks_kesehatan", "indeks_pendidikan"]
    for p in priority:
        if p in numeric_candidates:
            return p
    return numeric_candidates[0] if numeric_candidates else None


def build_onehot_feature_matrix(df_builder, feature_cols, rounding_decimals=2):
    if not feature_cols:
        return pd.DataFrame(np.zeros((len(df_builder), 1)), columns=["__no_feature_cols__"], index=df_builder.index)
    rounding_decimals = int(rounding_decimals) if pd.notnull(rounding_decimals) else 2
    rounding_decimals = 2 if rounding_decimals not in {0, 1, 2} else rounding_decimals
    feat_df = df_builder[list(feature_cols)].copy()
    for col in feature_cols:
        # Jika numerik, bulatkan dulu sesuai opsi desimal agar kategori one-hot lebih stabil.
        raw_col = feat_df[col].replace(["", "nan", "None", "none"], np.nan)
        num_col = pd.to_numeric(raw_col, errors="coerce")
        if num_col.notna().any():
            fmt_num = num_col.round(rounding_decimals).map(
                lambda x: f"{x:.{rounding_decimals}f}" if pd.notnull(x) else "__MISSING__"
            )
            fmt_raw = raw_col.astype("string").fillna("__MISSING__")
            feat_df[col] = np.where(num_col.notna(), fmt_num, fmt_raw)
        else:
            feat_df[col] = raw_col.astype("string").fillna("__MISSING__")
    dummies = pd.get_dummies(
        feat_df,
        columns=list(feature_cols),
        prefix=list(feature_cols),
        prefix_sep="=",
        dummy_na=False,
        dtype=float,
    )
    if dummies.empty:
        return pd.DataFrame(np.zeros((len(df_builder), 1)), columns=["__all_missing__"], index=df_builder.index)
    return dummies

def compute_cosine_similarity(vec_i, vec_j):
    denom = float(np.linalg.norm(vec_i) * np.linalg.norm(vec_j))
    if denom <= 1e-12:
        return 0.0
    return float(np.clip(np.dot(vec_i, vec_j) / denom, 0.0, 1.0))


def compute_jaccard_similarity(vec_i, vec_j):
    b_i = vec_i > 0
    b_j = vec_j > 0
    union = int(np.logical_or(b_i, b_j).sum())
    if union == 0:
        return 0.0
    inter = int(np.logical_and(b_i, b_j).sum())
    return float(inter / union)


def compute_pearson_similarity(vec_i, vec_j):
    std_i = float(np.std(vec_i))
    std_j = float(np.std(vec_j))
    if std_i <= 1e-12 or std_j <= 1e-12:
        return 0.0
    corr = float(np.corrcoef(vec_i, vec_j)[0, 1])
    if not np.isfinite(corr):
        return 0.0
    return float(corr)


def compute_auto_threshold_from_distribution(sim_values, threshold_grid=None):
    if threshold_grid is None:
        threshold_grid = [round(x, 1) for x in np.arange(0.1, 1.0, 0.1)]
    sim_series = pd.Series(sim_values, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if sim_series.empty:
        n_candidates = max(len(threshold_grid), 1)
        table = [
            {
                "threshold": float(t),
                "edge_count": 0,
                "total_edge_kumulatif": 0,
                "rata2_edge_umum_total_bagi_kandidat": 0.0,
                "jumlah_kandidat_threshold": int(n_candidates),
            }
            for t in threshold_grid
        ]
        return 0.4, table
    counts = []
    for t in threshold_grid:
        counts.append(int((sim_series >= float(t)).sum()))
    n_candidates = max(len(threshold_grid), 1)
    total_edge_kumulatif = int(np.sum(counts))
    target_count = float(total_edge_kumulatif / n_candidates)
    best_idx = min(range(len(threshold_grid)), key=lambda i: (abs(counts[i] - target_count), abs(threshold_grid[i] - 0.5)))
    table = [
        {
            "threshold": float(threshold_grid[i]),
            "edge_count": int(counts[i]),
            "jarak_ke_rata2_edge": float(abs(counts[i] - target_count)),
            "total_edge_kumulatif": int(total_edge_kumulatif),
            "rata2_edge_umum_total_bagi_kandidat": float(target_count),
            "jumlah_kandidat_threshold": int(n_candidates),
        }
        for i in range(len(threshold_grid))
    ]
    return float(threshold_grid[best_idx]), table


def compute_threshold_sensitivity_analysis(
    node_ids,
    candidate_edges,
    threshold_grid=None,
    lcc_only=True,
    force_louvain_lcc=False,
):
    if threshold_grid is None:
        threshold_grid = [round(x, 1) for x in np.arange(0.1, 1.0, 0.1)]
    nodes = list(node_ids or [])
    pair_total = int(len(candidate_edges or []))
    rows = []

    for threshold in threshold_grid:
        threshold_float = float(threshold)
        graph_raw = nx.Graph()
        graph_raw.add_nodes_from(nodes)
        for u, v, sim_weight in candidate_edges:
            if float(sim_weight) >= threshold_float:
                graph_raw.add_edge(u, v, weight=float(sim_weight))

        if graph_raw.number_of_nodes() > 0:
            lcc_nodes = max(nx.connected_components(graph_raw), key=len)
            graph_lcc = graph_raw.subgraph(lcc_nodes).copy()
        else:
            graph_lcc = nx.Graph()

        graph_target = graph_lcc if lcc_only else graph_raw
        partition_graph = graph_lcc if force_louvain_lcc else graph_target
        modularity_q = 0.0
        cluster_count = 0

        if partition_graph.number_of_nodes() > 0:
            if partition_graph.number_of_edges() > 0:
                try:
                    partition_tmp = community_louvain.best_partition(partition_graph, weight="weight", random_state=42)
                    cluster_count = int(len(set(partition_tmp.values())))
                    modularity_q = _safe_float_metric(
                        community_louvain.modularity(partition_tmp, partition_graph, weight="weight"),
                        default=0.0,
                    )
                except Exception:
                    cluster_count = int(nx.number_connected_components(partition_graph))
                    modularity_q = 0.0
            else:
                cluster_count = int(partition_graph.number_of_nodes())

        edge_count = int(graph_raw.number_of_edges())
        rows.append(
            {
                "threshold": threshold_float,
                "edge_count": edge_count,
                "edge_ratio_pct": float((edge_count / pair_total) * 100.0) if pair_total else 0.0,
                "density_raw": float(nx.density(graph_raw)) if graph_raw.number_of_nodes() > 1 else 0.0,
                "komponen_raw": int(nx.number_connected_components(graph_raw)) if graph_raw.number_of_nodes() else 0,
                "node_lcc": int(graph_lcc.number_of_nodes()),
                "edge_lcc": int(graph_lcc.number_of_edges()),
                "density_analisis": float(nx.density(graph_target)) if graph_target.number_of_nodes() > 1 else 0.0,
                "jumlah_cluster": int(cluster_count),
                "modularity": float(modularity_q),
            }
        )

    return rows


def merge_threshold_distribution_with_sensitivity(distribution_rows, sensitivity_rows):
    distribution_lookup = {
        round(float(row.get("threshold", 0.0)), 6): dict(row)
        for row in (distribution_rows or [])
    }
    merged_rows = []
    for sensitivity_row in sensitivity_rows or []:
        key = round(float(sensitivity_row.get("threshold", 0.0)), 6)
        merged_row = dict(sensitivity_row)
        for col, val in distribution_lookup.get(key, {}).items():
            if col not in merged_row:
                merged_row[col] = val
        merged_rows.append(merged_row)
    return merged_rows


def get_threshold_sensitivity_dataframe(meta):
    rows = meta.get("threshold_sensitivity") or meta.get("threshold_distribution") or []
    df_sens = pd.DataFrame(rows)
    if df_sens.empty or "threshold" not in df_sens.columns:
        return pd.DataFrame()
    df_sens = df_sens.copy()
    df_sens["threshold"] = pd.to_numeric(df_sens["threshold"], errors="coerce")
    df_sens = df_sens.dropna(subset=["threshold"]).sort_values("threshold").reset_index(drop=True)
    threshold_selected = round(float(meta.get("threshold_selected", 0.0)), 6)
    df_sens["threshold_terpilih"] = df_sens["threshold"].round(6).eq(threshold_selected)
    return df_sens


def render_threshold_sensitivity_heatmap(df_sens):
    heatmap_cols = [
        ("edge_count", "Edge"),
        ("density_analisis", "Density"),
        ("modularity", "Modularity Q"),
        ("jumlah_cluster", "Jumlah Klaster"),
        ("komponen_raw", "Komponen Raw"),
    ]
    available_cols = [(col, label) for col, label in heatmap_cols if col in df_sens.columns]
    if not available_cols:
        return

    heat_display = pd.DataFrame(
        {
            label: pd.to_numeric(df_sens[col], errors="coerce").fillna(0.0).to_numpy()
            for col, label in available_cols
        },
        index=df_sens["threshold"].map(lambda x: f"{float(x):.1f}"),
    ).T

    heat_norm = heat_display.copy()
    for idx in heat_norm.index:
        row = pd.to_numeric(heat_norm.loc[idx], errors="coerce").fillna(0.0)
        span = float(row.max() - row.min())
        heat_norm.loc[idx] = 0.5 if span <= 1e-12 else (row - row.min()) / span

    text_values = heat_display.copy()
    for idx in text_values.index:
        if idx in {"Edge", "Jumlah Klaster", "Komponen Raw"}:
            text_values.loc[idx] = text_values.loc[idx].map(lambda x: f"{float(x):.0f}")
        else:
            text_values.loc[idx] = text_values.loc[idx].map(lambda x: f"{float(x):.4f}")

    fig_heat = go.Figure(
        data=go.Heatmap(
            z=heat_norm.astype(float).values,
            x=list(heat_norm.columns),
            y=list(heat_norm.index),
            text=text_values.values,
            texttemplate="%{text}",
            colorscale="RdYlGn",
            colorbar=dict(title="Skala relatif"),
            hovertemplate="Threshold %{x}<br>%{y}: %{text}<extra></extra>",
        )
    )
    fig_heat.update_layout(
        title="Heatmap Sensitivitas Seluruh Kandidat Threshold",
        xaxis_title="Threshold",
        yaxis_title="Metrik",
        template="plotly_white",
        height=max(330, 70 * len(heat_norm.index)),
    )
    st.plotly_chart(fig_heat, use_container_width=True, config=PLOTLY_DRAW_CONFIG)


def render_auto_threshold_summary(
    meta,
    graph_obj,
    partition,
    selected_desa=None,
    basis_col=None,
    method_label="-",
    kernel_info="-",
    rounding_label="-",
    compact=False,
):
    threshold_used = float(meta.get("threshold_selected", 0.0))
    df_sens = get_threshold_sensitivity_dataframe(meta)
    if df_sens.empty:
        st.info("Data kandidat threshold belum tersedia untuk dirangkum.")
        return

    selected_rows = df_sens[df_sens["threshold_terpilih"]]
    selected_row = selected_rows.iloc[0].to_dict() if not selected_rows.empty else {}
    total_pair = int(len(meta.get("pairwise_similarity_values", [])))
    total_edge_kumulatif = int(df_sens["edge_count"].sum()) if "edge_count" in df_sens.columns else 0
    jumlah_kandidat = int(len(df_sens))
    rata2_edge_umum = float(total_edge_kumulatif / max(jumlah_kandidat, 1))
    jarak_terpilih = float(selected_row.get("jarak_ke_rata2_edge", abs(float(selected_row.get("edge_count", 0.0)) - rata2_edge_umum)))
    density_default = float(nx.density(graph_obj)) if graph_obj.number_of_nodes() > 1 else 0.0
    modularity_default = 0.0
    if graph_obj.number_of_edges() > 0 and partition:
        try:
            modularity_default = _safe_float_metric(community_louvain.modularity(partition, graph_obj, weight="weight"), default=0.0)
        except Exception:
            modularity_default = 0.0
    cluster_default = int(len(set(partition.values()))) if partition else 0

    if not compact:
        st.markdown(
            f"<div class='premium-hero'><b>Rangkuman Sensitivity Analysis Threshold Otomatis</b><br>"
            f"Desa: <b>{selected_desa or '-'}</b> | Basis: <b>{basis_col or '-'}</b> | "
            f"Metode: <b>{method_label}</b> ({kernel_info}) | One-Hot Rounding: <b>{rounding_label}</b><br>"
            f"Threshold otomatis dipilih dari {jumlah_kandidat} kandidat dengan membandingkan jumlah edge yang terbentuk "
            f"pada tiap nilai ambang.</div>",
            unsafe_allow_html=True,
        )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Threshold Terpilih", f"{threshold_used:.2f}")
    c2.metric("Edge Terpilih", f"{int(selected_row.get('edge_count', graph_obj.number_of_edges())):,}")
    c3.metric("Density Analisis", f"{float(selected_row.get('density_analisis', density_default)):.4f}")
    c4.metric("Modularity Q", f"{float(selected_row.get('modularity', modularity_default)):.4f}")
    c5.metric("Jumlah Klaster", f"{int(selected_row.get('jumlah_cluster', cluster_default))}")

    st.markdown(
        f"<div class='soft-card'><b>Justifikasi threshold otomatis.</b><br>"
        f"Ambang <b>{threshold_used:.2f}</b> dipilih karena jumlah edge pada kandidat ini paling dekat dengan "
        f"rata-rata edge kumulatif seluruh kandidat ({rata2_edge_umum:.2f}). "
        f"Jarak kandidat terpilih terhadap rata-rata tersebut adalah <b>{jarak_terpilih:.2f} edge</b>. "
        f"Tabel dan heatmap di bawah memperlihatkan bagaimana density, modularity, dan jumlah klaster berubah "
        f"di semua kandidat, sehingga hasil graf tidak hanya bergantung pada satu angka yang tampak subjektif.</div>",
        unsafe_allow_html=True,
    )

    chart_cols = st.columns([1.0, 1.0])
    with chart_cols[0]:
        fig_edges = px.line(
            df_sens,
            x="threshold",
            y="edge_count",
            markers=True,
            title="Threshold vs Jumlah Edge",
            labels={"threshold": "Threshold", "edge_count": "Jumlah Edge"},
        )
        fig_edges.add_hline(
            y=rata2_edge_umum,
            line_dash="dash",
            line_color=DDP_RED,
            annotation_text=f"Rata-rata edge = {rata2_edge_umum:.2f}",
        )
        fig_edges.add_vline(
            x=threshold_used,
            line_dash="dash",
            line_color="#111827",
            annotation_text=f"Terpilih {threshold_used:.2f}",
        )
        fig_edges.update_layout(template="plotly_white")
        st.plotly_chart(fig_edges, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
    with chart_cols[1]:
        trend_cols = [
            ("density_analisis", "Density"),
            ("modularity", "Modularity Q"),
            ("jumlah_cluster", "Jumlah Klaster"),
        ]
        trend_df = df_sens[["threshold"] + [col for col, _ in trend_cols if col in df_sens.columns]].copy()
        trend_long_rows = []
        for col, label in trend_cols:
            if col not in trend_df.columns:
                continue
            vals = pd.to_numeric(trend_df[col], errors="coerce").fillna(0.0)
            span = float(vals.max() - vals.min())
            norm_vals = pd.Series(0.5, index=vals.index) if span <= 1e-12 else (vals - vals.min()) / span
            for idx, val in norm_vals.items():
                trend_long_rows.append({"threshold": float(trend_df.loc[idx, "threshold"]), "Metrik": label, "Nilai Relatif": float(val)})
        trend_long = pd.DataFrame(trend_long_rows)
        if not trend_long.empty:
            fig_trend = px.line(
                trend_long,
                x="threshold",
                y="Nilai Relatif",
                color="Metrik",
                markers=True,
                title="Tren Relatif Density, Modularity, dan Klaster",
            )
            fig_trend.add_vline(x=threshold_used, line_dash="dash", line_color="#111827")
            fig_trend.update_layout(template="plotly_white", yaxis_title="Nilai relatif 0-1")
            st.plotly_chart(fig_trend, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    render_threshold_sensitivity_heatmap(df_sens)

    table_cols = [
        ("threshold", "Threshold"),
        ("edge_count", "Jumlah Edge Raw"),
        ("edge_ratio_pct", "Rasio Edge (%)"),
        ("density_raw", "Density Raw"),
        ("komponen_raw", "Komponen Raw"),
        ("node_lcc", "Node LCC"),
        ("edge_lcc", "Edge LCC"),
        ("density_analisis", "Density Analisis"),
        ("modularity", "Modularity Q"),
        ("jumlah_cluster", "Jumlah Klaster"),
        ("jarak_ke_rata2_edge", "Jarak ke Rata-rata Edge"),
        ("threshold_terpilih", "Terpilih"),
    ]
    display_cols = [col for col, _ in table_cols if col in df_sens.columns]
    df_display = df_sens[display_cols].copy()
    rename_map = {col: label for col, label in table_cols if col in df_display.columns}
    df_display = df_display.rename(columns=rename_map)
    if "Terpilih" in df_display.columns:
        df_display["Terpilih"] = df_display["Terpilih"].map(lambda x: "Ya" if bool(x) else "")

    def _highlight_selected_threshold(row):
        if str(row.get("Terpilih", "")).lower() == "ya":
            return ["background-color: #DCFCE7; color: #14532D; font-weight: 700;"] * len(row)
        return [""] * len(row)

    format_cols = {
        "Threshold": "{:.2f}",
        "Rasio Edge (%)": "{:.2f}",
        "Density Raw": "{:.4f}",
        "Density Analisis": "{:.4f}",
        "Modularity Q": "{:.4f}",
        "Jarak ke Rata-rata Edge": "{:.2f}",
    }
    st.dataframe(
        df_display.style.format({k: v for k, v in format_cols.items() if k in df_display.columns}).apply(_highlight_selected_threshold, axis=1),
        use_container_width=True,
    )


def safe_numeric_assortativity(graph_obj, attr_name, default=0.0):
    if graph_obj is None or graph_obj.number_of_nodes() < 2 or graph_obj.number_of_edges() == 0:
        return float(default)
    raw_series = pd.Series({n: graph_obj.nodes[n].get(attr_name) for n in graph_obj.nodes()})
    num_series = pd.to_numeric(raw_series, errors="coerce")
    valid_nodes = [n for n in graph_obj.nodes() if pd.notnull(num_series.get(n))]
    if len(valid_nodes) < 2:
        return float(default)
    g_sub = graph_obj.subgraph(valid_nodes).copy()
    if g_sub.number_of_edges() == 0:
        return float(default)
    for n in g_sub.nodes():
        g_sub.nodes[n][attr_name] = float(num_series.get(n))
    vals = pd.Series([g_sub.nodes[n].get(attr_name) for n in g_sub.nodes()], dtype=float)
    if vals.nunique() <= 1:
        return float(default)
    try:
        return _safe_float_metric(nx.numeric_assortativity_coefficient(g_sub, attr_name), default=default)
    except Exception:
        return float(default)


def interpret_assortativity_value(r_val):
    r = _safe_float_metric(r_val, default=0.0)
    abs_r = abs(r)
    if abs_r < 0.10:
        level = "Sangat lemah"
    elif abs_r < 0.30:
        level = "Lemah"
    elif abs_r < 0.50:
        level = "Sedang"
    else:
        level = "Kuat"
    direction = "Asortatif" if r > 0 else "Disasortatif" if r < 0 else "Campuran/Acak"
    return direction, level


def safe_attribute_assortativity(graph_obj, attr_name, default=0.0):
    if graph_obj is None or graph_obj.number_of_nodes() < 2 or graph_obj.number_of_edges() == 0:
        return float(default)
    valid_nodes = [n for n in graph_obj.nodes() if pd.notnull(graph_obj.nodes[n].get(attr_name))]
    if len(valid_nodes) < 2:
        return float(default)
    g_sub = graph_obj.subgraph(valid_nodes).copy()
    if g_sub.number_of_edges() == 0:
        return float(default)
    vals = pd.Series([g_sub.nodes[n].get(attr_name) for n in g_sub.nodes()])
    if vals.nunique() <= 1:
        return float(default)
    try:
        return _safe_float_metric(nx.attribute_assortativity_coefficient(g_sub, attr_name), default=default)
    except Exception:
        return float(default)


def steinley_segregation_label(r_val):
    r_abs = abs(_safe_float_metric(r_val, default=0.0))
    if r_abs < 0.10:
        return "Low Segregation"
    if r_abs < 0.30:
        return "Moderate Segregation"
    return "High Segregation"


def interpret_q_strength(q_val):
    q = _safe_float_metric(q_val, default=0.0)
    a = abs(q)
    if a < 0.10:
        return "lemah"
    if a < 0.30:
        return "cukup"
    if a < 0.50:
        return "sedang"
    return "kuat"


def build_audit_auto_narrative(df_audit):
    if df_audit is None or df_audit.empty:
        return "Narasi audit belum dapat dibuat karena tabel audit kosong."
    lines = []
    for _, row in df_audit.iterrows():
        metric = row.get("Metrik", "-")
        r_val = _safe_float_metric(row.get("r"), default=0.0)
        qw_val = _safe_float_metric(row.get("Qw*"), default=0.0)
        qb_val = _safe_float_metric(row.get("Qb*"), default=0.0)
        dir_r, lvl_r = interpret_assortativity_value(r_val)
        if qw_val >= 0.10:
            intra_note = f"intra-klaster {interpret_q_strength(qw_val)} homogen"
        elif qw_val <= -0.10:
            intra_note = "intra-klaster cenderung lintas kategori"
        else:
            intra_note = "intra-klaster campuran/netral"
        if qb_val >= 0.10:
            inter_note = f"antar-klaster {interpret_q_strength(qb_val)} homogen"
        elif qb_val <= -0.10:
            inter_note = "antar-klaster cenderung berbeda kategori"
        else:
            inter_note = "antar-klaster campuran/netral"
        lines.append(
            f"- <b>{metric}</b>: r=<b>{r_val:.3f}</b> ({dir_r}, {lvl_r}); Qw*=<b>{qw_val:.3f}</b> ({intra_note}); Qb*=<b>{qb_val:.3f}</b> ({inter_note})."
        )
    return "<br>".join(lines)


def resolve_first_existing_column(df_columns, candidates):
    lookup = {str(c).lower().strip(): c for c in df_columns}
    for cand in candidates:
        key = str(cand).lower().strip()
        if key in lookup:
            return lookup[key]
    return None


def compute_assortativity_for_column(graph_obj, col_name):
    node_vals = [graph_obj.nodes[n].get(col_name) for n in graph_obj.nodes()]
    num_series = pd.to_numeric(pd.Series(node_vals), errors="coerce")
    if num_series.notna().sum() >= 3 and num_series.nunique(dropna=True) > 1:
        return safe_numeric_assortativity(graph_obj, col_name, default=0.0), "numeric"
    return safe_attribute_assortativity(graph_obj, col_name, default=0.0), "attribute"


def centrality_help_text(metric_key):
    mapping = {
        "degree": "Degree: seberapa banyak dan seberapa kuat koneksi langsung node.",
        "betweenness": "Betweenness: seberapa sering node menjadi jembatan jalur terpendek antar-node.",
        "closeness": "Closeness: seberapa dekat node ke node lain (rata-rata jarak paling pendek).",
        "eigenvector": "Eigenvector: node penting jika terhubung ke node penting lainnya.",
    }
    return mapping.get(metric_key, "-")


def compute_centrality_on_similarity_graph(graph_obj, metric_key):
    if graph_obj is None or graph_obj.number_of_nodes() == 0:
        return {}
    metric_key = str(metric_key).strip().lower()
    if metric_key == "degree":
        return {n: float(graph_obj.degree(n, weight="weight")) for n in graph_obj.nodes()}

    # Untuk metrik shortest-path, similarity diubah jadi jarak: distance = 1 / similarity.
    graph_dist = graph_obj.copy()
    for u, v, d in graph_dist.edges(data=True):
        w = _safe_float_metric(d.get("weight"), default=0.0)
        d["distance"] = 1.0 / max(w, 1e-9)

    if metric_key == "betweenness":
        return nx.betweenness_centrality(graph_dist, weight="distance", normalized=True)
    if metric_key == "closeness":
        return nx.closeness_centrality(graph_dist, distance="distance")
    if metric_key == "eigenvector":
        try:
            return nx.eigenvector_centrality_numpy(graph_obj, weight="weight")
        except Exception:
            return nx.eigenvector_centrality(graph_obj, weight="weight", max_iter=2000, tol=1e-6)
    return {}


CENTRALITY_ROLE_COLORS = {
    "Penghubung informasi potensial": "#2563EB",
    "Jembatan antar-kelompok": "#7C3AED",
    "Prioritas verifikasi berbasis data": "#F59E0B",
    "Rentan dan relatif terisolasi": "#DC2626",
    "Node umum": "#94A3B8",
}

CENTRALITY_ROLE_ORDER = [
    "Prioritas verifikasi berbasis data",
    "Rentan dan relatif terisolasi",
    "Jembatan antar-kelompok",
    "Penghubung informasi potensial",
    "Node umum",
]


def make_anonymized_node_mapping(node_ids):
    stable_ids = sorted({str(n) for n in (node_ids or [])})
    return {node_id: f"N-{idx + 1:03d}" for idx, node_id in enumerate(stable_ids)}


def apply_privacy_view(df, id_col="family_id", name_col="Nama", publish_mode=True):
    if df is None:
        return pd.DataFrame()
    result = df.copy()
    if "Kode Node" not in result.columns:
        if id_col in result.columns:
            anon_map = make_anonymized_node_mapping(result[id_col].dropna().astype(str).tolist())
            result["Kode Node"] = result[id_col].astype(str).map(anon_map).fillna("N-000")
        else:
            result["Kode Node"] = [f"N-{idx + 1:03d}" for idx in range(len(result))]

    if "Dusun/Kode Dusun" not in result.columns and "Dusun" in result.columns:
        if publish_mode:
            dusun_vals = sorted(result["Dusun"].fillna("Tidak tersedia").astype(str).unique().tolist())
            dusun_map = {val: f"Dusun-{idx + 1}" for idx, val in enumerate(dusun_vals)}
            result["Dusun/Kode Dusun"] = result["Dusun"].fillna("Tidak tersedia").astype(str).map(dusun_map)
        else:
            result["Dusun/Kode Dusun"] = result["Dusun"].fillna("Tidak tersedia").astype(str)

    if publish_mode:
        drop_cols = [col for col in [id_col, name_col, "Dusun"] if col in result.columns]
        if drop_cols:
            result = result.drop(columns=drop_cols)
    return result


def safe_hover_text(row, publish_mode=True):
    def pick(*cols, default="-"):
        for col in cols:
            if col in row and pd.notnull(row.get(col)):
                value = row.get(col)
                if str(value).strip() != "":
                    return value
        return default

    parts = []
    if publish_mode:
        parts.append(f"Kode Node: {pick('Kode Node')}")
    else:
        parts.append(f"Nama: {pick('Nama')}")
        parts.append(f"family_id: {pick('family_id')}")
        parts.append(f"Kode Node: {pick('Kode Node')}")
    parts.extend(
        [
            f"Klaster Louvain: {pick('Klaster Louvain')}",
            f"Dusun/Kode Dusun: {pick('Dusun/Kode Dusun', 'Dusun')}",
            f"IKR Agregat: {_safe_float_metric(pick('IKR Agregat', default=np.nan), default=np.nan):.3f}",
            f"Status BPS: {pick('Status BPS')}",
            f"Status Bansos: {pick('Status Bansos')}",
            f"Akses Informasi: {pick('Akses Informasi')}",
            f"Peran Struktural: {pick('Peran Struktural')}",
        ]
    )
    for col in ["Degree Centrality", "Betweenness Centrality", "Closeness Centrality", "Eigenvector Centrality"]:
        if col in row:
            parts.append(f"{col}: {_safe_float_metric(row.get(col), default=0.0):.6f}")
    parts.append("Catatan: indikasi awal, perlu pendalaman lapangan, bukan bukti tunggal.")
    return "<br>".join(parts)


def centrality_level_from_quantile(value, q25, q75):
    val = _safe_float_metric(value, default=np.nan)
    if not np.isfinite(val):
        return "Tidak tersedia"
    q25_val = _safe_float_metric(q25, default=val)
    q75_val = _safe_float_metric(q75, default=val)
    if abs(q75_val - q25_val) <= 1e-12:
        return "Sedang"
    if val >= q75_val:
        return "Tinggi"
    if val <= q25_val:
        return "Rendah"
    return "Sedang"


def ikr_level_from_value(ikr_value, status_bps=None):
    status_text = str(status_bps or "").strip().lower()
    ikr = _safe_float_metric(ikr_value, default=np.nan)
    if status_text == "rendah" or (np.isfinite(ikr) and ikr < 60):
        return "Rendah"
    if status_text == "sedang" or (np.isfinite(ikr) and 60 <= ikr < 70):
        return "Sedang"
    if status_text in {"tinggi", "sangat tinggi"} or (np.isfinite(ikr) and ikr >= 70):
        return "Tinggi"
    return "Tidak tersedia"


def access_info_label(row):
    internet_available = "internet_num" in row and pd.notnull(row.get("internet_num"))
    ponsel_available = "ponsel_num" in row and pd.notnull(row.get("ponsel_num"))
    internet_val = _safe_float_metric(row.get("internet_num"), default=0.0)
    ponsel_val = _safe_float_metric(row.get("ponsel_num"), default=0.0)
    if not internet_available and not ponsel_available:
        return "Tidak tersedia"
    if internet_val >= 1 or ponsel_val >= 1:
        return "Tersedia"
    return "Tidak tersedia pada data"


def centrality_role_implication(role):
    mapping = {
        "Penghubung informasi potensial": "Dapat dipertimbangkan sebagai kanal penyebaran informasi program desa, dengan persetujuan dan konteks sosial yang tepat.",
        "Jembatan antar-kelompok": "Berpotensi membantu menjangkau beberapa klaster, tetapi perlu dipahami bersama struktur sosial lokal.",
        "Prioritas verifikasi berbasis data": "Masuk daftar awal untuk verifikasi atau pendampingan; bukan bukti tunggal ketepatan sasaran.",
        "Rentan dan relatif terisolasi": "Perlu pendekatan langsung karena posisinya tidak menonjol dalam jaringan informasi.",
        "Node umum": "Tetap dibaca sebagai bagian struktur jaringan, tanpa implikasi prioritas khusus dari centrality.",
    }
    return mapping.get(role, mapping["Node umum"])


def centrality_role_ethics_note(role=None):
    if role == "Prioritas verifikasi berbasis data":
        return "Indikasi awal untuk pendalaman lapangan; tidak boleh dimaknai sebagai bukti tunggal ketepatan sasaran."
    return "Gunakan sebagai alat bantu evaluasi; hindari label sosial dan tetap perlukan consent/verifikasi."


def classify_centrality_policy_role(row, centrality_col, betweenness_col=None):
    centrality_val = _safe_float_metric(row.get(centrality_col), default=0.0)
    centrality_q75 = _safe_float_metric(row.get("_centrality_q75"), default=centrality_val)
    centrality_q25 = _safe_float_metric(row.get("_centrality_q25"), default=centrality_val)
    bet_col = betweenness_col or "Betweenness Centrality"
    bet_val = _safe_float_metric(row.get(bet_col), default=0.0)
    bet_q75 = _safe_float_metric(row.get("_betweenness_q75"), default=bet_val)
    ikr_level = str(row.get("Level IKR", "")).strip()
    access_available = str(row.get("Akses Informasi", "")).strip().lower() == "tersedia"
    status_bansos = str(row.get("Status Bansos", "")).strip().lower()

    centrality_has_spread = abs(centrality_q75 - centrality_q25) > 1e-12
    centrality_high = centrality_has_spread and centrality_val >= centrality_q75
    centrality_low = centrality_has_spread and centrality_val <= centrality_q25
    betweenness_high = bet_q75 > 0 and bet_val >= bet_q75
    ikr_low_or_mid = ikr_level in {"Rendah", "Sedang"}
    ikr_low = ikr_level == "Rendah"
    not_receiving_support = status_bansos in {"tidak menerima", "belum menerima", "tidak"}

    if centrality_high and ikr_low_or_mid and not_receiving_support:
        return "Prioritas verifikasi berbasis data"
    if centrality_low and ikr_low:
        return "Rentan dan relatif terisolasi"
    if betweenness_high:
        return "Jembatan antar-kelompok"
    if centrality_high and access_available:
        return "Penghubung informasi potensial"
    if centrality_high and ikr_low_or_mid:
        return "Prioritas verifikasi berbasis data"
    return "Node umum"


def build_centrality_policy_narrative(df_role, centrality_name):
    if df_role is None or df_role.empty:
        return "Analisis centrality belum memiliki node yang cukup untuk ditafsirkan."
    role_counts = df_role["Peran Struktural"].value_counts()
    strategic_mask = df_role["Peran Struktural"].ne("Node umum")
    n_strategic = int(strategic_mask.sum())
    n_info = int(role_counts.get("Penghubung informasi potensial", 0))
    n_verify = int(role_counts.get("Prioritas verifikasi berbasis data", 0))
    n_isolated = int(role_counts.get("Rentan dan relatif terisolasi", 0))
    return (
        f"Analisis {centrality_name} menunjukkan terdapat {n_strategic} node dengan posisi struktural yang perlu dibaca lebih lanjut. "
        f"Sebanyak {n_info} node memiliki akses informasi yang tersedia sehingga berpotensi menjadi penghubung program desa. "
        f"Sebanyak {n_verify} node masuk kategori prioritas verifikasi berbasis data, dan {n_isolated} node menunjukkan kondisi kesejahteraan relatif rendah dengan posisi jaringan yang relatif tidak menonjol. "
        "Hasil ini digunakan sebagai alat bantu evaluasi dan bukan sebagai dasar tunggal penetapan status sosial atau kelayakan bantuan."
    )


def unique_existing_columns(df, columns):
    return [col for col in dict.fromkeys(columns) if col in df.columns]


def prepare_centrality_policy_dataframe(graph_obj, partition, selected_centrality_key, dusun_attr, publish_mode=True):
    centrality_name = {
        "degree": "Degree Centrality",
        "betweenness": "Betweenness Centrality",
        "closeness": "Closeness Centrality",
        "eigenvector": "Eigenvector Centrality",
    }.get(selected_centrality_key, "Degree Centrality")
    all_centrality_specs = [
        ("Degree Centrality", "degree"),
        ("Betweenness Centrality", "betweenness"),
        ("Closeness Centrality", "closeness"),
        ("Eigenvector Centrality", "eigenvector"),
    ]
    centrality_metric_values = {
        metric_label: compute_centrality_on_similarity_graph(graph_obj, metric_key)
        for metric_label, metric_key in all_centrality_specs
    }
    node_ids_local = list(graph_obj.nodes())
    anon_node_map = make_anonymized_node_mapping(node_ids_local)
    dusun_values = sorted(
        {
            str(graph_obj.nodes[n].get(dusun_attr, "Tidak tersedia"))
            for n in node_ids_local
        }
    )
    dusun_code_map = {val: f"Dusun-{idx + 1}" for idx, val in enumerate(dusun_values)}
    rows = []
    for n in node_ids_local:
        n_attr = graph_obj.nodes[n]
        profesi_raw = n_attr.get(
            "profesi pekerjaan",
            n_attr.get("profesi_pekerjaan", n_attr.get("pekerjaan", n_attr.get("profesi", "Tidak diketahui"))),
        )
        bansos_status = "Penerima" if int(_safe_float_metric(n_attr.get("bansos_num"), default=0.0) > 0) == 1 else "Tidak Menerima"
        row = {
            "family_id": n,
            "Nama": n_attr.get("nama", "-"),
            "Kode Node": anon_node_map.get(str(n), "N-000"),
            "Klaster Louvain": int(partition.get(n, -1)),
            "Dusun": n_attr.get(dusun_attr, "-"),
            "Dusun/Kode Dusun": (
                dusun_code_map.get(str(n_attr.get(dusun_attr, "Tidak tersedia")), "Dusun-0")
                if publish_mode
                else str(n_attr.get(dusun_attr, "-"))
            ),
            "Profesi/Pekerjaan": str(profesi_raw).strip() if pd.notnull(profesi_raw) else "Tidak diketahui",
            "Status Bansos": bansos_status,
            "IKR Agregat": _safe_float_metric(n_attr.get("f_ikr_dari_rekap_kk"), default=np.nan),
            "internet_num": n_attr.get("internet_num", n_attr.get("digital_num", np.nan)),
            "ponsel_num": n_attr.get("ponsel_num", np.nan),
        }
        for metric_label, _ in all_centrality_specs:
            row[metric_label] = float(centrality_metric_values.get(metric_label, {}).get(n, 0.0))
        row["Status BPS"] = n_attr.get("kategori_ikr", categorize_ikr_bps(row["IKR Agregat"])[0])
        for dim_label, dim_col in IKR_DIMENSION_MAP:
            row[dim_label] = _safe_float_metric(n_attr.get(dim_col), default=np.nan)
        rows.append(row)

    df_role = pd.DataFrame(rows)
    if df_role.empty:
        return df_role, centrality_name, all_centrality_specs

    c_series = pd.to_numeric(df_role[centrality_name], errors="coerce").fillna(0.0)
    b_series = pd.to_numeric(df_role["Betweenness Centrality"], errors="coerce").fillna(0.0)
    c_q25 = float(c_series.quantile(0.25)) if not c_series.empty else 0.0
    c_q75 = float(c_series.quantile(0.75)) if not c_series.empty else 0.0
    b_q75 = float(b_series.quantile(0.75)) if not b_series.empty else 0.0
    df_role["_centrality_q25"] = c_q25
    df_role["_centrality_q75"] = c_q75
    df_role["_betweenness_q75"] = b_q75
    df_role["Level Centrality"] = df_role[centrality_name].map(lambda v: centrality_level_from_quantile(v, c_q25, c_q75))
    df_role["Level IKR"] = df_role.apply(lambda r: ikr_level_from_value(r.get("IKR Agregat"), r.get("Status BPS")), axis=1)
    df_role["Akses Informasi"] = df_role.apply(access_info_label, axis=1)
    df_role["Peran Struktural"] = df_role.apply(
        lambda r: classify_centrality_policy_role(r, centrality_col=centrality_name, betweenness_col="Betweenness Centrality"),
        axis=1,
    )
    df_role["Implikasi Program"] = df_role["Peran Struktural"].map(centrality_role_implication)
    df_role["Catatan Etika"] = df_role["Peran Struktural"].map(centrality_role_ethics_note)
    df_role["Centrality terpilih"] = df_role[centrality_name]
    df_role["Hover Aman"] = df_role.apply(lambda r: safe_hover_text(r, publish_mode=publish_mode), axis=1)
    return df_role.sort_values(centrality_name, ascending=False).reset_index(drop=True), centrality_name, all_centrality_specs


def render_centrality_analysis_page(
    graph_obj,
    partition,
    df_v,
    selected_desa,
    selected_centrality_key,
    col_spasial,
    layout_spread=2.2,
):
    st.markdown(f"<h1 class='main-header'>Analisis Centrality: {selected_desa}</h1>", unsafe_allow_html=True)
    publish_mode = st.toggle("Mode publikasi / anonimisasi", value=True, key=f"centrality_page_publish_{selected_centrality_key}")
    highlight_roles = st.toggle("Highlight Peran Struktural", value=True, key=f"centrality_page_highlight_{selected_centrality_key}")
    dusun_attr = "dusun" if "dusun" in df_v.columns else col_spasial
    df_role, centrality_name, all_centrality_specs = prepare_centrality_policy_dataframe(
        graph_obj,
        partition,
        selected_centrality_key,
        dusun_attr,
        publish_mode=publish_mode,
    )
    if df_role.empty:
        st.info("Nilai centrality belum bisa dihitung untuk graf saat ini.")
        return

    cluster_opts = sorted(df_role["Klaster Louvain"].dropna().unique().tolist())
    dusun_filter_col = "Dusun/Kode Dusun" if publish_mode else "Dusun"
    dusun_opts = sorted(df_role[dusun_filter_col].fillna("Tidak tersedia").astype(str).unique().tolist())
    f1, f2 = st.columns(2)
    with f1:
        selected_clusters = st.multiselect("Pilih Klaster", options=cluster_opts, default=cluster_opts, key=f"cent_page_cluster_{selected_centrality_key}")
    with f2:
        selected_dusun = st.multiselect("Pilih Dusun/Kode Dusun", options=dusun_opts, default=dusun_opts, key=f"cent_page_dusun_{selected_centrality_key}")
    df_view = df_role[
        df_role["Klaster Louvain"].isin(selected_clusters)
        & df_role[dusun_filter_col].astype(str).isin([str(x) for x in selected_dusun])
    ].copy()
    if df_view.empty:
        st.warning("Filter klaster/dusun tidak memiliki node. Silakan ubah filter.")
        return

    selected_node_set = set(df_view["family_id"].tolist())
    graph_view = graph_obj.subgraph(selected_node_set).copy()
    df_view, centrality_name, all_centrality_specs = prepare_centrality_policy_dataframe(
        graph_view,
        {n: partition.get(n, -1) for n in graph_view.nodes()},
        selected_centrality_key,
        dusun_attr,
        publish_mode=publish_mode,
    )
    df_view = df_view.sort_values(centrality_name, ascending=False).reset_index(drop=True)
    node_ids_view = list(graph_view.nodes())
    pos = build_clustered_network_layout(graph_view, partition=partition, layout_spread=layout_spread, seed=42)
    edge_weights = [_safe_float_metric(d.get("weight"), default=0.0) for _, _, d in graph_view.edges(data=True)]
    edge_min = float(min(edge_weights)) if edge_weights else 0.0
    edge_max = float(max(edge_weights)) if edge_weights else 1.0
    edge_span = max(edge_max - edge_min, 1e-9)
    visible_edges = select_representative_edges(graph_view, max_edges=int(np.clip(graph_view.number_of_nodes() * 1.25, 120, 650)), per_node=1)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Node Terpilih", f"{len(df_view):,}")
    c2.metric("Edge Terpilih", f"{graph_view.number_of_edges():,}")
    c3.metric("Nilai Tertinggi", f"{float(df_view[centrality_name].max()):.6f}")
    c4.metric("Node Strategis", f"{int(df_view['Peran Struktural'].ne('Node umum').sum()):,}")

    st.markdown(f"#### Visual Jaringan Centrality ({centrality_name})")
    fig_cent = go.Figure()
    add_network_edge_traces(
        fig_cent,
        visible_edges,
        pos,
        edge_min,
        edge_span,
        color_fn=lambda *_args, **_kwargs: "rgba(148, 163, 184, 0.24)",
        base_width=0.26,
        width_scale=0.72,
        hover=False,
    )
    lookup = df_view.set_index("family_id")
    node_order = [n for n in node_ids_view if n in lookup.index and n in pos]
    node_vals = np.array([float(lookup.loc[n, centrality_name]) for n in node_order], dtype=float)
    size_vals = centrality_marker_sizes(node_vals, len(node_order))
    if highlight_roles:
        for role in [r for r in CENTRALITY_ROLE_ORDER if r in set(df_view["Peran Struktural"].astype(str))]:
            role_nodes = [n for n in node_order if str(lookup.loc[n, "Peran Struktural"]) == role]
            if not role_nodes:
                continue
            fig_cent.add_trace(
                go.Scatter(
                    x=[pos[n][0] for n in role_nodes],
                    y=[pos[n][1] for n in role_nodes],
                    mode="markers",
                    marker=dict(
                        size=[size_vals[node_order.index(n)] for n in role_nodes],
                        color=CENTRALITY_ROLE_COLORS.get(role, "#94A3B8"),
                        opacity=0.32 if role == "Node umum" else 0.9,
                        line=dict(color=NETWORK_NODE_LINE, width=0.7),
                    ),
                    text=[safe_hover_text(lookup.loc[n], publish_mode=publish_mode) for n in role_nodes],
                    hoverinfo="text",
                    name=role,
                )
            )
    else:
        cmin = float(np.nanmin(node_vals)) if len(node_vals) else 0.0
        cmax = float(np.nanmax(node_vals)) if len(node_vals) else 1.0
        fig_cent.add_trace(
            go.Scatter(
                x=[pos[n][0] for n in node_order],
                y=[pos[n][1] for n in node_order],
                mode="markers",
                marker=dict(
                    size=size_vals,
                    color=node_vals.tolist(),
                    colorscale="Viridis",
                    showscale=True,
                    cmin=cmin,
                    cmax=cmax if cmax > cmin else cmin + 1e-6,
                    colorbar=dict(title=centrality_name),
                    opacity=0.84,
                    line=dict(color=NETWORK_NODE_LINE, width=0.5),
                ),
                text=[safe_hover_text(lookup.loc[n], publish_mode=publish_mode) for n in node_order],
                hoverinfo="text",
                showlegend=False,
            )
        )
    top5_nodes = df_view.head(5)["family_id"].tolist()
    top5_nodes = [n for n in top5_nodes if n in pos]
    if top5_nodes:
        size_lookup = {n: size_vals[idx] for idx, n in enumerate(node_order)}
        fig_cent.add_trace(
            go.Scatter(
                x=[pos[n][0] for n in top5_nodes],
                y=[pos[n][1] for n in top5_nodes],
                mode="markers",
                marker=dict(size=[size_lookup.get(n, 12.0) + 5.0 for n in top5_nodes], color="rgba(255,255,255,0)", line=dict(color="#111827", width=1.8)),
                hoverinfo="skip",
                name="Top 5 centrality",
                showlegend=True,
            )
        )
    style_network_figure(fig_cent, title=f"Jaringan Louvain Menurut {centrality_name}", height=690, showlegend=highlight_roles)
    st.plotly_chart(fig_cent, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    st.markdown("#### Hubungan Centrality dengan Kesejahteraan Rumah Tangga")
    scatter_cols = unique_existing_columns(
        df_view,
        [
            "Kode Node",
            "IKR Agregat",
            "Status Bansos",
            "Peran Struktural",
            "Hover Aman",
            centrality_name,
            "Degree Centrality",
            "Betweenness Centrality",
        ],
    )
    scatter_df = df_view[scatter_cols].copy().dropna(subset=[centrality_name, "IKR Agregat"])
    if not scatter_df.empty:
        size_source = "Betweenness Centrality" if "Betweenness Centrality" in scatter_df.columns else "Degree Centrality"
        if float(pd.to_numeric(scatter_df[size_source], errors="coerce").fillna(0.0).max()) <= 0 and "Degree Centrality" in scatter_df.columns:
            size_source = "Degree Centrality"
        scatter_df["Ukuran Visual"] = pd.to_numeric(scatter_df[size_source], errors="coerce").fillna(0.0)
        scatter_df["Ukuran Visual"] = scatter_df["Ukuran Visual"] + max(float(scatter_df["Ukuran Visual"].max()) * 0.05, 1e-6)
        fig_scatter = px.scatter(
            scatter_df,
            x=centrality_name,
            y="IKR Agregat",
            color="Status Bansos",
            size="Ukuran Visual",
            symbol="Peran Struktural",
            hover_name="Kode Node",
            custom_data=["Hover Aman"],
            title="Hubungan Centrality dengan Kesejahteraan Rumah Tangga",
            color_discrete_map={"Penerima": "#2563EB", "Tidak Menerima": "#B91C1C"},
            category_orders={"Peran Struktural": CENTRALITY_ROLE_ORDER},
        )
        fig_scatter.update_traces(hovertemplate="%{customdata[0]}<extra></extra>", marker=dict(line=dict(color="#111827", width=0.45)))
        median_centrality = float(pd.to_numeric(scatter_df[centrality_name], errors="coerce").median())
        fig_scatter.add_vline(x=median_centrality, line_dash="dash", line_color="#111827", annotation_text="Median centrality")
        fig_scatter.add_hline(y=60.0, line_dash="dash", line_color="#B91C1C", annotation_text="Ambang IKR 60")
        fig_scatter.add_annotation(x=0.77, y=0.20, xref="paper", yref="paper", text="Prioritas verifikasi", showarrow=False, font=dict(size=12, color="#92400E"))
        fig_scatter.add_annotation(x=0.77, y=0.83, xref="paper", yref="paper", text="Penghubung informasi", showarrow=False, font=dict(size=12, color="#1D4ED8"))
        fig_scatter.add_annotation(x=0.20, y=0.20, xref="paper", yref="paper", text="Rentan terisolasi", showarrow=False, font=dict(size=12, color="#B91C1C"))
        fig_scatter.add_annotation(x=0.24, y=0.83, xref="paper", yref="paper", text="Relatif stabil, tidak dominan jaringan", showarrow=False, font=dict(size=12, color="#475569"))
        style_publication_figure(fig_scatter, title="Hubungan Centrality dengan Kesejahteraan Rumah Tangga", height=560, xaxis_title=centrality_name, yaxis_title="IKR Agregat")
        st.plotly_chart(fig_scatter, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    st.markdown("#### Matriks Posisi Struktural dan Kondisi Kesejahteraan")
    matrix_df = df_view.dropna(subset=[centrality_name, "IKR Agregat"]).copy()
    if not matrix_df.empty:
        matrix_df["Kelompok IKR"] = np.where(matrix_df["Level IKR"].isin(["Rendah", "Sedang"]), "IKR rendah/sedang", "IKR tinggi/sangat tinggi")
        matrix_df["Kelompok Centrality"] = np.where(matrix_df["Level Centrality"].eq("Tinggi"), "Centrality tinggi", "Centrality rendah/sedang")
        y_order = ["IKR rendah/sedang", "IKR tinggi/sangat tinggi"]
        x_order = ["Centrality rendah/sedang", "Centrality tinggi"]
        meanings = {
            ("IKR rendah/sedang", "Centrality tinggi"): "Strategis untuk verifikasi/pendampingan",
            ("IKR rendah/sedang", "Centrality rendah/sedang"): "Rentan dan perlu pendekatan langsung",
            ("IKR tinggi/sangat tinggi", "Centrality tinggi"): "Penghubung informasi potensial",
            ("IKR tinggi/sangat tinggi", "Centrality rendah/sedang"): "Relatif stabil, bukan prioritas jaringan",
        }
        total_n = max(len(matrix_df), 1)
        z_vals, text_vals = [], []
        for y_label in y_order:
            z_row, text_row = [], []
            for x_label in x_order:
                count_val = int(((matrix_df["Kelompok IKR"] == y_label) & (matrix_df["Kelompok Centrality"] == x_label)).sum())
                pct_val = (count_val / total_n) * 100.0
                z_row.append(count_val)
                text_row.append(f"{count_val} node<br>{pct_val:.1f}%<br>{meanings[(y_label, x_label)]}")
            z_vals.append(z_row)
            text_vals.append(text_row)
        fig_matrix = go.Figure(go.Heatmap(z=z_vals, x=x_order, y=y_order, text=text_vals, texttemplate="%{text}", colorscale="YlGnBu", colorbar=dict(title="Jumlah node")))
        style_publication_figure(fig_matrix, title="Matriks Posisi Struktural dan Kondisi Kesejahteraan", height=420, xaxis_title="Centrality", yaxis_title="Kondisi kesejahteraan")
        st.plotly_chart(fig_matrix, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    st.markdown("#### Profil Node Strategis dan Implikasi Program")
    top_n = st.slider("Jumlah node pada tabel profil", min_value=5, max_value=30, value=15, step=5, key=f"centrality_page_profile_topn_{selected_centrality_key}")
    role_rank = {role: idx for idx, role in enumerate(CENTRALITY_ROLE_ORDER)}
    df_profile = df_view.copy()
    df_profile["_role_rank"] = df_profile["Peran Struktural"].map(role_rank).fillna(len(role_rank))
    df_profile = df_profile.sort_values(["_role_rank", centrality_name], ascending=[True, False]).head(top_n).copy()
    profile_cols = unique_existing_columns(
        df_profile,
        [
            "Kode Node",
            "Klaster Louvain",
            "Dusun/Kode Dusun",
            "Centrality terpilih",
            "Degree Centrality",
            "Betweenness Centrality",
            "Closeness Centrality",
            "Eigenvector Centrality",
            "IKR Agregat",
            "Status BPS",
            "Status Bansos",
            "Akses Informasi",
            "Peran Struktural",
            "Implikasi Program",
            "Catatan Etika",
        ],
    )
    st.dataframe(
        df_profile[profile_cols].style.format(
            {
                "Centrality terpilih": "{:.6f}",
                "Degree Centrality": "{:.6f}",
                "Betweenness Centrality": "{:.6f}",
                "Closeness Centrality": "{:.6f}",
                "Eigenvector Centrality": "{:.6f}",
                "IKR Agregat": "{:.3f}",
            }
        ),
        use_container_width=True,
    )
    st.download_button(
        "Unduh Tabel Anonim",
        data=df_profile[profile_cols].to_csv(index=False).encode("utf-8"),
        file_name=f"profil_node_strategis_anonim_{selected_centrality_key}.csv",
        mime="text/csv",
        key=f"centrality_page_download_{selected_centrality_key}",
    )

    st.markdown("#### Komposisi Peran Struktural per Klaster dan Dusun")
    role_cluster_df = df_view.groupby(["Klaster Louvain", "Peran Struktural"], as_index=False).size().rename(columns={"size": "Jumlah Node"})
    role_dusun_col = "Dusun/Kode Dusun" if publish_mode else "Dusun"
    role_dusun_df = df_view.groupby([role_dusun_col, "Peran Struktural"], as_index=False).size().rename(columns={"size": "Jumlah Node"})
    rc1, rc2 = st.columns(2)
    with rc1:
        fig_role_cluster = px.bar(role_cluster_df, x="Klaster Louvain", y="Jumlah Node", color="Peran Struktural", barmode="stack", color_discrete_map=CENTRALITY_ROLE_COLORS, category_orders={"Peran Struktural": CENTRALITY_ROLE_ORDER}, title="Komposisi Peran Struktural per Klaster")
        style_publication_figure(fig_role_cluster, title="Komposisi Peran Struktural per Klaster", height=430, xaxis_title="Klaster Louvain", yaxis_title="Jumlah node", legend_title="Peran Struktural")
        st.plotly_chart(fig_role_cluster, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
    with rc2:
        fig_role_dusun = px.bar(role_dusun_df, x=role_dusun_col, y="Jumlah Node", color="Peran Struktural", barmode="stack", color_discrete_map=CENTRALITY_ROLE_COLORS, category_orders={"Peran Struktural": CENTRALITY_ROLE_ORDER}, title="Komposisi Peran Struktural per Dusun")
        style_publication_figure(fig_role_dusun, title="Komposisi Peran Struktural per Dusun", height=430, xaxis_title="Dusun/Kode Dusun", yaxis_title="Jumlah node", legend_title="Peran Struktural")
        st.plotly_chart(fig_role_dusun, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    st.markdown("#### Top 5 Centrality per Pilar")
    tabs = st.tabs([label.replace(" Centrality", "") for label, _ in all_centrality_specs])
    for tab, (metric_label, _) in zip(tabs, all_centrality_specs):
        with tab:
            top_cols = unique_existing_columns(df_view, ["Kode Node", role_dusun_col, "Profesi/Pekerjaan", "IKR Agregat", metric_label, "Peran Struktural"])
            st.dataframe(
                df_view[top_cols].sort_values(metric_label, ascending=False).head(5).style.format({metric_label: "{:.6f}", "IKR Agregat": "{:.3f}"}),
                use_container_width=True,
            )

    st.info(build_centrality_policy_narrative(df_view, centrality_name))
    with subbab_dropdown("Catatan Etika dan Batasan Interpretasi", expanded=publish_mode):
        st.markdown(
            """
            - Data mikro rumah tangga adalah data sensitif.
            - Identitas individu/KK perlu disamarkan dalam visualisasi publik.
            - Centrality tidak boleh dimaknai sebagai status sosial seseorang.
            - Hasil centrality tidak membuktikan inclusion error atau exclusion error.
            - Hasil hanya mendukung proses verifikasi, evaluasi, dan diskusi kebijakan.
            - Verifikasi lapangan dan persetujuan penggunaan data tetap diperlukan.
            - Hindari penyebutan nama orang, alamat spesifik, atau koordinat presisi pada materi presentasi/publikasi.
            """
        )


def build_centrality_top_table_figure(df_table, title, score_col):
    if df_table is None or df_table.empty:
        return None
    table_df = df_table.copy()
    zebra_fill = ["#F8FAFC" if idx % 2 == 0 else "#EEF2F7" for idx in range(len(table_df))]
    for col in ["Nama", "Dusun", "Pekerjaan"]:
        table_df[col] = table_df[col].fillna("-").astype(str)
    table_df["IKR Agregat"] = pd.to_numeric(table_df["IKR Agregat"], errors="coerce").map(
        lambda x: f"{x:.3f}" if pd.notnull(x) else "-"
    )
    table_df[score_col] = pd.to_numeric(table_df[score_col], errors="coerce").map(
        lambda x: f"{x:.6f}" if pd.notnull(x) else "-"
    )

    fig = go.Figure(
        data=[
            go.Table(
                columnwidth=[1.9, 1.3, 1.8, 1.0, 1.2],
                header=dict(
                    values=["Node", "Dusun/Kode", "Pekerjaan", "IKR Agregat", "Skor Centrality"],
                    fill_color="#0F172A",
                    font=dict(color="#F8FAFC", size=13),
                    align="left",
                    line_color="rgba(255,255,255,0.08)",
                    height=34,
                ),
                cells=dict(
                    values=[
                        table_df["Nama"],
                        table_df["Dusun"],
                        table_df["Pekerjaan"],
                        table_df["IKR Agregat"],
                        table_df[score_col],
                    ],
                    fill_color=[
                        zebra_fill,
                        zebra_fill,
                        zebra_fill,
                        zebra_fill,
                        zebra_fill,
                    ],
                    font=dict(color="#0F172A", size=12),
                    align="left",
                    line_color="#E2E8F0",
                    height=32,
                ),
            )
        ]
    )
    fig.update_layout(
        title=title,
        height=280,
        margin=dict(l=12, r=12, t=52, b=12),
        paper_bgcolor="rgba(255,255,255,0.0)",
    )
    return fig


def detect_lat_lon_columns(columns):
    lower_map = {str(c).lower().strip(): c for c in columns}
    lat_candidates = ["lat", "latitude", "lintang", "y", "coord_lat"]
    lon_candidates = ["lon", "lng", "long", "longitude", "bujur", "x", "coord_lon"]
    lat_col = next((lower_map[k] for k in lat_candidates if k in lower_map), None)
    lon_col = next((lower_map[k] for k in lon_candidates if k in lower_map), None)
    return lat_col, lon_col


def build_spatial_node_figure(
    graph_obj,
    node_ids,
    node_color_vals,
    node_hover_text,
    title,
    spatial_mode="Spasial OSM",
    marker_size=11,
    colorscale="Viridis",
    colorbar=None,
    cmin=None,
    cmax=None,
    line_color=NETWORK_NODE_LINE,
    line_width=0.6,
):
    if graph_obj is None or graph_obj.number_of_nodes() == 0:
        return None
    sample_cols = set()
    for n in graph_obj.nodes():
        sample_cols.update(graph_obj.nodes[n].keys())
        break
    lat_col, lon_col = detect_lat_lon_columns(sample_cols)
    if not lat_col or not lon_col:
        return None

    lats, lons, colors, hovers = [], [], [], []
    for idx, n in enumerate(node_ids):
        if n not in graph_obj.nodes():
            continue
        lat_v = _safe_float_metric(graph_obj.nodes[n].get(lat_col), default=np.nan)
        lon_v = _safe_float_metric(graph_obj.nodes[n].get(lon_col), default=np.nan)
        if not (np.isfinite(lat_v) and np.isfinite(lon_v)):
            continue
        lats.append(float(lat_v))
        lons.append(float(lon_v))
        colors.append(node_color_vals[idx] if idx < len(node_color_vals) else 0.0)
        hovers.append(node_hover_text[idx] if idx < len(node_hover_text) else f"Node: {n}")
    if len(lats) == 0:
        return None

    marker_cfg = dict(
        size=marker_size,
        color=colors,
        colorscale=colorscale,
        showscale=True,
    )
    if colorbar is not None:
        marker_cfg["colorbar"] = colorbar
    if cmin is not None:
        marker_cfg["cmin"] = cmin
    if cmax is not None:
        marker_cfg["cmax"] = cmax

    fig = go.Figure(
        go.Scattermapbox(
            lat=lats,
            lon=lons,
            mode="markers",
            marker=marker_cfg,
            text=hovers,
            hoverinfo="text",
            showlegend=False,
        )
    )
    center_lat = float(np.mean(lats))
    center_lon = float(np.mean(lons))
    mapbox_cfg = dict(
        zoom=12,
        center=dict(lat=center_lat, lon=center_lon),
    )
    if spatial_mode == "Spasial ArcGIS":
        mapbox_cfg["style"] = "white-bg"
        mapbox_cfg["layers"] = [
            {
                "sourcetype": "raster",
                "source": ["https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
                "below": "traces",
            }
        ]
    else:
        mapbox_cfg["style"] = "open-street-map"
    fig.update_layout(
        title=dict(text=title, x=0.02, xanchor="left"),
        height=560,
        template=PUBLICATION_TEMPLATE,
        margin=dict(l=20, r=20, t=60, b=20),
        mapbox=mapbox_cfg,
    )
    return fig


def categorize_ikr_bps(score):
    s = _safe_float_metric(score, default=np.nan)
    if not np.isfinite(s):
        return ("Tidak Valid", 0)
    # Referensi BPS (2014): kategori capaian skor (skala 0-100).
    if s >= 80.0:
        return ("Sangat Tinggi", 4)
    if s >= 70.0:
        return ("Tinggi", 3)
    if s >= 60.0:
        return ("Sedang", 2)
    return ("Rendah", 1)


def add_bps_ikr_category(df_in, ikr_col="f_ikr_dari_rekap_kk"):
    if ikr_col not in df_in.columns:
        return df_in.copy()
    df_out = df_in.copy()
    mapped = df_out[ikr_col].apply(categorize_ikr_bps)
    df_out["kategori_ikr"] = mapped.apply(lambda x: x[0])
    df_out["kategori_ikr_code"] = mapped.apply(lambda x: int(x[1]))
    return df_out


def compute_montes_within_between_assortativity(
    graph_obj,
    category_attr="kategori_ikr_code",
    group_attr="cluster",
    invalid_category_values=None,
):
    # Referensi Montes et al. (2018): dekomposisi within-between berbasis delta kategorikal.
    if graph_obj is None or graph_obj.number_of_nodes() < 2 or graph_obj.number_of_edges() == 0:
        return {
            "q_w_star": 0.0,
            "q_b_star": 0.0,
            "m_w": 0.0,
            "m_b": 0.0,
            "n_nodes": int(graph_obj.number_of_nodes() if graph_obj is not None else 0),
            "n_edges": int(graph_obj.number_of_edges() if graph_obj is not None else 0),
        }

    nodes = list(graph_obj.nodes())
    k = {n: _safe_float_metric(graph_obj.degree(n, weight="weight"), default=0.0) for n in nodes}
    two_m = float(sum(k.values()))
    if two_m <= 1e-12:
        return {
            "q_w_star": 0.0,
            "q_b_star": 0.0,
            "m_w": 0.0,
            "m_b": 0.0,
            "n_nodes": int(graph_obj.number_of_nodes()),
            "n_edges": int(graph_obj.number_of_edges()),
        }

    m_w = 0.0
    m_b = 0.0
    k_w = {n: 0.0 for n in nodes}
    k_b = {n: 0.0 for n in nodes}
    for u, v, d in graph_obj.edges(data=True):
        w = _safe_float_metric(d.get("weight"), default=0.0)
        if graph_obj.nodes[u].get(group_attr) == graph_obj.nodes[v].get(group_attr):
            m_w += w
            k_w[u] += w
            k_w[v] += w
        else:
            m_b += w
            k_b[u] += w
            k_b[v] += w

    two_m_w = 2.0 * m_w
    two_m_b = 2.0 * m_b
    sum_w = 0.0
    sum_b = 0.0
    exp_w_masked = 0.0
    exp_b_masked = 0.0
    invalid_set = set() if invalid_category_values is None else set(invalid_category_values)
    for i in nodes:
        xi = graph_obj.nodes[i].get(category_attr, 0)
        hi = graph_obj.nodes[i].get(group_attr, None)
        for j in nodes:
            xj = graph_obj.nodes[j].get(category_attr, 0)
            hj = graph_obj.nodes[j].get(group_attr, None)
            xi_valid = pd.notnull(xi) and xi not in invalid_set
            xj_valid = pd.notnull(xj) and xj not in invalid_set
            delta_x = 1.0 if xi_valid and xj_valid and xi == xj else 0.0
            delta_h = 1.0 if hi == hj else 0.0
            a_ij = _safe_float_metric(graph_obj[i][j].get("weight"), default=0.0) if graph_obj.has_edge(i, j) else 0.0
            expected_w = (k_w[i] * k_w[j] / two_m_w) if two_m_w > 1e-12 else 0.0
            expected_b = (k_b[i] * k_b[j] / two_m_b) if two_m_b > 1e-12 else 0.0
            mask_w = delta_x * delta_h
            mask_b = delta_x * (1.0 - delta_h)
            sum_w += (a_ij - expected_w) * mask_w
            sum_b += (a_ij - expected_b) * mask_b
            exp_w_masked += expected_w * mask_w
            exp_b_masked += expected_b * mask_b

    # Qw dan Qb raw sesuai pembagi 2m_w / 2m_b.
    q_w_raw = (sum_w / two_m_w) if two_m_w > 1e-12 else 0.0
    q_b_raw = (sum_b / two_m_b) if two_m_b > 1e-12 else 0.0

    # Normalisasi Montes (Pers. 6-7): Q* = Q / Qmax.
    # Qmax numerik dihitung dari: (2m - sum(expected_term_masked)).
    q_w_max_num = (two_m_w - exp_w_masked) if two_m_w > 1e-12 else 0.0
    q_b_max_num = (two_m_b - exp_b_masked) if two_m_b > 1e-12 else 0.0
    q_w_max = (q_w_max_num / two_m_w) if two_m_w > 1e-12 else 0.0
    q_b_max = (q_b_max_num / two_m_b) if two_m_b > 1e-12 else 0.0
    q_w_star = (q_w_raw / q_w_max) if abs(q_w_max) > 1e-12 else 0.0
    q_b_star = (q_b_raw / q_b_max) if abs(q_b_max) > 1e-12 else 0.0
    q_w_star = float(np.clip(q_w_star, -1.0, 1.0))
    q_b_star = float(np.clip(q_b_star, -1.0, 1.0))
    return {
        "q_w_star": float(q_w_star),
        "q_b_star": float(q_b_star),
        "q_w_raw": float(q_w_raw),
        "q_b_raw": float(q_b_raw),
        "q_w_max": float(q_w_max),
        "q_b_max": float(q_b_max),
        "m_w": float(m_w),
        "m_b": float(m_b),
        "n_nodes": int(graph_obj.number_of_nodes()),
        "n_edges": int(graph_obj.number_of_edges()),
    }


def build_category_connection_breakdown(
    graph_obj,
    category_attr="kategori_ikr",
    group_attr="cluster",
    category_order=None,
    invalid_label="Tidak Valid",
):
    if graph_obj is None or graph_obj.number_of_edges() == 0:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    category_order = list(category_order or [])
    order_map = {str(cat): idx for idx, cat in enumerate(category_order)}

    def normalize_cat(val):
        if pd.isna(val):
            return invalid_label
        txt = str(val).strip()
        return txt if txt else invalid_label

    def sort_pair(cat_a, cat_b):
        idx_a = order_map.get(cat_a, len(order_map))
        idx_b = order_map.get(cat_b, len(order_map))
        return (cat_a, cat_b) if (idx_a, cat_a) <= (idx_b, cat_b) else (cat_b, cat_a)

    rows = []
    for u, v, d in graph_obj.edges(data=True):
        cat_u = normalize_cat(graph_obj.nodes[u].get(category_attr, invalid_label))
        cat_v = normalize_cat(graph_obj.nodes[v].get(category_attr, invalid_label))
        cat_a, cat_b = sort_pair(cat_u, cat_v)
        scope = "Within" if graph_obj.nodes[u].get(group_attr) == graph_obj.nodes[v].get(group_attr) else "Between"
        weight = _safe_float_metric(d.get("weight"), default=0.0)
        rows.append(
            {
                "Ruang": scope,
                "Kategori 1": cat_a,
                "Kategori 2": cat_b,
                "Pasangan": f"{cat_a} - {cat_b}",
                "Jenis Pasangan": "Sama" if cat_a == cat_b else "Beda",
                "Bobot Edge": weight,
                "Jumlah Edge": 1,
            }
        )

    if not rows:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_pairs = pd.DataFrame(rows)
    df_summary = (
        df_pairs.groupby(["Ruang", "Kategori 1", "Kategori 2", "Pasangan", "Jenis Pasangan"], as_index=False)
        .agg({"Bobot Edge": "sum", "Jumlah Edge": "sum"})
    )
    scope_weight = df_summary.groupby("Ruang")["Bobot Edge"].transform("sum")
    scope_edge = df_summary.groupby("Ruang")["Jumlah Edge"].transform("sum")
    df_summary["Persentase Bobot (%)"] = np.where(scope_weight > 0, (df_summary["Bobot Edge"] / scope_weight) * 100.0, 0.0)
    df_summary["Persentase Edge (%)"] = np.where(scope_edge > 0, (df_summary["Jumlah Edge"] / scope_edge) * 100.0, 0.0)
    df_summary["Urut 1"] = df_summary["Kategori 1"].map(lambda x: order_map.get(str(x), len(order_map)))
    df_summary["Urut 2"] = df_summary["Kategori 2"].map(lambda x: order_map.get(str(x), len(order_map)))
    df_summary = df_summary.sort_values(["Ruang", "Urut 1", "Urut 2", "Pasangan"]).reset_index(drop=True)

    matrix_rows = []
    for _, row in df_summary.iterrows():
        matrix_rows.append(
            {
                "Ruang": row["Ruang"],
                "Kategori Baris": row["Kategori 1"],
                "Kategori Kolom": row["Kategori 2"],
                "Persentase Bobot (%)": float(row["Persentase Bobot (%)"]),
            }
        )
        if row["Kategori 1"] != row["Kategori 2"]:
            matrix_rows.append(
                {
                    "Ruang": row["Ruang"],
                    "Kategori Baris": row["Kategori 2"],
                    "Kategori Kolom": row["Kategori 1"],
                    "Persentase Bobot (%)": float(row["Persentase Bobot (%)"]),
                }
            )
    df_matrix_long = pd.DataFrame(matrix_rows)
    return df_pairs, df_summary, df_matrix_long


def build_spatial_category_figure(
    df_map,
    lat_col,
    lon_col,
    category_col,
    hover_col,
    title,
    spatial_mode="Spasial ArcGIS",
    category_order=None,
    category_colors=None,
    marker_size=12,
):
    if df_map is None or df_map.empty or lat_col not in df_map.columns or lon_col not in df_map.columns:
        return None

    plot_df = df_map.copy()
    plot_df[category_col] = plot_df[category_col].fillna("Tidak Valid").astype(str)
    plot_df[lat_col] = pd.to_numeric(plot_df[lat_col], errors="coerce")
    plot_df[lon_col] = pd.to_numeric(plot_df[lon_col], errors="coerce")
    plot_df = plot_df[plot_df[lat_col].notna() & plot_df[lon_col].notna()].copy()
    if plot_df.empty:
        return None

    uniq = plot_df[category_col].unique().tolist()
    category_order = list(category_order or [])
    ordered_categories = [cat for cat in category_order if cat in uniq]
    ordered_categories += sorted([cat for cat in uniq if cat not in ordered_categories])
    category_colors = category_colors or {}

    fig = go.Figure()
    for idx, category in enumerate(ordered_categories):
        sub = plot_df[plot_df[category_col] == category]
        if sub.empty:
            continue
        fig.add_trace(
            go.Scattermapbox(
                lat=sub[lat_col],
                lon=sub[lon_col],
                mode="markers",
                marker=dict(
                    size=marker_size,
                    color=category_colors.get(category, CONTRAST_COLORS[idx % len(CONTRAST_COLORS)]),
                    opacity=0.90,
                ),
                name=category,
                text=sub[hover_col],
                hoverinfo="text",
            )
        )

    mapbox_cfg = dict(
        zoom=12,
        center=dict(
            lat=float(plot_df[lat_col].mean()),
            lon=float(plot_df[lon_col].mean()),
        ),
    )
    if spatial_mode == "Spasial ArcGIS":
        mapbox_cfg["style"] = "white-bg"
        mapbox_cfg["layers"] = [
            {
                "sourcetype": "raster",
                "source": ["https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
                "below": "traces",
            }
        ]
    else:
        mapbox_cfg["style"] = "open-street-map"

    fig.update_layout(
        title=dict(text=title, x=0.02, xanchor="left"),
        height=620,
        template=PUBLICATION_TEMPLATE,
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(
            title=dict(text=category_col),
            orientation="h",
            yanchor="bottom",
            y=0.01,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255,255,255,0.85)",
        ),
        mapbox=mapbox_cfg,
    )
    return fig


def build_spatial_numeric_figure(
    df_map,
    lat_col,
    lon_col,
    value_col,
    hover_col,
    title,
    spatial_mode="Spasial ArcGIS",
    marker_size_col=None,
    colorscale="RdYlGn",
    colorbar_title="Nilai",
):
    if df_map is None or df_map.empty or lat_col not in df_map.columns or lon_col not in df_map.columns:
        return None

    plot_df = df_map.copy()
    plot_df[lat_col] = pd.to_numeric(plot_df[lat_col], errors="coerce")
    plot_df[lon_col] = pd.to_numeric(plot_df[lon_col], errors="coerce")
    plot_df[value_col] = pd.to_numeric(plot_df[value_col], errors="coerce")
    plot_df = plot_df[plot_df[lat_col].notna() & plot_df[lon_col].notna()].copy()
    if plot_df.empty:
        return None

    marker_size = 12
    if marker_size_col and marker_size_col in plot_df.columns:
        size_vals = pd.to_numeric(plot_df[marker_size_col], errors="coerce").fillna(0)
        marker_size = (size_vals.clip(lower=0) * 3) + 10

    fig = go.Figure(
        go.Scattermapbox(
            lat=plot_df[lat_col],
            lon=plot_df[lon_col],
            mode="markers",
            marker=dict(
                size=marker_size,
                color=plot_df[value_col],
                colorscale=colorscale,
                showscale=True,
                colorbar=dict(title=colorbar_title),
                cmin=float(plot_df[value_col].min()) if plot_df[value_col].notna().any() else 0.0,
                cmax=float(plot_df[value_col].max()) if plot_df[value_col].notna().any() else 100.0,
                opacity=0.92,
            ),
            text=plot_df[hover_col],
            hoverinfo="text",
            showlegend=False,
        )
    )

    mapbox_cfg = dict(
        zoom=12,
        center=dict(
            lat=float(plot_df[lat_col].mean()),
            lon=float(plot_df[lon_col].mean()),
        ),
    )
    if spatial_mode == "Spasial ArcGIS":
        mapbox_cfg["style"] = "white-bg"
        mapbox_cfg["layers"] = [
            {
                "sourcetype": "raster",
                "source": ["https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
                "below": "traces",
            }
        ]
    else:
        mapbox_cfg["style"] = "open-street-map"

    fig.update_layout(
        title=dict(text=title, x=0.02, xanchor="left"),
        height=620,
        template=PUBLICATION_TEMPLATE,
        margin=dict(l=20, r=20, t=60, b=20),
        mapbox=mapbox_cfg,
    )
    return fig


def render_bansos_spatial_analysis_page(
    df_v,
    graph_obj,
    partition,
    spatial_mode="Spasial ArcGIS",
    selected_dimension_col="f_a_dari_rekap_kk",
    map_color_mode="IKR Agregat",
    filter_mode="Semua KK",
    dim_thresholds=None,
):
    st.markdown("<h1 class='main-header'>Analisis Bansos Spasial</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div class='premium-hero'><b>Fokus Halaman:</b> memetakan penerima bansos berdasarkan "
        "dimensi IKR yang dipilih. Warna node selalu mengikuti <b>IKR agregat</b> sebagai tingkat "
        "kesejahteraan ekonomi, sedangkan detail hover menampilkan skor dimensi dan jenis bansos yang diterima.</div>",
        unsafe_allow_html=True,
    )

    if graph_obj is None or graph_obj.number_of_nodes() == 0:
        st.warning("Graf analisis belum tersedia.")
        return

    dim_thresholds = dim_thresholds or {}
    node_ids = list(graph_obj.nodes())
    df_nodes = (
        df_v[df_v["family_id"].isin(node_ids)]
        .drop_duplicates("family_id")
        .copy()
    )
    if df_nodes.empty:
        st.warning("Tidak ada node graf yang cocok dengan data desa terpilih.")
        return

    lat_col, lon_col = detect_lat_lon_columns(df_nodes.columns)
    raw_bansos_col = resolve_first_existing_column(df_nodes.columns, ["bansos", "keikutsertaan program bantuan", "program bantuan", "bantuan sosial", "bansos bantuan"])
    df_nodes["cluster"] = df_nodes["family_id"].map(lambda fid: int(partition.get(fid, -1)))
    df_nodes["Cluster Louvain"] = df_nodes["cluster"].map(lambda x: f"Klaster {x}" if x >= 0 else "Tidak Terklaster")
    df_nodes["Status Bansos"] = df_nodes["bansos_num"].apply(lambda x: "Penerima" if int(_safe_float_metric(x, default=0.0) > 0) == 1 else "Non-Penerima")
    df_nodes["F_IKR"] = pd.to_numeric(df_nodes.get("f_ikr_dari_rekap_kk"), errors="coerce")
    df_nodes["Jenis Bansos"] = (
        df_nodes[raw_bansos_col].astype(str).replace({"nan": "-", "None": "-", "none": "-", "": "-"}).fillna("-")
        if raw_bansos_col
        else "-"
    )

    dim_lookup = {col: label for label, col in IKR_DIMENSION_MAP}
    selected_dimension_label = dim_lookup.get(selected_dimension_col, selected_dimension_col)
    df_nodes["Skor Dimensi Pilihan"] = pd.to_numeric(df_nodes.get(selected_dimension_col), errors="coerce")
    selected_threshold = float(dim_thresholds.get(selected_dimension_col, 60.0))
    df_nodes["Status Dimensi Pilihan"] = np.where(
        df_nodes["Skor Dimensi Pilihan"].le(selected_threshold).fillna(False),
        f"Rentan {selected_dimension_label}",
        f"Tidak Rentan {selected_dimension_label}",
    )

    weak_dim_labels = []
    weak_dim_count = np.zeros(len(df_nodes), dtype=int)
    for dim_label, col_name in IKR_DIMENSION_MAP:
        thr = float(dim_thresholds.get(col_name, 60.0))
        dim_vals = pd.to_numeric(df_nodes.get(col_name), errors="coerce")
        weak_flag = (dim_vals <= thr).fillna(False)
        weak_dim_count += weak_flag.astype(int).to_numpy()
        weak_dim_labels.append(
            weak_flag.map(lambda x, lbl=dim_label: lbl if bool(x) else None)
        )
    weak_dim_df = pd.concat(weak_dim_labels, axis=1) if weak_dim_labels else pd.DataFrame(index=df_nodes.index)
    df_nodes["Jumlah Dimensi Rentan"] = weak_dim_count.astype(int)
    df_nodes["Dimensi Rentan"] = weak_dim_df.apply(
        lambda row: ", ".join([str(v) for v in row.tolist() if pd.notnull(v)]) if len(row) else "-",
        axis=1,
    )
    def classify_bps_bansos(row):
        kategori = str(row.get("kategori_ikr", "Tidak Valid"))
        status = str(row.get("Status Bansos", "Non-Penerima"))
        if kategori not in {"Rendah", "Sedang", "Tinggi", "Sangat Tinggi"}:
            return "Tidak Valid"
        return f"{kategori} - {'Penerima' if status == 'Penerima' else 'Belum Menerima'}"

    df_nodes["Status BPS-Bansos"] = df_nodes.apply(classify_bps_bansos, axis=1)
    df_nodes["Label Dimensi Rentan"] = df_nodes["Jumlah Dimensi Rentan"].map(lambda x: f"{int(x)} dimensi rentan")

    nx.set_node_attributes(
        graph_obj,
        df_nodes.set_index("family_id")[["kategori_ikr", "kategori_ikr_code"]].to_dict("index"),
    )
    montes_res = compute_montes_within_between_assortativity(
        graph_obj,
        category_attr="kategori_ikr_code",
        group_attr="cluster",
        invalid_category_values={0},
    )
    q_w_star = float(montes_res["q_w_star"])
    q_b_star = float(montes_res["q_b_star"])

    total_nodes = int(len(df_nodes))
    total_receiver = int((df_nodes["Status Bansos"] == "Penerima").sum())
    total_selected_dim_vulnerable = int(df_nodes["Status Dimensi Pilihan"].eq(f"Rentan {selected_dimension_label}").sum())
    total_rendah_terima = int((df_nodes["Status BPS-Bansos"] == "Rendah - Penerima").sum())
    total_rendah_belum = int((df_nodes["Status BPS-Bansos"] == "Rendah - Belum Menerima").sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Node Tersedia", total_nodes)
    k2.metric("Penerima Bansos", total_receiver)
    k3.metric(f"Rentan {selected_dimension_label}", total_selected_dim_vulnerable)
    k4.metric("Rendah - Penerima", total_rendah_terima)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Qw*", f"{q_w_star:.4f}")
    m2.metric("Qb*", f"{q_b_star:.4f}")
    m3.metric("Rendah - Belum Menerima", total_rendah_belum)
    m4.metric("Rata-rata Dimensi Rentan", f"{float(df_nodes['Jumlah Dimensi Rentan'].mean()):.2f}")

    st.markdown(
        f"<div class='soft-card'><b>Interpretasi Cepat:</b><br>"
        f"Qw* = <b>{q_w_star:.3f}</b> menunjukkan kekompakan kategori BPS di dalam klaster, "
        f"sedangkan Qb* = <b>{q_b_star:.3f}</b> membaca kemiripan strata BPS antar-klaster. "
        f"Pada page ini, <b>warna node memakai IKR agregat</b> sebagai base kesejahteraan ekonomi, "
        f"sedangkan analisis bansos dibaca dari dimensi terpilih <b>{selected_dimension_label}</b> "
        f"dengan ambang <b>{selected_threshold:.1f}</b>.</div>",
        unsafe_allow_html=True,
    )

    map_df = df_nodes.copy()
    if filter_mode == "Penerima Bansos":
        map_df = map_df[map_df["Status Bansos"] == "Penerima"].copy()
    elif filter_mode == "Rendah - Penerima":
        map_df = map_df[map_df["Status BPS-Bansos"] == "Rendah - Penerima"].copy()
    elif filter_mode == "Rendah - Belum Menerima":
        map_df = map_df[map_df["Status BPS-Bansos"] == "Rendah - Belum Menerima"].copy()
    elif filter_mode == "Sedang - Penerima":
        map_df = map_df[map_df["Status BPS-Bansos"] == "Sedang - Penerima"].copy()
    elif filter_mode == "Sedang - Belum Menerima":
        map_df = map_df[map_df["Status BPS-Bansos"] == "Sedang - Belum Menerima"].copy()
    elif filter_mode == "Tinggi - Penerima":
        map_df = map_df[map_df["Status BPS-Bansos"] == "Tinggi - Penerima"].copy()
    elif filter_mode == "Tinggi - Belum Menerima":
        map_df = map_df[map_df["Status BPS-Bansos"] == "Tinggi - Belum Menerima"].copy()
    elif filter_mode == "Sangat Tinggi - Penerima":
        map_df = map_df[map_df["Status BPS-Bansos"] == "Sangat Tinggi - Penerima"].copy()
    elif filter_mode == "Sangat Tinggi - Belum Menerima":
        map_df = map_df[map_df["Status BPS-Bansos"] == "Sangat Tinggi - Belum Menerima"].copy()
    elif filter_mode == "Rentan Dimensi Terpilih":
        map_df = map_df[map_df["Status Dimensi Pilihan"] == f"Rentan {selected_dimension_label}"].copy()
    elif filter_mode == "Penerima pada Dimensi Terpilih":
        map_df = map_df[
            (map_df["Status Bansos"] == "Penerima")
            & (map_df["Status Dimensi Pilihan"] == f"Rentan {selected_dimension_label}")
        ].copy()

    df_nodes["nama"] = df_nodes.get("nama", df_nodes["family_id"]).astype(str)
    hover_text = []
    for _, row in map_df.iterrows():
        dim_lines = []
        for dim_label, col_name in IKR_DIMENSION_MAP:
            dim_lines.append(f"{dim_label}: {_safe_float_metric(row.get(col_name), default=np.nan):.2f}")
        hover_text.append(
            f"Nama: {row.get('nama', row.get('family_id', '-'))}"
            f"<br>Family ID: {row.get('family_id', '-')}"
            f"<br>Status Bansos: {row.get('Status Bansos', '-')}"
            f"<br>Jenis Bansos: {row.get('Jenis Bansos', '-')}"
            f"<br>Status BPS-Bansos: {row.get('Status BPS-Bansos', '-')}"
            f"<br>Kategori BPS: {row.get('kategori_ikr', '-')}"
            f"<br>IKR Agregat: {_safe_float_metric(row.get('F_IKR'), default=np.nan):.2f}"
            f"<br>{selected_dimension_label}: {_safe_float_metric(row.get('Skor Dimensi Pilihan'), default=np.nan):.2f}"
            f"<br>Status Dimensi: {row.get('Status Dimensi Pilihan', '-')}"
            f"<br>Cluster: {row.get('Cluster Louvain', '-')}"
            f"<br>Dimensi Rentan: {row.get('Dimensi Rentan', '-')}"
            f"<br>{'<br>'.join(dim_lines)}"
        )
    map_df["__hover_text__"] = hover_text

    c_map, c_table = st.columns([1.6, 1.0], gap="large")
    with c_map:
        st.markdown("### Peta Spasial Bansos")
        if not lat_col or not lon_col:
            st.warning("Kolom lat/lon belum ditemukan sehingga peta spasial belum bisa ditampilkan.")
        elif map_df.empty:
            st.info("Tidak ada node yang cocok dengan filter peta saat ini.")
        else:
            if map_color_mode == "Status Bansos (YA/TIDAK)":
                fig_map = build_spatial_category_figure(
                    map_df,
                    lat_col=lat_col,
                    lon_col=lon_col,
                    category_col="Status Bansos",
                    hover_col="__hover_text__",
                    title=f"Peta Persebaran Bansos berdasarkan {selected_dimension_label} ({filter_mode})",
                    spatial_mode=spatial_mode,
                    category_order=["Penerima", "Non-Penerima"],
                    category_colors={"Penerima": "#0f766e", "Non-Penerima": "#b91c1c"},
                    marker_size=12,
                )
            elif map_color_mode == "Status BPS-Bansos":
                fig_map = build_spatial_category_figure(
                    map_df,
                    lat_col=lat_col,
                    lon_col=lon_col,
                    category_col="Status BPS-Bansos",
                    hover_col="__hover_text__",
                    title=f"Peta Persebaran Bansos berdasarkan {selected_dimension_label} ({filter_mode})",
                    spatial_mode=spatial_mode,
                    category_order=list(BANSOS_TARGETING_COLORS.keys()),
                    category_colors=BANSOS_TARGETING_COLORS,
                    marker_size=12,
                )
            else:
                fig_map = build_spatial_numeric_figure(
                    map_df,
                    lat_col=lat_col,
                    lon_col=lon_col,
                    value_col="F_IKR",
                    hover_col="__hover_text__",
                    title=f"Peta Persebaran Bansos berdasarkan {selected_dimension_label} ({filter_mode})",
                    spatial_mode=spatial_mode,
                    marker_size_col="bansos_num",
                    colorscale="RdYlGn",
                    colorbar_title="IKR Agregat",
                )
            if fig_map is not None:
                st.plotly_chart(fig_map, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
            else:
                st.warning("Koordinat belum valid untuk divisualisasikan di peta.")

    with c_table:
        st.markdown(f"### Ringkasan {selected_dimension_label}")
        summary_cols = [
            "Status Dimensi Pilihan",
            "Status BPS-Bansos",
            "Status Bansos",
            "kategori_ikr",
            "Jenis Bansos",
        ]
        df_summary = (
            df_nodes.groupby(summary_cols, dropna=False)
            .size()
            .reset_index(name="Jumlah KK")
            .sort_values(["Jumlah KK", "Status Dimensi Pilihan"], ascending=[False, True])
        )
        st.dataframe(df_summary, use_container_width=True)

        jenis_summary = (
            df_nodes[df_nodes["Status Bansos"] == "Penerima"]
            .groupby(["Status Dimensi Pilihan", "Jenis Bansos"], dropna=False)
            .size()
            .reset_index(name="Jumlah KK")
            .sort_values(["Status Dimensi Pilihan", "Jumlah KK"], ascending=[True, False])
        )
        st.markdown("#### Jenis Bansos pada Dimensi Terpilih")
        st.dataframe(jenis_summary, use_container_width=True)

    dim_priority_cols = [col for _, col in IKR_DIMENSION_MAP if col in df_nodes.columns]
    if dim_priority_cols:
        dim_long = (
            df_nodes.melt(
                id_vars=["Status Bansos"],
                value_vars=dim_priority_cols,
                var_name="Dimensi",
                value_name="Skor",
            )
        )
        dim_label_map = {col: label for label, col in IKR_DIMENSION_MAP}
        dim_long["Dimensi"] = dim_long["Dimensi"].map(lambda c: dim_label_map.get(c, c))
        dim_long["Skor"] = pd.to_numeric(dim_long["Skor"], errors="coerce")
        dim_long = dim_long.dropna(subset=["Skor"])
        if not dim_long.empty:
            fig_dim = px.box(
                dim_long,
                x="Dimensi",
                y="Skor",
                color="Status Bansos",
                color_discrete_map={"Penerima": "#0f766e", "Non-Penerima": "#b91c1c"},
                title="Sebaran Skor Tiap Dimensi menurut Status Penerimaan Bansos",
            )
            fig_dim.update_layout(template="plotly_white", height=460, xaxis_title="", yaxis_title="Skor")
            st.plotly_chart(fig_dim, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    bansos_dim_focus = df_nodes[["family_id", "nama", "Status Bansos", "Jenis Bansos", "kategori_ikr", "F_IKR", "Skor Dimensi Pilihan", "Status Dimensi Pilihan", "Cluster Louvain"]].copy()
    fig_focus = px.scatter(
        bansos_dim_focus,
        x="Skor Dimensi Pilihan",
        y="F_IKR",
        color="Status Bansos",
        symbol="kategori_ikr",
        hover_data=["nama", "family_id", "Jenis Bansos", "Cluster Louvain"],
        color_discrete_map={"Penerima": "#0f766e", "Non-Penerima": "#b91c1c"},
        labels={"F_IKR": "IKR Agregat", "Skor Dimensi Pilihan": selected_dimension_label},
        title=f"Relasi {selected_dimension_label} terhadap IKR Agregat dan Penerimaan Bansos",
    )
    fig_focus.add_vline(x=selected_threshold, line_dash="dash", line_color="#475569")
    fig_focus.update_layout(template="plotly_white", height=460, xaxis_title=selected_dimension_label, yaxis_title="IKR Agregat")
    st.plotly_chart(fig_focus, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    detail_cols = [
        "family_id",
        "nama",
        "Status Bansos",
        "Jenis Bansos",
        "Status Dimensi Pilihan",
        "Skor Dimensi Pilihan",
        "Status BPS-Bansos",
        "kategori_ikr",
        "F_IKR",
        "Cluster Louvain",
        "Jumlah Dimensi Rentan",
        "Dimensi Rentan",
    ] + [col for _, col in IKR_DIMENSION_MAP if col in df_nodes.columns]
    if lat_col and lon_col:
        detail_cols += [lat_col, lon_col]
    detail_cols = [col for col in detail_cols if col in df_nodes.columns]
    st.markdown("### Detail Rumah Tangga")
    detail_display_df = df_nodes[detail_cols].sort_values(
        ["Status Dimensi Pilihan", "Status Bansos", "F_IKR"],
        ascending=[True, True, True],
    )
    detail_display_df = detail_display_df.rename(
        columns={
            **{col: label for label, col in IKR_DIMENSION_MAP},
            "F_IKR": "IKR Agregat",
            "kategori_ikr": "Kategori IKR",
        }
    )
    st.dataframe(
        detail_display_df,
        use_container_width=True,
    )


def build_labeled_attribute_connection_breakdown(
    graph_obj,
    attr_name,
    value_map=None,
    group_attr="cluster",
    category_order=None,
    invalid_label="Tidak Valid",
):
    if graph_obj is None or graph_obj.number_of_edges() == 0:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    value_map = value_map or {}

    def map_value(val):
        if pd.isna(val):
            return invalid_label
        key = val
        if isinstance(val, str):
            key = val.strip()
        mapped = value_map.get(key, value_map.get(str(key), key))
        mapped_txt = str(mapped).strip()
        return mapped_txt if mapped_txt else invalid_label

    graph_tmp = graph_obj.copy()
    for n in graph_tmp.nodes():
        graph_tmp.nodes[n]["__mapped_attr__"] = map_value(graph_tmp.nodes[n].get(attr_name, invalid_label))
    return build_category_connection_breakdown(
        graph_tmp,
        category_attr="__mapped_attr__",
        group_attr=group_attr,
        category_order=category_order,
        invalid_label=invalid_label,
    )


def build_spatial_indicator_profile(
    graph_obj,
    dusun_attr,
    indicator_specs,
):
    if graph_obj is None or graph_obj.number_of_nodes() == 0 or not dusun_attr:
        return pd.DataFrame()

    rows = []
    dusun_vals = pd.Series([graph_obj.nodes[n].get(dusun_attr) for n in graph_obj.nodes()]).fillna("Tidak Valid").astype(str)
    dusun_order = sorted(dusun_vals.unique().tolist())
    for dusun_name in dusun_order:
        dusun_nodes = [n for n in graph_obj.nodes() if str(graph_obj.nodes[n].get(dusun_attr, "Tidak Valid")) == dusun_name]
        if not dusun_nodes:
            continue
        sub_g = graph_obj.subgraph(dusun_nodes).copy()
        row = {
            "Dusun": dusun_name,
            "Jumlah KK": int(len(dusun_nodes)),
            "Jumlah Edge Internal": int(sub_g.number_of_edges()),
            "Total Bobot Edge Internal": float(sum(_safe_float_metric(d.get("weight"), default=0.0) for _, _, d in sub_g.edges(data=True))),
        }
        for spec in indicator_specs:
            label = spec["label"]
            col = spec["col"]
            bin_vals = [int(_safe_float_metric(graph_obj.nodes[n].get(col), default=0.0) > 0) for n in dusun_nodes]
            yes_count = int(sum(bin_vals))
            row[f"{label} - Jumlah YA"] = yes_count
            row[f"{label} - Persentase YA (%)"] = float((yes_count / len(dusun_nodes)) * 100.0) if dusun_nodes else 0.0

            yy_weight = 0.0
            yn_weight = 0.0
            nn_weight = 0.0
            yy_edges = 0
            yn_edges = 0
            nn_edges = 0
            total_weight = 0.0
            total_edges = 0
            for u, v, d in sub_g.edges(data=True):
                u_yes = int(_safe_float_metric(sub_g.nodes[u].get(col), default=0.0) > 0)
                v_yes = int(_safe_float_metric(sub_g.nodes[v].get(col), default=0.0) > 0)
                weight = _safe_float_metric(d.get("weight"), default=0.0)
                total_weight += weight
                total_edges += 1
                if u_yes == 1 and v_yes == 1:
                    yy_weight += weight
                    yy_edges += 1
                elif u_yes == 0 and v_yes == 0:
                    nn_weight += weight
                    nn_edges += 1
                else:
                    yn_weight += weight
                    yn_edges += 1

            row[f"{label} - YA-YA Bobot (%)"] = float((yy_weight / total_weight) * 100.0) if total_weight > 0 else 0.0
            row[f"{label} - YA-TIDAK Bobot (%)"] = float((yn_weight / total_weight) * 100.0) if total_weight > 0 else 0.0
            row[f"{label} - TIDAK-TIDAK Bobot (%)"] = float((nn_weight / total_weight) * 100.0) if total_weight > 0 else 0.0
            row[f"{label} - YA-YA Edge (%)"] = float((yy_edges / total_edges) * 100.0) if total_edges > 0 else 0.0
            row[f"{label} - YA-TIDAK Edge (%)"] = float((yn_edges / total_edges) * 100.0) if total_edges > 0 else 0.0
            row[f"{label} - TIDAK-TIDAK Edge (%)"] = float((nn_edges / total_edges) * 100.0) if total_edges > 0 else 0.0
        rows.append(row)
    return pd.DataFrame(rows)


def build_dusun_cluster_composition(graph_obj, dusun_attr, partition=None):
    if graph_obj is None or graph_obj.number_of_nodes() == 0 or not dusun_attr:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    rows = []
    for n in graph_obj.nodes():
        node_attr = graph_obj.nodes[n]
        dusun_raw = node_attr.get(dusun_attr, "Tidak Valid")
        dusun_name = str(dusun_raw).strip() if pd.notnull(dusun_raw) and str(dusun_raw).strip() else "Tidak Valid"
        raw_cluster = partition.get(n, node_attr.get("cluster", -1)) if partition else node_attr.get("cluster", -1)
        try:
            cluster_id = int(raw_cluster)
        except (TypeError, ValueError):
            cluster_id = -1
        rows.append(
            {
                "family_id": n,
                "Dusun": dusun_name,
                "ID Klaster Internal": cluster_id,
            }
        )

    if not rows:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_nodes = pd.DataFrame(rows)
    cluster_order = sorted(df_nodes["ID Klaster Internal"].dropna().unique().tolist())
    valid_cluster_order = [cid for cid in cluster_order if cid >= 0]
    cluster_label_map = {cid: f"Klaster {idx + 1}" for idx, cid in enumerate(valid_cluster_order)}
    for cid in cluster_order:
        if cid < 0:
            cluster_label_map[cid] = "Tidak Terklaster"
    df_nodes["Klaster Louvain"] = df_nodes["ID Klaster Internal"].map(cluster_label_map)
    cluster_label_order = [cluster_label_map[cid] for cid in cluster_order]

    df_long = (
        df_nodes.groupby(["Dusun", "ID Klaster Internal", "Klaster Louvain"], as_index=False)
        .size()
        .rename(columns={"size": "Jumlah KK"})
    )
    dusun_totals = (
        df_nodes.groupby("Dusun", as_index=False)
        .size()
        .rename(columns={"size": "Total KK Dusun"})
    )
    cluster_totals = (
        df_nodes.groupby("Klaster Louvain", as_index=False)
        .size()
        .rename(columns={"size": "Total KK Klaster"})
    )
    total_kk = int(df_nodes.shape[0])
    df_long = df_long.merge(dusun_totals, on="Dusun", how="left").merge(cluster_totals, on="Klaster Louvain", how="left")
    df_long["Persentase dalam Dusun (%)"] = np.where(
        df_long["Total KK Dusun"] > 0,
        (df_long["Jumlah KK"] / df_long["Total KK Dusun"]) * 100.0,
        0.0,
    )
    df_long["Persentase dari Total Graf (%)"] = (df_long["Jumlah KK"] / max(total_kk, 1)) * 100.0
    df_long["Persentase dari Klaster (%)"] = np.where(
        df_long["Total KK Klaster"] > 0,
        (df_long["Jumlah KK"] / df_long["Total KK Klaster"]) * 100.0,
        0.0,
    )
    df_long["Label Persen"] = df_long["Persentase dalam Dusun (%)"].map(lambda v: f"{v:.1f}%" if v >= 5 else "")
    df_long = df_long.sort_values(["Dusun", "ID Klaster Internal"]).reset_index(drop=True)

    count_pivot = (
        df_long.pivot_table(index="Dusun", columns="Klaster Louvain", values="Jumlah KK", aggfunc="sum", fill_value=0)
        .reindex(columns=cluster_label_order, fill_value=0)
        .astype(int)
    )
    pct_pivot = (
        df_long.pivot_table(index="Dusun", columns="Klaster Louvain", values="Persentase dalam Dusun (%)", aggfunc="sum", fill_value=0.0)
        .reindex(columns=cluster_label_order, fill_value=0.0)
    )
    wide_rows = dusun_totals.set_index("Dusun").sort_index().copy()
    for cluster_label in cluster_label_order:
        wide_rows[f"{cluster_label} - Jumlah KK"] = count_pivot.get(cluster_label, pd.Series(0, index=wide_rows.index)).reindex(wide_rows.index, fill_value=0).astype(int)
        wide_rows[f"{cluster_label} - Persentase (%)"] = pct_pivot.get(cluster_label, pd.Series(0.0, index=wide_rows.index)).reindex(wide_rows.index, fill_value=0.0)
    df_wide = wide_rows.reset_index()

    df_overall = (
        df_nodes.groupby(["ID Klaster Internal", "Klaster Louvain"], as_index=False)
        .size()
        .rename(columns={"size": "Jumlah KK"})
        .sort_values("ID Klaster Internal")
        .reset_index(drop=True)
    )
    df_overall["Persentase KK (%)"] = (df_overall["Jumlah KK"] / max(total_kk, 1)) * 100.0
    df_overall["Label Batang"] = df_overall.apply(
        lambda r: f"{int(r['Jumlah KK'])} KK ({float(r['Persentase KK (%)']):.1f}%)",
        axis=1,
    )
    return df_long, df_wide, df_overall


def build_ikr_assortativity_table(graph_obj, dimension_map=None):
    dimension_map = dimension_map or IKR_DIMENSION_MAP
    rows = []
    overall_label, overall_col = IKR_OVERALL_METRIC
    r_overall = safe_numeric_assortativity(graph_obj, overall_col, default=0.0)
    dir_overall, lvl_overall = interpret_assortativity_value(r_overall)
    rows.append(
        {
            "Dimensi IKR": overall_label,
            "Sumber Skor": format_dimension_source_label(overall_col),
            "Kolom Internal": overall_col,
            "Assortativity r": float(r_overall),
            "Arah": dir_overall,
            "Kekuatan": lvl_overall,
            "Jenis": "Agregat",
        }
    )
    for dim_label, col_name in dimension_map:
        r_val = safe_numeric_assortativity(graph_obj, col_name, default=0.0)
        direction, strength = interpret_assortativity_value(r_val)
        rows.append(
            {
                "Dimensi IKR": dim_label,
                "Sumber Skor": format_dimension_source_label(col_name),
                "Kolom Internal": col_name,
                "Assortativity r": float(r_val),
                "Arah": direction,
                "Kekuatan": strength,
                "Jenis": "Dimensi",
            }
        )
    return pd.DataFrame(rows)


def compute_base_five_dimension_summary(df_assort):
    if df_assort is None or df_assort.empty:
        return None
    filter_col = "Kolom Internal" if "Kolom Internal" in df_assort.columns else "Kolom Database"
    df_dims = df_assort[df_assort[filter_col].isin(EDGE_REKAP_COLS)].copy()
    if df_dims.empty:
        return None
    r_mean = float(df_dims["Assortativity r"].mean())
    direction, strength = interpret_assortativity_value(r_mean)
    return {
        "Dimensi IKR": "Ringkasan Lima Dimensi Kesejahteraan",
        "Sumber Skor": "Rata-rata koefisien lima dimensi penyusun",
        "Kolom Internal": "ringkasan_lima_dimensi",
        "Assortativity r": r_mean,
        "Arah": direction,
        "Kekuatan": strength,
        "Jenis": "Ringkasan",
    }

# =========================================================
# 2. CORE ANALYTICS ENGINE
# =========================================================

@st.cache_data
def load_and_clean_ddp(file):
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            xls = pd.ExcelFile(file)
            sheet_name = next(
                (s for s in xls.sheet_names if str(s).lower() == "database"),
                next((s for s in xls.sheet_names if str(s).lower() == "dataset"), xls.sheet_names[0])
            )
            df = xls.parse(sheet_name)
        df.columns = [
            str(c).lower().strip().replace("\u00a0", " ").replace("\n", " ")
            for c in df.columns
        ]
        df.columns = [c.replace("  ", " ").strip() for c in df.columns]

        def pick_col(candidates):
            for cand in candidates:
                if cand in df.columns:
                    return cand
            return None

        subjek_col = pick_col(["subjek", "subyek", "subject"])
        if subjek_col is None:
            subjek_candidates = [c for c in df.columns if "subjek" in c]
            if subjek_candidates:
                subjek_col = subjek_candidates[0]
        if subjek_col is None:
            st.error("Kolom wajib `subjek` tidak ditemukan pada file upload.")
            return pd.DataFrame()

        family_src = pick_col([
            "family_id", "nomor kartu keluarga", "nomor_kartu_keluarga", "no_kk", "nokk", "kk"
        ])
        if family_src is None:
            st.warning("Kolom ID keluarga tidak ditemukan, `family_id` dibuat otomatis dari nomor baris.")
            df["family_id"] = [f"AUTO_FID_{i+1}" for i in range(len(df))]
        else:
            df["family_id"] = df[family_src].astype(str).str.strip()
            bad_fid = df["family_id"].isin(["", "nan", "none"])
            if bad_fid.any():
                df.loc[bad_fid, "family_id"] = [f"AUTO_FID_{i+1}" for i in range(int(bad_fid.sum()))]

        if "deskel" not in df.columns:
            nama_deskel_col = pick_col(["nama deskel", "desa", "nama desa"])
            if nama_deskel_col is not None:
                df["deskel"] = df[nama_deskel_col]
        if "desa" not in df.columns and "deskel" in df.columns:
            df["desa"] = df["deskel"]

        if "par_organisa" not in df.columns and "partisipasi organisasi" in df.columns:
            df["par_organisa"] = df["partisipasi organisasi"]
        if "bansos" not in df.columns and "keikutsertaan program bantuan" in df.columns:
            df["bansos"] = df["keikutsertaan program bantuan"]

        df['subjek_clean'] = df[subjek_col].astype(str).str.lower().str.strip()
        df_kk = df[df['subjek_clean'].str.contains('kepala keluarga', na=False)].drop_duplicates('family_id').copy()
        if df_kk.empty:
            st.error("Tidak ada baris dengan `subjek` berisi 'kepala keluarga'.")
            return pd.DataFrame()

        bansos_col = pick_col(["bansos", "keikutsertaan program bantuan", "program bantuan", "bantuan sosial", "bansos bantuan"])
        media_info_col = pick_col(["media informasi", "media_informasi", "akses informasi", "sumber informasi", "media info"])
        ponsel_col = pick_col(["kepemilikan ponsel", "kepemilikan_ponsel", "memiliki ponsel", "ponsel", "hp"])

        bansos_src = df_kk[bansos_col] if bansos_col in df_kk.columns else pd.Series(["0"] * len(df_kk), index=df_kk.index)
        df_kk['bansos_num'] = bansos_src.apply(to_binary_presence).astype(int)

        if media_info_col in df_kk.columns:
            df_kk['internet_num'] = df_kk[media_info_col].apply(to_binary_presence).astype(int)
        else:
            # Fallback: legacy digital indicator dari wifi/medsos bila kolom media informasi tidak tersedia.
            df_kk['internet_num'] = df_kk.apply(
                lambda r: 1 if (pd.notnull(r.get('wifi')) and _normalize_text(r.get('wifi')) not in {'tidak ada', '0', '0.0', 'nan', 'none', ''})
                or (pd.notnull(r.get('medsos')) and _normalize_text(r.get('medsos')) not in {'tidak ada', '0', '0.0', 'nan', 'none', ''})
                else 0,
                axis=1
            ).astype(int)

        if ponsel_col in df_kk.columns:
            df_kk['ponsel_num'] = df_kk[ponsel_col].apply(to_binary_phone).astype(int)
        else:
            df_kk['ponsel_num'] = 0

        # Backward compatibility untuk bagian dashboard lama yang masih memakai nama digital_num.
        df_kk['digital_num'] = df_kk['internet_num']
        df_kk['organisasi_num'] = df_kk.apply(lambda r: 1 if (pd.notnull(r.get('par_organisa')) or pd.notnull(r.get('par_organisasi'))) and 
                                           str(r.get('par_organisa') if pd.notnull(r.get('par_organisa')) else r.get('par_organisasi')).lower() not in ['0','tidak','tidak ada','nan',''] else 0, axis=1)
        return df_kk
    except Exception as e:
        st.error(f"Gagal memproses file: {e}")
        return None

def build_sna_network(
    df_v,
    basis_col,
    threshold_val=None,
    auto_threshold=True,
    lcc_only=True,
    similarity_method="cosine",
    force_louvain_lcc=False,
    threshold_grid=None,
    edge_feature_cols=None,
    onehot_round_decimals=2,
):
    if len(df_v) < 5:
        return None
    method_norm = str(similarity_method or "cosine").lower().strip()
    if method_norm not in {"cosine", "jaccard", "pearson"}:
        method_norm = "cosine"
    if threshold_grid is None:
        threshold_grid = [round(x, 1) for x in np.arange(0.1, 1.0, 0.1)]

    G = nx.Graph()
    threshold_used = 0.4
    threshold_distribution = []
    threshold_sensitivity = []
    pairwise_similarity_values = []
    if not edge_feature_cols:
        st.error("Kolom fitur edge belum ditentukan.")
        return None
    col_lookup = {str(c).lower().strip(): c for c in df_v.columns}
    resolved_feature_cols = []
    missing_cols = []
    for c in edge_feature_cols:
        key = str(c).lower().strip()
        if key in col_lookup:
            resolved_feature_cols.append(col_lookup[key])
        else:
            missing_cols.append(c)
    feature_cols = tuple(resolved_feature_cols)
    required_cols = {"family_id", *feature_cols}
    missing_cols += [c for c in required_cols if c not in df_v.columns]
    if missing_cols:
        st.error(f"Kolom fitur edge belum lengkap: {', '.join(missing_cols)}")
        return None
    df_builder = df_v.copy()
    df_builder = df_builder[df_builder["family_id"].notna()].drop_duplicates("family_id").copy()
    if len(df_builder) < 5:
        return None
    node_data = df_builder.set_index("family_id").to_dict("index")
    for nid, attr in node_data.items():
        G.add_node(nid, **attr)
    ids = list(node_data.keys())
    feature_matrix = build_onehot_feature_matrix(
        df_builder,
        feature_cols,
        rounding_decimals=onehot_round_decimals,
    ).astype(float)
    feature_vectors = {
        fid: feature_matrix.iloc[idx].to_numpy(dtype=float)
        for idx, fid in enumerate(df_builder["family_id"].tolist())
    }
    candidate_edges = []
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            vec_i = feature_vectors[ids[i]]
            vec_j = feature_vectors[ids[j]]
            if method_norm == "cosine":
                sim_weight = compute_cosine_similarity(vec_i, vec_j)
            elif method_norm == "jaccard":
                sim_weight = compute_jaccard_similarity(vec_i, vec_j)
            else:
                sim_weight = compute_pearson_similarity(vec_i, vec_j)
            pairwise_similarity_values.append(float(sim_weight))
            candidate_edges.append((ids[i], ids[j], float(sim_weight)))
    if auto_threshold:
        threshold_used, threshold_distribution = compute_auto_threshold_from_distribution(
            pairwise_similarity_values,
            threshold_grid=threshold_grid,
        )
        threshold_sensitivity = compute_threshold_sensitivity_analysis(
            ids,
            candidate_edges,
            threshold_grid=threshold_grid,
            lcc_only=lcc_only,
            force_louvain_lcc=force_louvain_lcc,
        )
        threshold_sensitivity = merge_threshold_distribution_with_sensitivity(
            threshold_distribution,
            threshold_sensitivity,
        )
        threshold_distribution = threshold_sensitivity
    else:
        try:
            threshold_used = float(threshold_val)
        except (TypeError, ValueError):
            threshold_used = 0.4
    for u, v, sim_weight in candidate_edges:
        if sim_weight >= threshold_used:
            G.add_edge(u, v, weight=sim_weight)

    if G.number_of_edges() == 0:
        return None
    G_lcc = G.subgraph(max(nx.connected_components(G), key=len)).copy()
    G_target = G_lcc if lcc_only else G.copy()
    partition_graph = G_lcc if force_louvain_lcc else G_target

    partition_raw = community_louvain.best_partition(partition_graph, weight='weight', random_state=42)
    basis_for_cluster_order = resolve_basis_column(df_v, basis_col)
    if basis_for_cluster_order:
        cluster_means = {
            cid: np.mean(
                [
                    _safe_float_metric(G_target.nodes[n].get(basis_for_cluster_order), default=0.0)
                    for n, c in partition_raw.items()
                    if c == cid
                ]
            )
            for cid in set(partition_raw.values())
        }
    else:
        cluster_means = {
            cid: float(
                np.mean([G_target.degree(n, weight="weight") for n, c in partition_raw.items() if c == cid])
            )
            for cid in set(partition_raw.values())
        }
    reorder_map = {old: new for new, (old, _) in enumerate(sorted(cluster_means.items(), key=lambda x: x[1]))}
    partition = {node: reorder_map[cid] for node, cid in partition_raw.items()}
    if not lcc_only and force_louvain_lcc:
        for n in G_target.nodes():
            if n not in partition:
                partition[n] = -1
    nx.set_node_attributes(G_target, partition, 'cluster')

    meta = {
        "raw_nodes": G.number_of_nodes(),
        "raw_edges": G.number_of_edges(),
        "lcc_nodes": G_lcc.number_of_nodes(),
        "lcc_edges": G_lcc.number_of_edges(),
        "similarity_method": method_norm,
        "threshold_selected": threshold_used,
        "threshold_auto": bool(auto_threshold),
        "threshold_distribution": threshold_distribution,
        "threshold_sensitivity": threshold_sensitivity,
        "pairwise_similarity_values": pairwise_similarity_values,
        "mode": "LCC only" if lcc_only else "Semua komponen",
        "onehot_round_decimals": int(onehot_round_decimals),
    }
    return G_target, partition, sorted(list(set(reorder_map.values()))), meta


def render_weighting_methods_page(
    df_v,
    edge_feature_cols,
    rounding_decimals=2,
    threshold_grid=None,
    sample_max_nodes=120,
):
    threshold_grid = threshold_grid or [round(x, 1) for x in np.arange(0.1, 1.0, 0.1)]
    demo_threshold = 0.30
    st.markdown("<h1 class='main-header'>Halaman Metode Pembobotan: Cosine Similarity</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div class='premium-hero'><b>Fokus Halaman:</b> Simulasi data pseudo untuk menjelaskan pembobotan edge "
        "dengan <b>Cosine Similarity</b> pada representasi one-hot dari 5 dimensi IKR.</div>",
        unsafe_allow_html=True,
    )
    feature_cols = PSEUDO_DIMENSION_COLS
    pseudo_two_nodes = pd.DataFrame(
        [
            {
                "family_id": "KK_A",
                "Sandang, Pangan, dan Papan": 33,
                "Pendidikan": 70,
                "Sosial, Hukum, dan HAM": 55,
                "Kesehatan dan Pekerjaan": 80,
                "Lingkungan dan Infrastruktur": 61,
            },
            {
                "family_id": "KK_B",
                "Sandang, Pangan, dan Papan": 33,
                "Pendidikan": 70,
                "Sosial, Hukum, dan HAM": 55,
                "Kesehatan dan Pekerjaan": 79,
                "Lingkungan dan Infrastruktur": 62,
            },
        ]
    )
    pseudo_for_onehot = pseudo_two_nodes.copy()
    onehot_two = build_onehot_feature_matrix(
        pseudo_for_onehot,
        feature_cols,
        rounding_decimals=rounding_decimals,
    )

    tab_alur, tab_rumus, tab_sim, tab_dist = st.tabs(
        ["Alur Metode", "Rumus Matematis", "Simulasi 2 Node (Pseudo)", "Distribusi Cosine (Pseudo)"]
    )

    with tab_alur:
        st.markdown(
            "<div class='soft-card'><b>Alur Pembentukan Bobot Edge</b><br>"
            "Input dimensi: <b>Sandang, Pangan, dan Papan; Pendidikan; Sosial, Hukum, dan HAM; "
            "Kesehatan dan Pekerjaan; Lingkungan dan Infrastruktur</b>.<br>"
            "Setiap <b>kepala keluarga (KK)</b> didefinisikan sebagai satu node.<br>"
            "Pembuatan edge dimulai dari pembobotan similarity antar-node (Cosine).<br>"
            "Aturan keputusan threshold:<br>"
            "- Jika similarity <b>&ge; threshold rata-rata</b> -> edge dibuat.<br>"
            "- Jika similarity <b>&lt; threshold rata-rata</b> -> edge tidak dibuat.<br>"
            "Output akhir: <b>graf base siap diproses algoritma Louvain</b>."
            "</div>",
            unsafe_allow_html=True,
        )
        flow_df = pd.DataFrame(
            [
                {"Tahap": "Mulai", "Deskripsi": "Inisialisasi pembentukan graf base."},
                {
                    "Tahap": "Input 5 Dimensi",
                    "Deskripsi": "Masukkan lima skor dimensi kesejahteraan per KK dari rekap IKR.",
                },
                {"Tahap": "Definisi Node", "Deskripsi": "Setiap kepala keluarga (KK) didefinisikan sebagai 1 node."},
                {
                    "Tahap": "Transformasi One-Hot",
                    "Deskripsi": (
                        "Nilai dibulatkan ke bilangan bulat lalu di-encode menjadi vektor biner 0/1."
                        if int(rounding_decimals) == 0
                        else f"Nilai dibulatkan {rounding_decimals} desimal lalu di-encode menjadi vektor biner 0/1."
                    ),
                },
                {
                    "Tahap": "Pembobotan Edge",
                    "Deskripsi": "Hitung similarity antar pasangan node dengan Cosine Similarity: s_ij.",
                },
                {
                    "Tahap": "Rule Threshold",
                    "Deskripsi": "Jika s_ij >= threshold rata-rata -> buat edge (w_ij = s_ij). Jika s_ij < threshold -> edge tidak dibuat.",
                },
                {"Tahap": "Output", "Deskripsi": "Graf base berbobot siap diproses algoritma Louvain."},
            ]
        )
        fig_flow = px.funnel(
            flow_df,
            y="Tahap",
            x=[1] * len(flow_df),
            title="Tahapan Pembobotan Graf Base",
        )
        fig_flow.update_traces(
            marker_color="#1d4ed8",
            text=flow_df["Deskripsi"],
            textposition="inside",
            texttemplate="<b>%{y}</b><br>%{text}",
            insidetextfont=dict(size=12, color="#ffffff"),
        )
        fig_flow.update_layout(
            showlegend=False,
            yaxis_title="",
            xaxis_title="",
            height=640,
            template="plotly_white",
        )
        st.plotly_chart(fig_flow, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        st.dataframe(flow_df, use_container_width=True)

    with tab_rumus:
        st.markdown("#### Cosine Similarity")
        st.latex(r"s_{ij}^{(\cos)} = \frac{\mathbf{x}_i^\top \mathbf{x}_j}{\|\mathbf{x}_i\|_2 \, \|\mathbf{x}_j\|_2}")
        st.latex(r"\mathbf{x}_i^\top \mathbf{x}_j = \sum_{k=1}^{p} x_{ik}x_{jk}")
        st.latex(r"\|\mathbf{x}_i\|_2 = \sqrt{\sum_{k=1}^{p} x_{ik}^2}")
        st.latex(r"w_{ij} = s_{ij}^{(\cos)} \;\;\text{dan edge terbentuk jika}\;\; s_{ij}^{(\cos)} \ge 0.30")
        st.caption(
            "x_i adalah vektor one-hot node i, x_j adalah vektor one-hot node j, "
            "dan s_ij menjadi bobot edge saat lolos threshold."
        )
        st.markdown("#### Notasi Matematis")
        notation_rows = [
            (r"\mathbf{x}_i = [x_{i1},\ldots,x_{ip}]", "Vektor one-hot node i."),
            (r"\mathbf{x}_j = [x_{j1},\ldots,x_{jp}]", "Vektor one-hot node j."),
            (r"x_{ik}\in\{0,1\}", "Komponen ke-k dari node i (aktif/tidak aktif)."),
            (r"\mathbf{x}_i^\top \mathbf{x}_j=\sum_{k=1}^{p}x_{ik}x_{jk}", "Dot product (jumlah kecocokan komponen aktif)."),
            (r"\|\mathbf{x}_i\|_2=\sqrt{\sum_{k=1}^{p}x_{ik}^2}", "Panjang vektor node i."),
            (r"s_{ij}^{(\cos)}=\frac{\mathbf{x}_i^\top \mathbf{x}_j}{\|\mathbf{x}_i\|_2\|\mathbf{x}_j\|_2}", "Skor cosine similarity."),
            (r"w_{ij}=s_{ij}^{(\cos)}", "Bobot edge yang dipakai di graf."),
        ]
        for expr, meaning in notation_rows:
            st.latex(expr)
            st.caption(meaning)

    with tab_sim:
        st.markdown("#### Data 2 Node Terpilih")
        st.dataframe(pseudo_two_nodes, use_container_width=True)
        active_cols = onehot_two.columns[(onehot_two.sum(axis=0) > 0)].tolist()
        onehot_view = onehot_two[active_cols].copy() if active_cols else onehot_two.copy()
        onehot_view.index = pseudo_two_nodes["family_id"].astype(str).tolist()
        st.markdown("#### Hasil One-Hot (kolom aktif)")
        st.dataframe(onehot_view, use_container_width=True)

        vec_a = onehot_two.iloc[0].to_numpy(dtype=float)
        vec_b = onehot_two.iloc[1].to_numpy(dtype=float)
        dot_val = float(np.dot(vec_a, vec_b))
        norm_a = float(np.linalg.norm(vec_a))
        norm_b = float(np.linalg.norm(vec_b))
        cos_val = compute_cosine_similarity(vec_a, vec_b)
        edge_decision = "TERBENTUK" if cos_val >= demo_threshold else "TIDAK TERBENTUK"
        c1, c2 = st.columns(2)
        c1.metric("Cosine", f"{cos_val:.4f}")
        c2.metric("Dot Product", f"{dot_val:.4f}")
        c3, c4 = st.columns(2)
        c3.metric("Threshold Contoh", f"{demo_threshold:.2f}")
        c4.metric("Status Edge", edge_decision)
        st.caption("Contoh 2 node ini disusun agar tingkat kemiripan lolos threshold 0,30.")
        st.markdown("#### Detail Hitung Cosine (2 Node)")
        st.latex(r"\mathbf{x}_i^\top \mathbf{x}_j = \sum_{k=1}^{p}x_{ik}x_{jk}")
        st.latex(rf"\mathbf{{x}}_i^\top \mathbf{{x}}_j = {dot_val:.4f}")
        st.latex(r"\|\mathbf{x}_i\|_2=\sqrt{\sum_{k=1}^{p}x_{ik}^2},\quad \|\mathbf{x}_j\|_2=\sqrt{\sum_{k=1}^{p}x_{jk}^2}")
        st.latex(rf"\|\mathbf{{x}}_i\|_2 = {norm_a:.4f},\quad \|\mathbf{{x}}_j\|_2 = {norm_b:.4f}")
        st.latex(r"s_{ij}^{(\cos)}=\frac{\mathbf{x}_i^\top \mathbf{x}_j}{\|\mathbf{x}_i\|_2\|\mathbf{x}_j\|_2}")
        st.markdown("#### Substitusi Angka ke Persamaan")
        st.latex(rf"s_{{ij}}^{{(\cos)}}=\frac{{{dot_val:.4f}}}{{{norm_a:.4f}\times {norm_b:.4f}}}={cos_val:.4f}")
        st.latex(rf"w_{{ij}} = s_{{ij}}^{{(\cos)}} = {cos_val:.4f}")
        st.latex(rf"\text{{Edge terbentuk jika }} s_{{ij}}^{{(\cos)}} \ge {demo_threshold:.2f}")
        st.latex(rf"{cos_val:.4f} \; {'\\ge' if cos_val >= demo_threshold else '<'} \; {demo_threshold:.2f} \Rightarrow \text{{{edge_decision}}}")
        st.caption(
            "Dot product adalah jumlah hasil kali komponen seposisi pada dua vektor. "
            "Pada one-hot, dot product merepresentasikan jumlah kategori yang sama-sama aktif."
        )

    with tab_dist:
        st.markdown("#### Distribusi Cosine dari Sampel Pseudo")
        rng = np.random.default_rng(42)
        value_pool = {
            "Sandang, Pangan, dan Papan": [33.40, 33.50, 33.60, 34.00],
            "Pendidikan": [70.00, 70.50, 71.00, 71.50],
            "Sosial, Hukum, dan HAM": [55.00, 55.20, 55.40, 55.60],
            "Kesehatan dan Pekerjaan": [79.50, 80.00, 80.50, 81.00],
            "Lingkungan dan Infrastruktur": [61.20, 61.30, 61.40, 61.50],
        }
        n_nodes = int(max(20, sample_max_nodes))
        pseudo_nodes = []
        for idx in range(n_nodes):
            row = {"family_id": f"PS_{idx+1:03d}"}
            for c in feature_cols:
                row[c] = float(rng.choice(value_pool[c]))
            pseudo_nodes.append(row)
        pseudo_df = pd.DataFrame(pseudo_nodes)
        onehot_sample = build_onehot_feature_matrix(pseudo_df, feature_cols, rounding_decimals=rounding_decimals)
        ids = pseudo_df["family_id"].tolist()
        vectors = onehot_sample.to_numpy(dtype=float)
        cos_values = []
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                vec_i = vectors[i]
                vec_j = vectors[j]
                cos_values.append(float(compute_cosine_similarity(vec_i, vec_j)))
        if not cos_values:
            st.warning("Pasangan node pseudo belum cukup.")
            return

        fig_hist = px.histogram(
            x=cos_values,
            nbins=24,
            title="Histogram Cosine Similarity (Data Pseudo)",
        )
        fig_hist.update_layout(xaxis_title="Cosine Similarity", yaxis_title="Frekuensi")
        st.plotly_chart(fig_hist, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

        edge_at_demo = int(np.sum(np.array(cos_values) >= demo_threshold))
        summary_df = pd.DataFrame(
            [
                {
                    "Metode": "Cosine",
                    "Similarity Rata-rata": float(np.mean(cos_values)),
                    "Similarity Median": float(np.median(cos_values)),
                    "Threshold (Contoh)": float(demo_threshold),
                    "Edge Count @ Threshold": int(edge_at_demo),
                }
            ]
        )
        st.dataframe(summary_df, use_container_width=True)
        st.caption(
            f"Pada halaman metode ini, keputusan edge didemokan dengan threshold tetap {demo_threshold:.2f}."
        )


def render_louvain_methods_page(
    n_nodes=60,
    rounding_decimals=2,
    threshold=0.30,
    seed=42,
):
    st.markdown("<h1 class='main-header'>Halaman Metode Louvain (Simulasi Pseudo)</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div class='premium-hero'><b>Fokus Halaman:</b> Menjelaskan logika penerapan Louvain dari graf base berbobot "
        "yang dibangun dari data pseudo lima dimensi kesejahteraan.</div>",
        unsafe_allow_html=True,
    )
    tab_alur, tab_rumus, tab_sim, tab_out = st.tabs(
        ["Alur Louvain", "Rumus Modularity", "Simulasi dari Graf Base Pseudo", "Output Komunitas"]
    )

    with tab_alur:
        louvain_flow = pd.DataFrame(
            [
                {"Tahap": "Input Graf Base", "Deskripsi": "Gunakan graf berbobot hasil similarity antar node."},
                {"Tahap": "Inisialisasi", "Deskripsi": "Setiap node mulai sebagai komunitas sendiri."},
                {"Tahap": "Local Moving", "Deskripsi": "Pindahkan node ke komunitas tetangga jika modularity naik."},
                {"Tahap": "Agregasi", "Deskripsi": "Gabungkan komunitas jadi super-node (graf baru)."},
                {"Tahap": "Iterasi", "Deskripsi": "Ulangi Local Moving + Agregasi sampai Q tidak naik lagi."},
                {"Tahap": "Output", "Deskripsi": "Partisi komunitas final Louvain."},
            ]
        )
        fig_louvain_flow = px.funnel(
            louvain_flow,
            y="Tahap",
            x=[1] * len(louvain_flow),
            title="Tahapan Penerapan Louvain dari Graf Base",
        )
        fig_louvain_flow.update_traces(
            marker_color="#1d4ed8",
            text=louvain_flow["Deskripsi"],
            textposition="inside",
            texttemplate="<b>%{y}</b><br>%{text}",
            insidetextfont=dict(size=12, color="#ffffff"),
        )
        fig_louvain_flow.update_layout(height=560, template="plotly_white", xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_louvain_flow, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        st.dataframe(louvain_flow, use_container_width=True)

    with tab_rumus:
        st.markdown("#### Fungsi Modularity (Q)")
        st.latex(
            r"Q = \frac{1}{2m}\sum_{i,j}\left(A_{ij} - \frac{k_i k_j}{2m}\right)\delta(c_i, c_j)"
        )
        st.markdown("#### Arti Simbol")
        st.latex(r"A_{ij}: \text{bobot edge antara node } i \text{ dan } j")
        st.latex(r"k_i: \text{weighted degree node } i,\;\; k_i=\sum_j A_{ij}")
        st.latex(r"2m: \sum_{i,j} A_{ij}")
        st.latex(r"c_i: \text{komunitas node } i")
        st.latex(r"\delta(c_i,c_j)=1 \text{ jika sama komunitas, selain itu }0")
        st.caption(
            "Louvain mencari partisi komunitas yang memaksimalkan Q. "
            "Node dipindahkan lokal jika meningkatkan modularity."
        )

    # Bangun graf base pseudo dari 5 dimensi, lalu jalankan Louvain.
    rng = np.random.default_rng(int(seed))
    pools = {
        "Sandang, Pangan, dan Papan": [33.40, 33.50, 33.60, 34.00],
        "Pendidikan": [70.00, 70.50, 71.00, 71.50],
        "Sosial, Hukum, dan HAM": [55.00, 55.20, 55.40, 55.60],
        "Kesehatan dan Pekerjaan": [79.50, 80.00, 80.50, 81.00],
        "Lingkungan dan Infrastruktur": [61.20, 61.30, 61.40, 61.50],
    }
    pseudo_rows = []
    for i in range(int(max(20, n_nodes))):
        row = {"family_id": f"LV_{i+1:03d}"}
        for c in PSEUDO_DIMENSION_COLS:
            row[c] = float(rng.choice(pools[c]))
        pseudo_rows.append(row)
    pseudo_df = pd.DataFrame(pseudo_rows)
    onehot = build_onehot_feature_matrix(
        pseudo_df,
        PSEUDO_DIMENSION_COLS,
        rounding_decimals=rounding_decimals,
    )

    G = nx.Graph()
    for _, r in pseudo_df.iterrows():
        G.add_node(r["family_id"], **r.to_dict())
    ids = pseudo_df["family_id"].tolist()
    vecs = onehot.to_numpy(dtype=float)
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            sim = float(compute_cosine_similarity(vecs[i], vecs[j]))
            if sim >= float(threshold):
                G.add_edge(ids[i], ids[j], weight=sim)

    if G.number_of_edges() == 0:
        # fallback agar simulasi tetap jalan jika threshold terlalu tinggi.
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                sim = float(compute_cosine_similarity(vecs[i], vecs[j]))
                if sim >= 0.20:
                    G.add_edge(ids[i], ids[j], weight=sim)

    if G.number_of_edges() > 0:
        init_partition = {n: idx for idx, n in enumerate(G.nodes())}
        q_init = _safe_float_metric(community_louvain.modularity(init_partition, G, weight="weight"), default=0.0)
        final_partition = community_louvain.best_partition(G, weight="weight", random_state=int(seed))
        q_final = _safe_float_metric(community_louvain.modularity(final_partition, G, weight="weight"), default=0.0)
    else:
        init_partition = {n: 0 for n in G.nodes()}
        final_partition = init_partition.copy()
        q_init = 0.0
        q_final = 0.0

    with tab_sim:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Node", G.number_of_nodes())
        c2.metric("Edge", G.number_of_edges())
        c3.metric("Q Awal", f"{q_init:.5f}")
        c4.metric("Q Final Louvain", f"{q_final:.5f}", f"ΔQ = {q_final - q_init:.5f}")

        if G.number_of_edges() > 0:
            pos = nx.spring_layout(G, seed=int(seed), weight="weight")
            nodes = list(G.nodes())
            comm_ids = [final_partition.get(n, 0) for n in nodes]
            fig_graph = go.Figure()
            for u, v, d in G.edges(data=True):
                cu = int(final_partition.get(u, 0))
                cv = int(final_partition.get(v, 0))
                edge_weight = _safe_float_metric(d.get("weight"), 0.0)
                edge_color = (
                    rgba_from_hex(CONTRAST_COLORS[cu % len(CONTRAST_COLORS)], 0.44)
                    if cu == cv
                    else rgba_from_hex(CONTRAST_COLORS[((cu + 1) * 7 + (cv + 1) * 13) % len(CONTRAST_COLORS)], 0.32)
                )
                fig_graph.add_trace(
                    go.Scatter(
                        x=[pos[u][0], pos[v][0], None],
                        y=[pos[u][1], pos[v][1], None],
                        mode="lines",
                        line=dict(width=1.0 + (1.6 * edge_weight), color=edge_color),
                        hoverinfo="none",
                        showlegend=False,
                    )
                )
            fig_graph.add_trace(
                go.Scatter(
                    x=[pos[n][0] for n in nodes],
                    y=[pos[n][1] for n in nodes],
                    mode="markers",
                    marker=dict(
                        size=10,
                        color=comm_ids,
                        colorscale="Blues",
                        showscale=True,
                        colorbar=dict(title="Komunitas"),
                                        line=dict(color=NETWORK_NODE_LINE, width=0.7),
                    ),
                    text=[f"Node: {n}<br>Komunitas: {final_partition.get(n, 0)}" for n in nodes],
                    hoverinfo="text",
                    showlegend=False,
                )
            )
            fig_graph.update_layout(
                title="Graf Base Pseudo setelah Louvain (warna = komunitas)",
                height=560,
                template="plotly_white",
                margin=dict(l=20, r=20, t=60, b=20),
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
            )
            st.plotly_chart(fig_graph, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        else:
            st.warning("Graf pseudo tidak memiliki edge pada konfigurasi saat ini.")

    with tab_out:
        if G.number_of_nodes() == 0:
            st.warning("Tidak ada node untuk dianalisis.")
        else:
            out_df = pd.DataFrame(
                [{"family_id": n, "Komunitas Louvain": int(final_partition.get(n, 0))} for n in G.nodes()]
            )
            degree_map = {n: float(G.degree(n, weight="weight")) for n in G.nodes()}
            pseudo_profile = pseudo_df.copy()
            pseudo_profile["Komunitas Louvain"] = pseudo_profile["family_id"].map(
                lambda n: int(final_partition.get(n, -1))
            )
            pseudo_profile["Weighted Degree"] = pseudo_profile["family_id"].map(
                lambda n: float(degree_map.get(n, 0.0))
            )
            size_df = (
                out_df["Komunitas Louvain"]
                .value_counts()
                .rename_axis("Komunitas Louvain")
                .reset_index(name="Jumlah Node")
                .sort_values("Komunitas Louvain")
                .reset_index(drop=True)
            )
            total_clusters = int(size_df["Komunitas Louvain"].nunique())
            st.markdown("#### Ringkasan Komunitas Louvain")
            csum1, csum2 = st.columns(2)
            csum1.metric("Jumlah Klaster Terbentuk", f"{total_clusters}")
            csum2.metric("Total Node Terpartisi", f"{int(size_df['Jumlah Node'].sum())}")
            st.dataframe(size_df, use_container_width=True)

            cluster_desc = (
                pseudo_profile.groupby("Komunitas Louvain", as_index=False)
                .agg(
                    Jumlah_Node=("family_id", "count"),
                    Rerata_Sandang_Pangan_Papan=("Sandang, Pangan, dan Papan", "mean"),
                    Rerata_Pendidikan=("Pendidikan", "mean"),
                    Rerata_Sosial_Hukum_HAM=("Sosial, Hukum, dan HAM", "mean"),
                    Rerata_Kesehatan_Pekerjaan=("Kesehatan dan Pekerjaan", "mean"),
                    Rerata_Lingkungan_Infrastruktur=("Lingkungan dan Infrastruktur", "mean"),
                    Rerata_Weighted_Degree=("Weighted Degree", "mean"),
                )
                .sort_values("Komunitas Louvain")
                .reset_index(drop=True)
            )
            cluster_desc["Rerata_IKR_Agregat"] = cluster_desc[
                [
                    "Rerata_Sandang_Pangan_Papan",
                    "Rerata_Pendidikan",
                    "Rerata_Sosial_Hukum_HAM",
                    "Rerata_Kesehatan_Pekerjaan",
                    "Rerata_Lingkungan_Infrastruktur",
                ]
            ].mean(axis=1)
            st.markdown("#### Statistik Deskriptif per Klaster")
            st.dataframe(
                cluster_desc.style.format(
                    {
                        "Rerata_Sandang_Pangan_Papan": "{:.2f}",
                        "Rerata_Pendidikan": "{:.2f}",
                        "Rerata_Sosial_Hukum_HAM": "{:.2f}",
                        "Rerata_Kesehatan_Pekerjaan": "{:.2f}",
                        "Rerata_Lingkungan_Infrastruktur": "{:.2f}",
                        "Rerata_IKR_Agregat": "{:.2f}",
                        "Rerata_Weighted_Degree": "{:.2f}",
                    }
                ),
                use_container_width=True,
            )

            cluster_profile_long = cluster_desc.melt(
                id_vars=["Komunitas Louvain"],
                value_vars=[
                    "Rerata_Sandang_Pangan_Papan",
                    "Rerata_Pendidikan",
                    "Rerata_Sosial_Hukum_HAM",
                    "Rerata_Kesehatan_Pekerjaan",
                    "Rerata_Lingkungan_Infrastruktur",
                    "Rerata_IKR_Agregat",
                ],
                var_name="Indikator",
                value_name="Rerata Nilai",
            )
            cluster_profile_long["Indikator"] = cluster_profile_long["Indikator"].map(
                {
                    "Rerata_Sandang_Pangan_Papan": "Sandang, Pangan, dan Papan",
                    "Rerata_Pendidikan": "Pendidikan",
                    "Rerata_Sosial_Hukum_HAM": "Sosial, Hukum, dan HAM",
                    "Rerata_Kesehatan_Pekerjaan": "Kesehatan dan Pekerjaan",
                    "Rerata_Lingkungan_Infrastruktur": "Lingkungan dan Infrastruktur",
                    "Rerata_IKR_Agregat": "IKR Agregat",
                }
            )
            fig_cluster_profile = px.bar(
                cluster_profile_long,
                x="Komunitas Louvain",
                y="Rerata Nilai",
                color="Indikator",
                barmode="group",
                title="Profil Rerata Dimensi per Klaster Louvain",
            )
            st.plotly_chart(fig_cluster_profile, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

            st.markdown("#### Narasi Otomatis Karakter Tiap Klaster")
            for _, row in cluster_desc.iterrows():
                cid = int(row["Komunitas Louvain"])
                n_k = int(row["Jumlah_Node"])
                avg_ikr = float(row["Rerata_IKR_Agregat"])
                ikr_lbl, _ = categorize_ikr_bps(avg_ikr)
                if avg_ikr >= 75:
                    tone = "klaster relatif kuat secara skor dimensi"
                elif avg_ikr >= 65:
                    tone = "klaster menengah dengan kapasitas campuran"
                else:
                    tone = "klaster dengan kerentanan dimensi yang perlu prioritas"
                st.markdown(
                    f"<div class='soft-card'><b>Klaster {cid}</b> berisi <b>{n_k}</b> node, "
                    f"rerata IKR agregat simulasi <b>{avg_ikr:.2f}</b> (kategori <b>{ikr_lbl}</b>), "
                    f"dengan rerata weighted degree <b>{float(row['Rerata_Weighted_Degree']):.2f}</b>. "
                    f"Interpretasi cepat: {tone}.</div>",
                    unsafe_allow_html=True,
                )

            st.markdown("#### Sampel Hasil Partisi Node")
            st.dataframe(out_df.head(30), use_container_width=True)
            st.caption(
                "Logika penerapan: graf base pseudo -> optimasi modularity (Louvain) -> partisi komunitas final."
            )


def render_assortativity_methods_page(
    n_nodes=90,
    seed=42,
):
    st.markdown("<h1 class='main-header'>Halaman Metode Assortativity (Komprehensif)</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div class='premium-hero'><b>Fokus Halaman:</b> Membahas semua jenis assortativity pada kode ini: "
        "numeric assortativity, attribute assortativity (biner/kategorikal), dan within-between assortativity (Montes).</div>",
        unsafe_allow_html=True,
    )

    rng = np.random.default_rng(int(seed))
    n_nodes = int(max(30, n_nodes))
    cluster_count = 3
    cluster_ids = [int(i % cluster_count) for i in range(n_nodes)]
    node_ids = [f"AS_{i+1:03d}" for i in range(n_nodes)]

    G = nx.Graph()
    rows = []
    for idx, nid in enumerate(node_ids):
        c = int(cluster_ids[idx])
        base = 62 + (c * 7)
        f_a = float(np.clip(rng.normal(base + 1.5, 2.2), 45, 95))
        f_b = float(np.clip(rng.normal(base + 0.8, 2.5), 45, 95))
        f_c = float(np.clip(rng.normal(base - 0.5, 2.4), 45, 95))
        f_d = float(np.clip(rng.normal(base + 1.2, 2.6), 45, 95))
        f_e = float(np.clip(rng.normal(base + 0.2, 2.7), 45, 95))
        f_ikr = float(np.mean([f_a, f_b, f_c, f_d, f_e]))

        bansos_num = int(rng.random() < (0.70 if c == 0 else 0.40 if c == 1 else 0.18))
        internet_num = int(rng.random() < (0.28 if c == 0 else 0.56 if c == 1 else 0.82))
        ponsel_num = int(rng.random() < (0.45 if c == 0 else 0.66 if c == 1 else 0.86))
        dusun = f"Dusun-{c+1}"
        cat_label, cat_code = categorize_ikr_bps(f_ikr)

        attrs = {
            "family_id": nid,
            "cluster": c,
            "f_a_dari_rekap_kk": f_a,
            "f_b_dari_rekap_kk": f_b,
            "f_c_dari_rekap_kk": f_c,
            "f_d_dari_rekap_kk": f_d,
            "f_e_dari_rekap_kk": f_e,
            "f_ikr_dari_rekap_kk": f_ikr,
            "bansos_num": bansos_num,
            "internet_num": internet_num,
            "ponsel_num": ponsel_num,
            "dusun": dusun,
            "kategori_ikr": cat_label,
            "kategori_ikr_code": int(cat_code),
        }
        G.add_node(nid, **attrs)
        rows.append(attrs)

    for i in range(n_nodes):
        for j in range(i + 1, n_nodes):
            ci = cluster_ids[i]
            cj = cluster_ids[j]
            p_edge = 0.18 if ci == cj else 0.045
            if rng.random() < p_edge:
                si = rows[i]["f_ikr_dari_rekap_kk"]
                sj = rows[j]["f_ikr_dari_rekap_kk"]
                w = float(np.clip(1.0 - (abs(si - sj) / 60.0), 0.05, 1.0))
                G.add_edge(node_ids[i], node_ids[j], weight=w)

    if G.number_of_edges() == 0:
        st.warning("Graf pseudo assortativity belum memiliki edge. Ubah seed/jumlah node.")
        return

    tab_alur, tab_konsep, tab_num, tab_attr, tab_montes, tab_ringkas = st.tabs(
        ["Alur Assortativity", "Konsep & Rumus", "Numeric Assortativity", "Attribute Assortativity", "Within-Between Montes", "Ringkasan Interpretasi"]
    )

    with tab_alur:
        flow_assort = pd.DataFrame(
            [
                {"Tahap": "Input Graf Base", "Deskripsi": "Gunakan graf berbobot beserta atribut node."},
                {"Tahap": "Pilih Jenis Assortativity", "Deskripsi": "Numeric / Attribute / Within-Between (Montes)."},
                {"Tahap": "Hitung Metrik", "Deskripsi": "Dapatkan r (numeric/attribute), Qw*, dan Qb*."},
                {"Tahap": "Bandingkan Hasil", "Deskripsi": "Bandingkan antar dimensi/atribut untuk menemukan yang dominan."},
                {"Tahap": "Interpretasi", "Deskripsi": "Baca arah (asortatif/disasortatif) dan kekuatan pola."},
                {"Tahap": "Output Analitik", "Deskripsi": "Rekomendasi audit sosial-kebijakan berbasis hasil assortativity."},
            ]
        )
        fig_flow_assort = px.funnel(
            flow_assort,
            y="Tahap",
            x=[1] * len(flow_assort),
            title="Tahapan Analisis Assortativity",
        )
        fig_flow_assort.update_traces(
            marker_color="#1d4ed8",
            text=flow_assort["Deskripsi"],
            textposition="inside",
            texttemplate="<b>%{y}</b><br>%{text}",
            insidetextfont=dict(size=12, color="#ffffff"),
        )
        fig_flow_assort.update_layout(height=560, template="plotly_white", xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_flow_assort, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        st.dataframe(flow_assort, use_container_width=True)

    with tab_konsep:
        k1, k2, k3 = st.columns(3)
        k1.metric("Jumlah Node (Pseudo)", G.number_of_nodes())
        k2.metric("Jumlah Edge (Pseudo)", G.number_of_edges())
        k3.metric("Jumlah Klaster Simulasi", cluster_count)
        st.markdown(
            "<div class='soft-card'><b>Cara Baca Nilai Assortativity (r):</b><br>"
            "<b>r > 0</b> = cenderung terhubung dengan node yang mirip (asortatif).<br>"
            "<b>r = 0</b> = cenderung acak/campuran.<br>"
            "<b>r < 0</b> = cenderung terhubung dengan node yang berbeda (disasortatif).<br>"
            "Semakin besar |r|, semakin kuat pola pemilahannya.</div>",
            unsafe_allow_html=True,
        )
        st.markdown("#### 1) Numeric Assortativity (Newman)")
        st.latex(r"r = \mathrm{corr}(x_u, x_v)\;\; \text{untuk setiap edge } (u,v)")
        st.markdown("#### 2) Attribute Assortativity (Kategorikal/Biner)")
        st.latex(r"r = \frac{\sum_i e_{ii} - \sum_i a_i b_i}{1 - \sum_i a_i b_i}")
        st.markdown("#### 3) Within-Between Assortativity (Montes)")
        st.latex(r"Q_w^* = \frac{Q_w}{Q_{w,\max}},\quad Q_b^* = \frac{Q_b}{Q_{b,\max}}")
        st.caption(
            "Di kode ini: numeric assortativity dihitung untuk lima dimensi kesejahteraan dan IKR agregat; "
            "attribute assortativity untuk bansos, internet, ponsel, dan dusun; "
            "Within-Between memakai kategori IKR (BPS) sebagai category_attr dan klaster sebagai group_attr."
        )
        cara_baca_df = pd.DataFrame(
            [
                {"Rentang Nilai": "r >= 0.50", "Interpretasi": "Asortatif kuat (pengelompokan tegas)"},
                {"Rentang Nilai": "0.30 <= r < 0.50", "Interpretasi": "Asortatif sedang"},
                {"Rentang Nilai": "0.10 <= r < 0.30", "Interpretasi": "Asortatif lemah"},
                {"Rentang Nilai": "-0.10 < r < 0.10", "Interpretasi": "Campuran / mendekati acak"},
                {"Rentang Nilai": "r <= -0.10", "Interpretasi": "Disasortatif (lebih sering lintas kategori)"},
            ]
        )
        st.dataframe(cara_baca_df, use_container_width=True)
        st.markdown("#### Kenapa Masuk Jenis Ini?")
        jenis_df = pd.DataFrame(
            [
                {
                    "Objek/Metrik": "Lima dimensi kesejahteraan dan IKR agregat",
                    "Jenis Assortativity": "Numeric Assortativity",
                    "Alasan Metodologis": "Nilai berbentuk skor kontinu, sehingga yang diukur adalah korelasi nilai antar-node pada edge.",
                    "Rumus Inti": "r = corr(x_u, x_v) pada edge (u,v)",
                },
                {
                    "Objek/Metrik": "Bansos, Internet, Ponsel, Dusun",
                    "Jenis Assortativity": "Attribute Assortativity",
                    "Alasan Metodologis": "Nilai berbentuk kategori/biner, sehingga yang diukur adalah kecenderungan edge menghubungkan kategori yang sama.",
                    "Rumus Inti": "r berbasis matriks mixing kategori",
                },
                {
                    "Objek/Metrik": "Kategori IKR BPS + Klaster",
                    "Jenis Assortativity": "Within-Between (Montes)",
                    "Alasan Metodologis": "Tujuannya memisahkan pola homogenitas di dalam klaster vs antar-klaster.",
                    "Rumus Inti": "Qw* (within), Qb* (between)",
                },
            ]
        )
        st.dataframe(jenis_df, use_container_width=True)
        fig_class = px.treemap(
            jenis_df,
            path=["Jenis Assortativity", "Objek/Metrik"],
            values=[1, 1, 1],
            color="Jenis Assortativity",
            color_discrete_sequence=["#1d4ed8", "#2563eb", "#60a5fa"],
            title="Peta Klasifikasi Jenis Assortativity di Dashboard",
        )
        st.plotly_chart(fig_class, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    df_nodes = pd.DataFrame(rows)
    edge_pairs = []
    for u, v, d in G.edges(data=True):
        au = G.nodes[u]
        av = G.nodes[v]
        edge_pairs.append(
            {
                "u": u,
                "v": v,
                "weight": _safe_float_metric(d.get("weight"), 0.0),
                "cluster_u": int(au.get("cluster", -1)),
                "cluster_v": int(av.get("cluster", -1)),
                "f_a_u": _safe_float_metric(au.get("f_a_dari_rekap_kk"), np.nan),
                "f_a_v": _safe_float_metric(av.get("f_a_dari_rekap_kk"), np.nan),
                "f_b_u": _safe_float_metric(au.get("f_b_dari_rekap_kk"), np.nan),
                "f_b_v": _safe_float_metric(av.get("f_b_dari_rekap_kk"), np.nan),
                "f_c_u": _safe_float_metric(au.get("f_c_dari_rekap_kk"), np.nan),
                "f_c_v": _safe_float_metric(av.get("f_c_dari_rekap_kk"), np.nan),
                "f_d_u": _safe_float_metric(au.get("f_d_dari_rekap_kk"), np.nan),
                "f_d_v": _safe_float_metric(av.get("f_d_dari_rekap_kk"), np.nan),
                "f_e_u": _safe_float_metric(au.get("f_e_dari_rekap_kk"), np.nan),
                "f_e_v": _safe_float_metric(av.get("f_e_dari_rekap_kk"), np.nan),
                "f_ikr_u": _safe_float_metric(au.get("f_ikr_dari_rekap_kk"), np.nan),
                "f_ikr_v": _safe_float_metric(av.get("f_ikr_dari_rekap_kk"), np.nan),
                "bansos_u": str(au.get("bansos_num", "NA")),
                "bansos_v": str(av.get("bansos_num", "NA")),
                "internet_u": str(au.get("internet_num", "NA")),
                "internet_v": str(av.get("internet_num", "NA")),
                "ponsel_u": str(au.get("ponsel_num", "NA")),
                "ponsel_v": str(av.get("ponsel_num", "NA")),
                "dusun_u": str(au.get("dusun", "NA")),
                "dusun_v": str(av.get("dusun", "NA")),
            }
        )
    df_edges = pd.DataFrame(edge_pairs)
    numeric_specs = list(IKR_DIMENSION_MAP) + [IKR_OVERALL_METRIC]
    numeric_rows = []
    for lbl, col in numeric_specs:
        r_val = safe_numeric_assortativity(G, col, default=0.0)
        direction, strength = interpret_assortativity_value(r_val)
        numeric_rows.append(
            {"Metrik": lbl, "Sumber Skor": format_dimension_source_label(col), "r": float(r_val), "Arah": direction, "Kekuatan": strength}
        )
    df_num = pd.DataFrame(numeric_rows)

    with tab_num:
        st.markdown("#### Assortativity Numerik per Dimensi")
        st.caption(
            "Tab ini menjawab: dimensi skor mana yang paling mendorong pemilahan keterhubungan antar node."
        )
        with st.expander("Langkah Hitung Numeric Assortativity (Detail)", expanded=False):
            st.markdown("1. Ambil semua pasangan node yang terhubung (edge).")
            st.markdown("2. Untuk tiap edge, ambil nilai dimensi di node kiri dan kanan.")
            st.markdown("3. Hitung korelasi pasangan nilai tersebut.")
            st.markdown("4. Korelasi itulah nilai `r` numeric assortativity.")
        df_num_sorted = df_num.sort_values("r", ascending=False).reset_index(drop=True)
        fig_num = px.bar(
            df_num_sorted,
            x="r",
            y="Metrik",
            orientation="h",
            color="r",
            color_continuous_scale="Blues",
            range_color=[-1, 1],
            title="Perbandingan Numeric Assortativity",
            hover_data=["Sumber Skor", "Arah", "Kekuatan"],
        )
        fig_num.add_vline(x=0.0, line_dash="dash", line_color="#475569")
        fig_num.update_traces(text=df_num_sorted["r"].map(lambda x: f"{x:.3f}"), textposition="outside")
        st.plotly_chart(fig_num, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        st.dataframe(df_num.style.background_gradient(cmap="Blues", subset=["r"]), use_container_width=True)
        top_num_local = df_num.iloc[df_num["r"].abs().idxmax()]
        st.markdown(
            f"<div class='soft-card'><b>Interpretasi Cepat Numeric:</b><br>"
            f"Dimensi paling dominan adalah <b>{top_num_local['Metrik']}</b> dengan r=<b>{float(top_num_local['r']):.4f}</b> "
            f"({top_num_local['Arah']} | {top_num_local['Kekuatan']}).</div>",
            unsafe_allow_html=True,
        )
        numeric_pair_map = {
            "Sandang, Pangan, dan Papan": ("f_a_u", "f_a_v"),
            "Pendidikan": ("f_b_u", "f_b_v"),
            "Sosial, Hukum, dan HAM": ("f_c_u", "f_c_v"),
            "Kesehatan dan Pekerjaan": ("f_d_u", "f_d_v"),
            "Lingkungan dan Infrastruktur": ("f_e_u", "f_e_v"),
            "IKR Agregat": ("f_ikr_u", "f_ikr_v"),
        }
        chosen_metric = st.selectbox(
            "Visual Pair Nilai per Edge (Numeric)",
            options=list(numeric_pair_map.keys()),
            index=5,
        )
        ux, vx = numeric_pair_map[chosen_metric]
        if not df_edges.empty:
            fig_pair = px.scatter(
                df_edges,
                x=ux,
                y=vx,
                color="weight",
                color_continuous_scale="Blues",
                title=f"Pasangan Nilai pada Setiap Edge - {chosen_metric}",
                labels={ux: f"{chosen_metric} (Node U)", vx: f"{chosen_metric} (Node V)", "weight": "Bobot Edge"},
                hover_data=["u", "v", "cluster_u", "cluster_v"],
            )
            min_xy = float(np.nanmin([df_edges[ux].min(), df_edges[vx].min()]))
            max_xy = float(np.nanmax([df_edges[ux].max(), df_edges[vx].max()]))
            fig_pair.add_trace(
                go.Scatter(
                    x=[min_xy, max_xy],
                    y=[min_xy, max_xy],
                    mode="lines",
                    line=dict(color="#334155", dash="dash"),
                    name="Garis x=y",
                )
            )
            st.plotly_chart(fig_pair, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
            st.caption("Semakin rapat titik di sekitar garis x=y, semakin tinggi kecenderungan asortatif untuk dimensi itu.")

    attr_specs = [
        ("Bansos", "bansos_num"),
        ("Internet", "internet_num"),
        ("Ponsel", "ponsel_num"),
        ("Dusun", "dusun"),
    ]
    attr_rows = []
    for lbl, col in attr_specs:
        r_attr = safe_attribute_assortativity(G, col, default=0.0)
        d_attr, s_attr = interpret_assortativity_value(r_attr)
        montes_attr = compute_montes_within_between_assortativity(
            G,
            category_attr=col,
            group_attr="cluster",
            invalid_category_values=None,
        )
        attr_rows.append(
            {
                "Metrik": lbl,
                "Kolom": col,
                "r": float(r_attr),
                "Qw*": float(montes_attr["q_w_star"]),
                "Qb*": float(montes_attr["q_b_star"]),
                "Arah": d_attr,
                "Kekuatan": s_attr,
                "Label Steinley": steinley_segregation_label(r_attr),
            }
        )
    df_attr = pd.DataFrame(attr_rows)
    audit_qw_mean = float(df_attr["Qw*"].mean()) if not df_attr.empty else 0.0
    audit_qb_mean = float(df_attr["Qb*"].mean()) if not df_attr.empty else 0.0

    with tab_attr:
        st.markdown("#### Assortativity Atribut (Biner/Kategorikal)")
        st.caption(
            "Tab ini menunjukkan atribut kebijakan/spasial mana yang paling homogen dalam jaringan."
        )
        st.markdown(
            "<div class='soft-card'><b>Cara Baca Khusus Audit (Bansos/Internet/Ponsel/Dusun):</b><br>"
            "<b>Nilai r</b> menilai seberapa kuat atribut tersebut mengikuti struktur keterhubungan pada <b>graf base</b> "
            "(semakin besar |r|, semakin kuat keterkaitannya dengan pola graf base).<br>"
            "<b>Nilai Qw*</b> dan <b>Qb*</b> baru dipakai untuk memecah konteks relasi menjadi "
            "<b>intra-klaster (within)</b> dan <b>inter-klaster (between)</b>."
            "</div>",
            unsafe_allow_html=True,
        )
        with st.expander("Langkah Hitung Attribute Assortativity (Detail)", expanded=False):
            st.markdown("1. Untuk tiap edge, baca kategori atribut di kedua ujung edge.")
            st.markdown("2. Hitung proporsi edge yang menghubungkan kategori sama vs berbeda.")
            st.markdown("3. Bandingkan dengan proporsi acak yang diharapkan.")
            st.markdown("4. Hasil normalisasinya menjadi nilai `r` attribute assortativity.")
        fig_attr = px.bar(
            df_attr,
            x="Metrik",
            y="r",
            color="r",
            color_continuous_scale="Blues",
            range_color=[-1, 1],
            title="Perbandingan Attribute Assortativity",
            hover_data=["Kolom", "Qw*", "Qb*", "Arah", "Kekuatan", "Label Steinley"],
        )
        fig_attr.add_hline(y=0.0, line_dash="dash", line_color="#475569")
        st.plotly_chart(fig_attr, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        st.dataframe(df_attr.style.background_gradient(cmap="Blues", subset=["r"]), use_container_width=True)
        melt_attr = df_attr.melt(
            id_vars=["Metrik"],
            value_vars=["r", "Qw*", "Qb*"],
            var_name="Komponen",
            value_name="Nilai",
        )
        fig_attr_comp = px.bar(
            melt_attr,
            x="Metrik",
            y="Nilai",
            color="Komponen",
            barmode="group",
            title="Perbandingan Komponen r vs Qw* vs Qb*",
            color_discrete_sequence=["#1d4ed8", "#60a5fa", "#93c5fd"],
        )
        fig_attr_comp.add_hline(y=0.0, line_dash="dash", line_color="#475569")
        st.plotly_chart(fig_attr_comp, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        attr_choice = st.selectbox(
            "Detail Matriks Kategori per Edge",
            options=["Bansos", "Internet", "Ponsel", "Dusun"],
            index=0,
        )
        attr_edge_cols = {
            "Bansos": ("bansos_u", "bansos_v"),
            "Internet": ("internet_u", "internet_v"),
            "Ponsel": ("ponsel_u", "ponsel_v"),
            "Dusun": ("dusun_u", "dusun_v"),
        }
        cu, cv = attr_edge_cols[attr_choice]
        if not df_edges.empty:
            ct = pd.crosstab(df_edges[cu], df_edges[cv]).sort_index().sort_index(axis=1)
            heat = go.Figure(
                data=go.Heatmap(
                    z=ct.values,
                    x=[str(x) for x in ct.columns],
                    y=[str(y) for y in ct.index],
                    colorscale="Blues",
                    colorbar=dict(title="Jumlah Edge"),
                    text=ct.values,
                    texttemplate="%{text}",
                )
            )
            heat.update_layout(
                title=f"Matriks Kategori Edge: {attr_choice} (Node U vs Node V)",
                xaxis_title=f"{attr_choice} Node V",
                yaxis_title=f"{attr_choice} Node U",
                height=430,
                template="plotly_white",
            )
            st.plotly_chart(heat, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
            same_ratio = float(np.mean(df_edges[cu].astype(str).values == df_edges[cv].astype(str).values))
            pie_df = pd.DataFrame(
                [
                    {"Kondisi Edge": "Kategori Sama", "Proporsi": same_ratio},
                    {"Kondisi Edge": "Kategori Berbeda", "Proporsi": 1.0 - same_ratio},
                ]
            )
            fig_pie = px.pie(
                pie_df,
                names="Kondisi Edge",
                values="Proporsi",
                color="Kondisi Edge",
                color_discrete_sequence=["#1d4ed8", "#93c5fd"],
                hole=0.55,
                title=f"Komposisi Edge Sama vs Berbeda - {attr_choice}",
            )
            st.plotly_chart(fig_pie, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
            st.caption(
                "Semakin besar porsi 'Kategori Sama', biasanya r attribute cenderung makin positif."
            )

    montes_res = compute_montes_within_between_assortativity(
        G,
        category_attr="kategori_ikr_code",
        group_attr="cluster",
        invalid_category_values={0},
    )
    q_w_star = float(montes_res["q_w_star"])
    q_b_star = float(montes_res["q_b_star"])

    with tab_montes:
        st.markdown("#### Within-Between Assortativity (Montes) dengan Kategori BPS")
        st.markdown(
            "<div class='soft-card'><b>Pembeda Inti Audit vs BPS:</b><br>"
            "<b>Within-Between Audit</b> di sini dihitung per atribut kebijakan/spasial (Bansos, Internet, Ponsel, Dusun). "
            "Artinya category_attr berubah sesuai atribut yang diaudit.<br>"
            "<b>Within-Between BPS</b> dihitung khusus dari kategori IKR BPS (kategori_ikr_code), "
            "sehingga fokusnya stratifikasi kesejahteraan, bukan atribut program tertentu."
            "</div>",
            unsafe_allow_html=True,
        )
        st.caption(
            "Ringkasnya: pada audit, r = kekuatan keterkaitan atribut dengan graf base; "
            "Qw*/Qb* = pemisahan pola intra vs inter klaster."
        )
        with st.expander("Langkah Hitung Within-Between (Detail)", expanded=False):
            st.markdown("1. Tetapkan `x` = kategori IKR (BPS), dan `h` = klaster.")
            st.markdown("2. Pisahkan kontribusi edge dalam-klaster (within) dan antar-klaster (between).")
            st.markdown("3. Hitung skor mentah Qw, Qb lalu normalisasi jadi Qw*, Qb*.")
            st.markdown("4. Interpretasikan: Qw* tinggi = homogen dalam klaster; Qb* tinggi = homogen antar-klaster.")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Qw*", f"{q_w_star:.5f}")
        m2.metric("Qb*", f"{q_b_star:.5f}")
        m3.metric("m_w", f"{float(montes_res['m_w']):.4f}")
        m4.metric("m_b", f"{float(montes_res['m_b']):.4f}")
        df_montes = pd.DataFrame(
            [
                {"Komponen": "Qw* (Within)", "Nilai": q_w_star},
                {"Komponen": "Qb* (Between)", "Nilai": q_b_star},
            ]
        )
        fig_montes = px.bar(
            df_montes,
            x="Komponen",
            y="Nilai",
            color="Nilai",
            color_continuous_scale="Blues",
            title="Skor Normalized Within-Between Assortativity",
        )
        fig_montes.add_hline(y=0.0, line_dash="dash", line_color="#475569")
        st.plotly_chart(fig_montes, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        wb_compare_df = pd.DataFrame(
            [
                {"Kelompok": "Audit (Rata-rata 4 atribut)", "Komponen": "Qw*", "Nilai": audit_qw_mean},
                {"Kelompok": "Audit (Rata-rata 4 atribut)", "Komponen": "Qb*", "Nilai": audit_qb_mean},
                {"Kelompok": "BPS (Kategori IKR)", "Komponen": "Qw*", "Nilai": q_w_star},
                {"Kelompok": "BPS (Kategori IKR)", "Komponen": "Qb*", "Nilai": q_b_star},
            ]
        )
        fig_wb_cmp = px.bar(
            wb_compare_df,
            x="Kelompok",
            y="Nilai",
            color="Komponen",
            barmode="group",
            color_discrete_sequence=["#1d4ed8", "#93c5fd"],
            title="Perbandingan Within-Between: Audit vs BPS",
        )
        fig_wb_cmp.add_hline(y=0.0, line_dash="dash", line_color="#475569")
        st.plotly_chart(fig_wb_cmp, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        st.dataframe(wb_compare_df, use_container_width=True)
        fig_quad = go.Figure()
        fig_quad.add_shape(type="line", x0=-1, x1=1, y0=0, y1=0, line=dict(color="#64748b", dash="dash"))
        fig_quad.add_shape(type="line", x0=0, x1=0, y0=-1, y1=1, line=dict(color="#64748b", dash="dash"))
        fig_quad.add_annotation(x=0.55, y=0.75, text="Within kuat<br>Between kuat", showarrow=False, font=dict(size=11, color="#1e3a8a"))
        fig_quad.add_annotation(x=-0.55, y=0.75, text="Within kuat<br>Between lemah", showarrow=False, font=dict(size=11, color="#1e3a8a"))
        fig_quad.add_annotation(x=0.55, y=-0.75, text="Within lemah<br>Between kuat", showarrow=False, font=dict(size=11, color="#1e3a8a"))
        fig_quad.add_annotation(x=-0.55, y=-0.75, text="Within lemah<br>Between lemah", showarrow=False, font=dict(size=11, color="#1e3a8a"))
        fig_quad.add_trace(
            go.Scatter(
                x=[q_b_star],
                y=[q_w_star],
                mode="markers+text",
                marker=dict(size=14, color="#1d4ed8"),
                text=[f"(Qb*={q_b_star:.3f}, Qw*={q_w_star:.3f})"],
                textposition="top center",
                name="Posisi Hasil",
            )
        )
        fig_quad.update_layout(
            title="Peta Interpretasi Within-Between (sumbu X=Qb*, Y=Qw*)",
            xaxis_title="Qb* (Between)",
            yaxis_title="Qw* (Within)",
            xaxis=dict(range=[-1, 1]),
            yaxis=dict(range=[-1, 1]),
            height=430,
            template="plotly_white",
        )
        st.plotly_chart(fig_quad, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        if q_w_star >= 0.30 and q_b_star >= 0.30:
            montes_note = "Homogenitas kuat baik intra maupun antar-klaster."
        elif q_w_star >= 0.30 and q_b_star < 0.10:
            montes_note = "Homogenitas kuat di dalam klaster, melemah antar-klaster."
        elif q_w_star < 0.10 and q_b_star >= 0.30:
            montes_note = "Dalam klaster campuran, tetapi antar-klaster cenderung mirip."
        else:
            montes_note = "Pola within-between cenderung campuran/netral."
        st.markdown(
            f"<div class='soft-card'><b>Interpretasi Cepat Montes:</b><br>"
            f"Qw* menunjukkan homogenitas kategori dalam klaster; Qb* menunjukkan homogenitas kategori antar-klaster.<br>"
            f"Hasil saat ini: <b>{montes_note}</b></div>",
            unsafe_allow_html=True,
        )

    with tab_ringkas:
        top_num = df_num.iloc[df_num["r"].abs().idxmax()]
        top_attr = df_attr.iloc[df_attr["r"].abs().idxmax()]
        st.markdown(
            f"<div class='soft-card'><b>Ringkasan Otomatis Assortativity:</b><br>"
            f"Numeric paling dominan: <b>{top_num['Metrik']}</b> dengan r=<b>{float(top_num['r']):.4f}</b> "
            f"({top_num['Arah']} | {top_num['Kekuatan']}).<br><br>"
            f"Attribute paling dominan: <b>{top_attr['Metrik']}</b> dengan r=<b>{float(top_attr['r']):.4f}</b> "
            f"({top_attr['Arah']} | {top_attr['Kekuatan']}).<br><br>"
            f"Within-Between BPS: Qw*=<b>{q_w_star:.4f}</b>, Qb*=<b>{q_b_star:.4f}</b>."
            f"</div>",
            unsafe_allow_html=True,
        )
        st.markdown("#### Checklist Membaca Hasil (Praktis)")
        st.markdown(
            "1. Lihat tanda `r`: positif (homogen), negatif (heterogen), sekitar nol (campuran)."
        )
        st.markdown(
            "2. Lihat besar `|r|`: semakin besar semakin kuat pola pemilahannya."
        )
        st.markdown(
            "3. Bandingkan `Qw*` vs `Qb*`: apakah pemilahan dominan di dalam klaster atau juga lintas klaster."
        )
        st.dataframe(df_num, use_container_width=True)
        st.dataframe(df_attr, use_container_width=True)


def render_centrality_methods_page(
    n_nodes=80,
    threshold=0.30,
    seed=42,
):
    st.markdown("<h1 class='main-header'>Halaman Metode Centrality (Simulasi Pseudo)</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div class='premium-hero'><b>Fokus Halaman:</b> Menjelaskan logika centrality pada graf hasil Louvain: "
        "<b>Degree, Betweenness, Closeness, dan Eigenvector</b>.</div>",
        unsafe_allow_html=True,
    )

    tab_alur, tab_rumus, tab_sim, tab_out = st.tabs(
        ["Alur Centrality", "Rumus Matematis", "Simulasi Graf Pseudo", "Output & Interpretasi"]
    )

    with tab_alur:
        flow_df = pd.DataFrame(
            [
                {"Tahap": "Input Graf Louvain", "Deskripsi": "Gunakan graf berbobot hasil proses pembobotan dan Louvain."},
                {"Tahap": "Pilih Metrik", "Deskripsi": "Pilih Degree / Betweenness / Closeness / Eigenvector."},
                {"Tahap": "Hitung Skor Node", "Deskripsi": "Setiap node mendapat nilai centrality sesuai metrik terpilih."},
                {"Tahap": "Peringkat Node", "Deskripsi": "Urutkan node dari nilai centrality tertinggi ke terendah."},
                {"Tahap": "Analisis Segmentasi", "Deskripsi": "Bandingkan top node per klaster Louvain dan per dusun."},
                {"Tahap": "Output", "Deskripsi": "Graf visual (ukuran/warna node) + tabel top node untuk keputusan audit."},
            ]
        )
        fig_flow = px.funnel(
            flow_df,
            y="Tahap",
            x=[1] * len(flow_df),
            title="Tahapan Analisis Centrality di Graf Hasil Louvain",
        )
        fig_flow.update_traces(
            marker_color="#1d4ed8",
            text=flow_df["Deskripsi"],
            textposition="inside",
            texttemplate="<b>%{y}</b><br>%{text}",
            insidetextfont=dict(size=12, color="#ffffff"),
        )
        fig_flow.update_layout(height=620, template="plotly_white", xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_flow, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
        st.dataframe(flow_df, use_container_width=True)

    with tab_rumus:
        st.markdown("#### 1) Degree Centrality (weighted degree)")
        st.latex(r"C_D(i) = \sum_{j=1}^{N} A_{ij}")
        st.caption("Makna praktis: node dengan koneksi langsung paling banyak/kuat akan bernilai tinggi.")

        st.markdown("#### 2) Betweenness Centrality")
        st.latex(r"C_B(i) = \sum_{j\neq k \in V}\frac{\sigma_{jk}(i)}{\sigma_{jk}}")
        st.caption("Makna praktis: node penghubung antarbagian graf (broker/jembatan) akan bernilai tinggi.")

        st.markdown("#### 3) Closeness Centrality")
        st.latex(r"C_C(i)=\frac{1}{\sum_{j\in V}\mathrm{dist}(i,j)}")
        st.caption("Makna praktis: node yang rata-rata jaraknya paling dekat ke node lain akan bernilai tinggi.")

        st.markdown("#### 4) Eigenvector Centrality")
        st.latex(r"\lambda_{\max}\mathbf{E}=A\mathbf{E}, \quad C_E(i)=\frac{E_i}{\|\mathbf{E}\|}")
        st.caption("Makna praktis: node penting jika terhubung ke node-node penting lainnya.")

    rng = np.random.default_rng(int(seed))
    n_nodes = int(max(30, n_nodes))
    cluster_count = 3
    node_ids = [f"CT_{i+1:03d}" for i in range(n_nodes)]
    G = nx.Graph()
    rows = []
    for i, nid in enumerate(node_ids):
        cid = int(i % cluster_count)
        base = 60 + (cid * 8)
        f_a = float(np.clip(rng.normal(base + 1.0, 2.0), 40, 98))
        f_b = float(np.clip(rng.normal(base + 0.6, 2.2), 40, 98))
        f_c = float(np.clip(rng.normal(base - 0.4, 2.3), 40, 98))
        f_d = float(np.clip(rng.normal(base + 0.9, 2.1), 40, 98))
        f_e = float(np.clip(rng.normal(base + 0.2, 2.4), 40, 98))
        f_ikr = float(np.mean([f_a, f_b, f_c, f_d, f_e]))
        dusun = f"Dusun-{cid+1}"
        rows.append(
            {
                "family_id": nid,
                "nama": f"Keluarga {i+1}",
                "f_a_dari_rekap_kk": f_a,
                "f_b_dari_rekap_kk": f_b,
                "f_c_dari_rekap_kk": f_c,
                "f_d_dari_rekap_kk": f_d,
                "f_e_dari_rekap_kk": f_e,
                "f_ikr_dari_rekap_kk": f_ikr,
                "dusun": dusun,
                "cluster": cid,
            }
        )
    pseudo_df = pd.DataFrame(rows)
    for _, r in pseudo_df.iterrows():
        G.add_node(r["family_id"], **r.to_dict())
    for i in range(len(node_ids)):
        for j in range(i + 1, len(node_ids)):
            ui, uj = node_ids[i], node_ids[j]
            ai, aj = G.nodes[ui], G.nodes[uj]
            vi = np.array(
                [
                    ai["f_a_dari_rekap_kk"],
                    ai["f_b_dari_rekap_kk"],
                    ai["f_c_dari_rekap_kk"],
                    ai["f_d_dari_rekap_kk"],
                    ai["f_e_dari_rekap_kk"],
                ],
                dtype=float,
            )
            vj = np.array(
                [
                    aj["f_a_dari_rekap_kk"],
                    aj["f_b_dari_rekap_kk"],
                    aj["f_c_dari_rekap_kk"],
                    aj["f_d_dari_rekap_kk"],
                    aj["f_e_dari_rekap_kk"],
                ],
                dtype=float,
            )
            sim = float(compute_cosine_similarity(vi, vj))
            if sim >= float(threshold):
                G.add_edge(ui, uj, weight=sim)
    if G.number_of_edges() == 0:
        for i in range(len(node_ids)):
            for j in range(i + 1, len(node_ids)):
                ui, uj = node_ids[i], node_ids[j]
                ai, aj = G.nodes[ui], G.nodes[uj]
                vi = np.array(
                    [
                        ai["f_a_dari_rekap_kk"],
                        ai["f_b_dari_rekap_kk"],
                        ai["f_c_dari_rekap_kk"],
                        ai["f_d_dari_rekap_kk"],
                        ai["f_e_dari_rekap_kk"],
                    ],
                    dtype=float,
                )
                vj = np.array(
                    [
                        aj["f_a_dari_rekap_kk"],
                        aj["f_b_dari_rekap_kk"],
                        aj["f_c_dari_rekap_kk"],
                        aj["f_d_dari_rekap_kk"],
                        aj["f_e_dari_rekap_kk"],
                    ],
                    dtype=float,
                )
                sim = float(compute_cosine_similarity(vi, vj))
                if sim >= 0.20:
                    G.add_edge(ui, uj, weight=sim)

    if G.number_of_edges() > 0:
        partition = community_louvain.best_partition(G, weight="weight", random_state=int(seed))
        nx.set_node_attributes(G, partition, "cluster")
    else:
        partition = {n: 0 for n in G.nodes()}
        nx.set_node_attributes(G, partition, "cluster")

    deg_vals = compute_centrality_on_similarity_graph(G, "degree")
    bet_vals = compute_centrality_on_similarity_graph(G, "betweenness")
    clo_vals = compute_centrality_on_similarity_graph(G, "closeness")
    eig_vals = compute_centrality_on_similarity_graph(G, "eigenvector")

    with tab_sim:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Node", f"{int(G.number_of_nodes())}")
        c2.metric("Edge", f"{int(G.number_of_edges())}")
        c3.metric("Threshold Similarity", f"{float(threshold):.2f}")
        c4.metric("Jumlah Klaster Louvain", f"{int(len(set(partition.values())))}")

        pos = nx.spring_layout(G, seed=int(seed), weight="weight")
        nodes = list(G.nodes())
        size_vals = [9.0 + (22.0 * float(deg_vals.get(n, 0.0)) / max(max(deg_vals.values()) if deg_vals else 1.0, 1e-9)) for n in nodes]
        fig_graph = go.Figure()
        for u, v, d in G.edges(data=True):
            cu = int(partition.get(u, 0))
            cv = int(partition.get(v, 0))
            edge_weight = _safe_float_metric(d.get("weight"), 0.0)
            edge_color = (
                rgba_from_hex(CONTRAST_COLORS[cu % len(CONTRAST_COLORS)], 0.44)
                if cu == cv
                else rgba_from_hex(CONTRAST_COLORS[((cu + 1) * 7 + (cv + 1) * 13) % len(CONTRAST_COLORS)], 0.32)
            )
            fig_graph.add_trace(
                go.Scatter(
                    x=[pos[u][0], pos[v][0], None],
                    y=[pos[u][1], pos[v][1], None],
                    mode="lines",
                    line=dict(width=1.0 + (1.6 * edge_weight), color=edge_color),
                    hoverinfo="none",
                    showlegend=False,
                )
            )
        fig_graph.add_trace(
            go.Scatter(
                x=[pos[n][0] for n in nodes],
                y=[pos[n][1] for n in nodes],
                mode="markers",
                marker=dict(
                    size=size_vals,
                    color=[float(eig_vals.get(n, 0.0)) for n in nodes],
                                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="Eigenvector"),
                    line=dict(color=NETWORK_NODE_LINE, width=0.7),
                ),
                text=[
                    f"Node: {n}<br>Klaster: {int(partition.get(n, 0))}"
                    f"<br>Degree: {float(deg_vals.get(n, 0.0)):.4f}"
                    f"<br>Betweenness: {float(bet_vals.get(n, 0.0)):.4f}"
                    f"<br>Closeness: {float(clo_vals.get(n, 0.0)):.4f}"
                    f"<br>Eigenvector: {float(eig_vals.get(n, 0.0)):.4f}"
                    for n in nodes
                ],
                hoverinfo="text",
                showlegend=False,
            )
        )
        fig_graph.update_layout(
            title="Graf Pseudo Centrality (ukuran = Degree, warna = Eigenvector)",
            height=560,
            template="plotly_white",
            margin=dict(l=20, r=20, t=60, b=20),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
        )
        st.plotly_chart(fig_graph, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

    with tab_out:
        df_cent = pd.DataFrame(
            [
                {
                    "family_id": n,
                    "Klaster Louvain": int(partition.get(n, 0)),
                    "Dusun": G.nodes[n].get("dusun", "-"),
                    "Degree": float(deg_vals.get(n, 0.0)),
                    "Betweenness": float(bet_vals.get(n, 0.0)),
                    "Closeness": float(clo_vals.get(n, 0.0)),
                    "Eigenvector": float(eig_vals.get(n, 0.0)),
                }
                for n in G.nodes()
            ]
        ).sort_values("Degree", ascending=False).reset_index(drop=True)
        st.markdown("#### Top 10 Node per Metrik")
        m1, m2 = st.columns(2)
        with m1:
            st.caption("Top 10 Degree")
            st.dataframe(df_cent.sort_values("Degree", ascending=False).head(10), use_container_width=True)
            st.caption("Top 10 Betweenness")
            st.dataframe(df_cent.sort_values("Betweenness", ascending=False).head(10), use_container_width=True)
        with m2:
            st.caption("Top 10 Closeness")
            st.dataframe(df_cent.sort_values("Closeness", ascending=False).head(10), use_container_width=True)
            st.caption("Top 10 Eigenvector")
            st.dataframe(df_cent.sort_values("Eigenvector", ascending=False).head(10), use_container_width=True)

        st.markdown("#### Ringkasan per Klaster")
        df_cluster = (
            df_cent.groupby("Klaster Louvain", as_index=False)
            .agg(
                Jumlah_Node=("family_id", "count"),
                Rerata_Degree=("Degree", "mean"),
                Rerata_Betweenness=("Betweenness", "mean"),
                Rerata_Closeness=("Closeness", "mean"),
                Rerata_Eigenvector=("Eigenvector", "mean"),
            )
            .sort_values("Klaster Louvain")
            .reset_index(drop=True)
        )
        st.dataframe(df_cluster, use_container_width=True)
        st.caption(
            "Cara baca sederhana: Degree tinggi = pusat interaksi lokal; Betweenness tinggi = node jembatan; "
            "Closeness tinggi = cepat menjangkau node lain; Eigenvector tinggi = penting karena terhubung ke node penting."
        )


# =========================================================
# 3. SIDEBAR NAVIGATION
# =========================================================
with st.sidebar:
    logo_col, title_col = st.columns([1, 3], gap="small")
    with logo_col:
        logo_data_uri = get_logo_data_uri(LOGO_PATH)
        logo_inner_html = (
            f"<img src='{logo_data_uri}' class='sidebar-logo-img' alt='Logo SNA' />"
            if logo_data_uri
            else "<div class='sidebar-logo-fallback'>SNA</div>"
        )
        st.markdown(
            f"<div class='sidebar-logo-shell'><div class='sidebar-logo-disc'>{logo_inner_html}</div></div>",
            unsafe_allow_html=True,
        )
    with title_col:
        st.markdown(
            "<div style='padding-top:8px; font-size:1.05rem; font-weight:700; color:#E5E7EB;'>SNA Data Desa Presisi</div>",
            unsafe_allow_html=True,
        )
page_mode = st.sidebar.radio(
    "Pilih Halaman",
    [
        "Dashboard Audit",
        "Rangkuman Threshold Otomatis",
        "Analisis Centrality",
        "Analisis Bansos Spasial",
        "Metode Pembobotan",
        "Metode Louvain",
        "Metode Assortativity",
        "Metode Centrality",
    ],
    index=0,
)
st.sidebar.caption(f"Mode aktif: {page_mode}")
uploaded_file = st.sidebar.file_uploader("Unggah Database", type=['csv', 'xlsx'])
render_global_header()

if page_mode == "Metode Louvain" and not uploaded_file:
    render_louvain_methods_page(n_nodes=60, rounding_decimals=2, threshold=0.30, seed=42)
    st.stop()
if page_mode == "Analisis Bansos Spasial" and not uploaded_file:
    st.markdown("<h1 class='main-header'>Analisis Bansos Spasial</h1>", unsafe_allow_html=True)
    st.info("Unggah database desa terlebih dahulu untuk menampilkan peta ArcGIS analisis bansos.")
    st.stop()
if page_mode == "Rangkuman Threshold Otomatis" and not uploaded_file:
    st.markdown("<h1 class='main-header'>Rangkuman Threshold Otomatis</h1>", unsafe_allow_html=True)
    st.info("Unggah database desa terlebih dahulu untuk menjalankan sensitivity analysis threshold otomatis.")
    st.stop()
if page_mode == "Analisis Centrality" and not uploaded_file:
    st.markdown("<h1 class='main-header'>Analisis Centrality</h1>", unsafe_allow_html=True)
    st.info("Unggah database desa terlebih dahulu untuk menampilkan analisis centrality berbasis jaringan Louvain.")
    st.stop()
if page_mode == "Metode Pembobotan" and not uploaded_file:
    render_weighting_methods_page(
        df_v=pd.DataFrame(),
        edge_feature_cols=EDGE_REKAP_COLS,
        rounding_decimals=2,
        threshold_grid=[round(x, 1) for x in np.arange(0.1, 1.0, 0.1)],
        sample_max_nodes=120,
    )
    st.stop()
if page_mode == "Metode Assortativity" and not uploaded_file:
    render_assortativity_methods_page(n_nodes=90, seed=42)
    st.stop()
if page_mode == "Metode Centrality" and not uploaded_file:
    render_centrality_methods_page(n_nodes=80, threshold=0.30, seed=42)
    st.stop()

if uploaded_file:
    df_kk = load_and_clean_ddp(uploaded_file)
    if df_kk is None or df_kk.empty:
        st.stop()
    col_desa = 'deskel' if 'deskel' in df_kk.columns else 'desa'
    col_spasial = 'dusun' if 'dusun' in df_kk.columns else 'rt'
    show_map_edges = True
    selected_centrality_key = "none"
    graph_spatial_mode = "Layout Jaringan"

    with st.sidebar:
        selected_desa = st.selectbox("Pilih Desa", sorted(df_kk[col_desa].unique()))
        basis_candidates = [
            ("IKR Rekap KK", "f_ikr_dari_rekap_kk"),
            ("IPM Mikro", "ipm_mikro"),
            ("Ekonomi", "indeks_pengeluaran"),
            ("Kesehatan", "indeks_kesehatan"),
            ("Pendidikan", "indeks_pendidikan"),
        ]
        available_basis = []
        for label, col in basis_candidates:
            if col in df_kk.columns:
                available_basis.append((label, col))
        if not available_basis:
            numeric_cols = []
            for c in df_kk.columns:
                s = pd.to_numeric(df_kk[c], errors="coerce")
                if s.notna().sum() >= max(3, int(0.2 * len(df_kk))):
                    numeric_cols.append(c)
            available_basis = [(f"Kolom Numerik: {c}", c) for c in numeric_cols[:10]]
        if not available_basis:
            st.error("Tidak ada kolom numerik yang bisa dijadikan basis jaringan.")
            st.stop()
        onehot_round_decimals = st.selectbox(
            "Pembulatan One-Hot",
            options=[0, 2, 1],
            format_func=lambda d: "Bilangan bulat (tanpa koma)" if d == 0 else f"{d} angka di belakang koma",
            index=0,
        )
        threshold_grid = [round(x, 1) for x in np.arange(0.1, 1.0, 0.1)]

        if page_mode in {"Dashboard Audit", "Rangkuman Threshold Otomatis", "Analisis Centrality"}:
            basis_col = st.selectbox("Basis Jaringan", available_basis, format_func=lambda x: x[0])[1]
            weighting_mode = st.selectbox(
                "Metode Pembobotan Graf",
                options=[
                    ("Cosine Similarity", "cosine"),
                    ("Jaccard Index", "jaccard"),
                    ("Pearson Correlation", "pearson"),
                ],
                format_func=lambda x: x[0],
            )[1]
            if page_mode in {"Rangkuman Threshold Otomatis", "Analisis Centrality"}:
                auto_threshold_mode = True
                threshold_val = 0.40
                if page_mode == "Rangkuman Threshold Otomatis":
                    st.caption("Halaman ini menjalankan threshold otomatis dan membandingkan seluruh kandidat ambang.")
                else:
                    st.caption("Halaman ini memakai graf Louvain yang sama, lalu fokus pada centrality dan interpretasi kebijakan.")
            else:
                threshold_mode = st.radio("Mode Threshold", ["Otomatis (Distribusi)", "Manual"], index=0)
                auto_threshold_mode = threshold_mode.startswith("Otomatis")
                if auto_threshold_mode:
                    threshold_val = 0.40
                else:
                    threshold_val = st.slider("Threshold Manual", 0.1, 0.9, 0.4, 0.1)
                    st.caption("Threshold manual aktif: edge dibentuk jika similarity >= threshold.")
            comp_mode = st.radio("Mode Komponen", ["LCC only", "Semua komponen"], index=0, help="LCC only menganalisis komponen terbesar saja.")
            lcc_only = comp_mode == "LCC only"
            if page_mode == "Analisis Centrality":
                layout_spread = st.slider(
                    "Sebaran Layout Jaringan",
                    min_value=1.0,
                    max_value=3.4,
                    value=2.2,
                    step=0.1,
                    help="Semakin besar nilai ini, jarak visual antarklaster makin renggang.",
                )
                graph_spatial_mode = "Layout Jaringan"
                selected_centrality_key = st.selectbox(
                    "Metrik Centrality",
                    options=[
                        ("Degree Centrality", "degree"),
                        ("Betweenness Centrality", "betweenness"),
                        ("Closeness Centrality", "closeness"),
                        ("Eigenvector Centrality", "eigenvector"),
                    ],
                    format_func=lambda x: x[0],
                    index=0,
                )[1]
            elif page_mode == "Dashboard Audit":
                layout_spread = st.slider(
                    "Sebaran Layout Jaringan",
                    min_value=1.0,
                    max_value=3.4,
                    value=2.2,
                    step=0.1,
                    help="Semakin besar nilai ini, jarak visual antarklaster makin renggang.",
                )
                selected_dim_key = st.selectbox(
                    "Drill-Down Dimensi",
                    options=list(DRILLDOWN_DIMENSIONS.keys()),
                    format_func=lambda k: DRILLDOWN_DIMENSIONS[k]["label"],
                )
                selected_graph_dim = st.selectbox(
                    "Visual Jaringan Dimensi Kesejahteraan",
                    options=[IKR_OVERALL_METRIC] + list(IKR_DIMENSION_MAP),
                    format_func=lambda x: f"{x[0]} ({x[1]})",
                )
                graph_spatial_mode = st.selectbox(
                    "Mode Visualisasi Jaringan",
                    options=["Layout Jaringan", "Spasial OSM", "Spasial ArcGIS"],
                    index=0,
                    help="Jika memilih mode spasial, node ditampilkan di peta berdasarkan lat/lon tanpa edge.",
                )
                selected_centrality_key = "none"
                st.caption("Analisis centrality dipisahkan ke halaman khusus: Analisis Centrality.")
        elif page_mode == "Analisis Bansos Spasial":
            basis_col = st.selectbox("Basis Jaringan", available_basis, format_func=lambda x: x[0])[1]
            weighting_mode = st.selectbox(
                "Metode Pembobotan Graf",
                options=[
                    ("Cosine Similarity", "cosine"),
                    ("Jaccard Index", "jaccard"),
                    ("Pearson Correlation", "pearson"),
                ],
                format_func=lambda x: x[0],
                index=0,
            )[1]
            threshold_mode = st.radio("Mode Threshold", ["Otomatis (Distribusi)", "Manual"], index=0)
            auto_threshold_mode = threshold_mode.startswith("Otomatis")
            if auto_threshold_mode:
                threshold_val = 0.40
            else:
                threshold_val = st.slider("Threshold Manual", 0.1, 0.9, 0.4, 0.1)
            comp_mode = st.radio("Mode Komponen", ["LCC only", "Semua komponen"], index=0)
            lcc_only = comp_mode == "LCC only"
            graph_spatial_mode = st.selectbox(
                "Basemap Spasial",
                options=["Spasial ArcGIS", "Spasial OSM"],
                index=0,
            )
            selected_bansos_dimension = st.selectbox(
                "Dimensi Analisis Bansos",
                options=[col for _, col in IKR_DIMENSION_MAP],
                format_func=lambda c: next((label for label, col in IKR_DIMENSION_MAP if col == c), c),
            )
            bansos_map_color_mode = st.selectbox(
                "Warna Node Peta",
                options=["IKR Agregat", "Status Bansos (YA/TIDAK)", "Status BPS-Bansos"],
                index=0,
            )
            bansos_filter_mode = st.selectbox(
                "Filter Node di Peta",
                options=[
                    "Semua KK",
                    "Penerima Bansos",
                    "Rendah - Penerima",
                    "Rendah - Belum Menerima",
                    "Sedang - Penerima",
                    "Sedang - Belum Menerima",
                    "Tinggi - Penerima",
                    "Tinggi - Belum Menerima",
                    "Sangat Tinggi - Penerima",
                    "Sangat Tinggi - Belum Menerima",
                    "Rentan Dimensi Terpilih",
                    "Penerima pada Dimensi Terpilih",
                ],
                index=0,
            )
            st.markdown("**Ambang Skor Tiap Dimensi IKR**")
            bansos_dim_thresholds = {}
            for dim_label, dim_col in IKR_DIMENSION_MAP:
                slider_label = dim_label.split("(", 1)[0].strip()
                bansos_dim_thresholds[dim_col] = st.slider(
                    f"{slider_label}",
                    min_value=0.0,
                    max_value=100.0,
                    value=60.0,
                    step=1.0,
                    help=f"KK dengan skor {slider_label} <= nilai ini ditandai rentan.",
                )
        elif page_mode == "Metode Pembobotan":
            sample_max_nodes = st.slider(
                "Maks Node Simulasi",
                min_value=30,
                max_value=250,
                value=120,
                step=10,
                help="Batas node untuk perbandingan distribusi similarity agar performa tetap ringan.",
            )
        elif page_mode == "Metode Louvain":
            louvain_n_nodes = st.slider(
                "Jumlah Node Pseudo Louvain",
                min_value=30,
                max_value=220,
                value=80,
                step=10,
            )
            louvain_threshold = st.slider(
                "Threshold Graf Base (Pseudo)",
                min_value=0.10,
                max_value=0.90,
                value=0.30,
                step=0.05,
            )
            louvain_seed = st.number_input("Random Seed Louvain", min_value=1, max_value=9999, value=42, step=1)
        elif page_mode == "Metode Assortativity":
            assort_n_nodes = st.slider(
                "Jumlah Node Pseudo Assortativity",
                min_value=40,
                max_value=260,
                value=90,
                step=10,
            )
            assort_seed = st.number_input("Random Seed Assortativity", min_value=1, max_value=9999, value=42, step=1)
        else:
            centrality_n_nodes = st.slider(
                "Jumlah Node Pseudo Centrality",
                min_value=40,
                max_value=260,
                value=80,
                step=10,
            )
            centrality_threshold = st.slider(
                "Threshold Graf Pseudo (Centrality)",
                min_value=0.10,
                max_value=0.90,
                value=0.30,
                step=0.05,
            )
            centrality_seed = st.number_input("Random Seed Centrality", min_value=1, max_value=9999, value=42, step=1)

    # --- PROCESS ---
    if page_mode == "Metode Louvain":
        render_louvain_methods_page(
            n_nodes=louvain_n_nodes,
            rounding_decimals=onehot_round_decimals,
            threshold=louvain_threshold,
            seed=louvain_seed,
        )
        st.stop()
    if page_mode == "Metode Assortativity":
        render_assortativity_methods_page(
            n_nodes=assort_n_nodes,
            seed=assort_seed,
        )
        st.stop()
    if page_mode == "Metode Centrality":
        render_centrality_methods_page(
            n_nodes=centrality_n_nodes,
            threshold=centrality_threshold,
            seed=centrality_seed,
        )
        st.stop()

    df_v = df_kk[df_kk[col_desa] == selected_desa].copy()
    df_v = add_bps_ikr_category(df_v, ikr_col="f_ikr_dari_rekap_kk")

    if page_mode == "Metode Pembobotan":
        render_weighting_methods_page(
            df_v=df_v,
            edge_feature_cols=EDGE_REKAP_COLS,
            rounding_decimals=onehot_round_decimals,
            threshold_grid=threshold_grid,
            sample_max_nodes=sample_max_nodes,
        )
        st.stop()

    res = build_sna_network(
        df_v,
        basis_col,
        threshold_val,
        auto_threshold=auto_threshold_mode,
        lcc_only=lcc_only,
        similarity_method=weighting_mode,
        force_louvain_lcc=lcc_only,
        threshold_grid=threshold_grid,
        edge_feature_cols=EDGE_REKAP_COLS,
        onehot_round_decimals=onehot_round_decimals,
    )

    if res:
        G, partition, cluster_list, meta = res
        if page_mode == "Analisis Bansos Spasial":
            render_bansos_spatial_analysis_page(
                df_v=df_v,
                graph_obj=G,
                partition=partition,
                spatial_mode=graph_spatial_mode,
                selected_dimension_col=selected_bansos_dimension,
                map_color_mode=bansos_map_color_mode,
                filter_mode=bansos_filter_mode,
                dim_thresholds=bansos_dim_thresholds,
            )
            st.stop()
        method_used = meta.get("similarity_method")
        threshold_used = float(meta.get("threshold_selected", threshold_val))
        if method_used == "cosine":
            method_label = "Cosine Similarity"
            kernel_info = "Vektor one-hot dari lima dimensi kesejahteraan"
        elif method_used == "jaccard":
            method_label = "Jaccard Index"
            kernel_info = "Irisan/union fitur aktif dari vektor one-hot lima dimensi kesejahteraan"
        elif method_used == "pearson":
            method_label = "Pearson Correlation"
            kernel_info = "Korelasi antar vektor one-hot lima dimensi kesejahteraan"
        else:
            method_label = str(method_used).upper() if method_used else "-"
            kernel_info = "Metode custom"
        rounding_label = (
            "Bilangan bulat"
            if int(meta.get("onehot_round_decimals", 2)) == 0
            else f"{int(meta.get('onehot_round_decimals', 2))} desimal"
        )
        if page_mode == "Rangkuman Threshold Otomatis":
            st.markdown(f"<h1 class='main-header'>Rangkuman Threshold Otomatis: {selected_desa}</h1>", unsafe_allow_html=True)
            render_auto_threshold_summary(
                meta=meta,
                graph_obj=G,
                partition=partition,
                selected_desa=selected_desa,
                basis_col=basis_col,
                method_label=method_label,
                kernel_info=kernel_info,
                rounding_label=rounding_label,
                compact=False,
            )
            st.stop()
        if page_mode == "Analisis Centrality":
            render_centrality_analysis_page(
                graph_obj=G,
                partition=partition,
                df_v=df_v,
                selected_desa=selected_desa,
                selected_centrality_key=selected_centrality_key,
                col_spasial=col_spasial,
                layout_spread=layout_spread,
            )
            st.stop()

        st.markdown(f"<h1 class='main-header'>Dashboard Master SNA Audit: {selected_desa}</h1>", unsafe_allow_html=True)
        st.markdown(
            f"<div class='premium-hero'><b>Ringkasan Konfigurasi</b><br>"
            f"Basis: <b>{basis_col}</b> | Threshold Kemiripan Terpilih: <b>{threshold_used:.0%} ({threshold_used:.2f})</b> | "
            f"Metode: <b>{method_label}</b> ({kernel_info}) | One-Hot Rounding: <b>{rounding_label}</b> | Komponen: <b>{meta['mode']}</b><br>"
            f"Node dianalisis: <b>{G.number_of_nodes()}</b> (Raw {meta['raw_nodes']}, LCC {meta['lcc_nodes']})"
            f"</div>",
            unsafe_allow_html=True,
        )
        if meta.get("threshold_distribution"):
            with subbab_dropdown("Audit Distribusi Similarity dan Threshold Otomatis", expanded=False):
                c_auto_1, c_auto_2 = st.columns([1.2, 1.0])
                with c_auto_1:
                    sim_vals = meta.get("pairwise_similarity_values", [])
                    if sim_vals:
                        fig_dist = px.histogram(
                            x=sim_vals,
                            nbins=20,
                            title="Distribusi Nilai Similarity Antar-Pasangan Node",
                            labels={"x": "Similarity"},
                        )
                        fig_dist.update_layout(
                            xaxis_title="Similarity",
                            yaxis_title="Frekuensi",
                            template="plotly_white",
                        )
                        fig_dist.add_vline(
                            x=threshold_used,
                            line_width=2,
                            line_dash="dash",
                            line_color="#B91C1C",
                            annotation_text=f"Threshold terpilih {threshold_used:.2f}",
                        )
                        st.plotly_chart(fig_dist, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                with c_auto_2:
                    df_thr = pd.DataFrame(meta["threshold_distribution"]).sort_values("threshold").reset_index(drop=True)
                    if not df_thr.empty:
                        total_edge_kumulatif = int(df_thr["edge_count"].sum())
                        jumlah_kandidat = int(len(df_thr))
                        rata2_edge_umum = float(total_edge_kumulatif / max(jumlah_kandidat, 1))
                        pair_total = int(len(meta.get("pairwise_similarity_values", [])))
                        s1, s2, s3, s4 = st.columns(4)
                        s1.metric("Total Pair Kandidat", pair_total)
                        s2.metric("Total Edge Kumulatif", total_edge_kumulatif)
                        s3.metric("Rata-rata Umum (Total/9)", f"{rata2_edge_umum:.2f}")
                        s4.metric("Jumlah Parameter", jumlah_kandidat)

                        fig_thr_cmp = px.line(
                            df_thr,
                            x="threshold",
                            y="edge_count",
                            markers=True,
                            title="Perbandingan Semua Parameter Threshold vs Edge",
                        )
                        fig_thr_cmp.add_hline(
                            y=rata2_edge_umum,
                            line_dash="dash",
                            line_color="#B91C1C",
                            annotation_text=f"Rata-rata umum = {rata2_edge_umum:.2f}",
                        )
                        st.plotly_chart(fig_thr_cmp, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                    thr_selected = round(float(threshold_used), 1)
                    if "threshold" in df_thr.columns:
                        df_thr["threshold"] = df_thr["threshold"].round(1)
                    def _highlight_selected_threshold(row):
                        if float(row.get("threshold", -999)) == thr_selected:
                            return ["background-color: #22c55e; color: #052e16; font-weight: 700;"] * len(row)
                        return [""] * len(row)
                    st.dataframe(
                        df_thr.style.apply(_highlight_selected_threshold, axis=1),
                        use_container_width=True,
                    )
                df_sens_dashboard = get_threshold_sensitivity_dataframe(meta)
                if not df_sens_dashboard.empty:
                    render_threshold_sensitivity_heatmap(df_sens_dashboard)
        with subbab_dropdown("Alur Jaringan: Pembentukan Base Graph, Louvain, dan Audit", expanded=True):
            c_base = st.columns(4)
            with c_base[0]:
                st.metric("Node", G.number_of_nodes())
            with c_base[1]:
                st.metric("Edge", G.number_of_edges())
            with c_base[2]:
                st.metric("Density", f"{nx.density(G):.4f}")
            with c_base[3]:
                st.metric("Komponen", nx.number_connected_components(G))

            modularity_focus = _safe_float_metric(community_louvain.modularity(partition, G, weight="weight"), default=0.0)
            c_louv = st.columns(3)
            with c_louv[0]:
                st.metric("Jumlah Klaster Louvain", len(set(partition.values())))
            with c_louv[1]:
                st.metric("Modularity Q", f"{modularity_focus:.4f}")
            with c_louv[2]:
                st.metric("Threshold Terpilih", f"{threshold_used:.2f}")

            n_nodes_layout = max(G.number_of_nodes(), 2)
            pos_focus = build_clustered_network_layout(
                G,
                partition=partition,
                layout_spread=layout_spread,
                seed=42,
            )
            edge_weights = [_safe_float_metric(d.get("weight"), default=0.0) for _, _, d in G.edges(data=True)]
            edge_min = float(min(edge_weights)) if edge_weights else 0.0
            edge_max = float(max(edge_weights)) if edge_weights else 1.0
            edge_span = max(edge_max - edge_min, 1e-9)
            visible_edge_limit = int(np.clip(n_nodes_layout * 1.45, 180, 950))
            visible_edges_focus = select_representative_edges(
                G,
                max_edges=visible_edge_limit,
                per_node=1,
            )
            node_size_main = network_marker_size(n_nodes_layout, base=9.0)
            node_line_width = 0.42 if n_nodes_layout >= 300 else 0.6
            cluster_ids_sorted = sorted(set(partition.values()))
            # Palet diskret yang ramah cetak dan tetap kontras untuk publikasi akademik.
            cluster_palette_base = [
                "#0072B2", "#D55E00", "#009E73", "#CC79A7", "#56B4E9", "#E69F00",
                "#332288", "#88CCEE", "#44AA99", "#117733", "#999933", "#882255",
                "#AA4499", "#DDCC77", "#CC6677", "#6699CC", "#661100", "#999999",
            ] + px.colors.qualitative.Dark24 + px.colors.qualitative.Alphabet
            cluster_palette = cluster_palette_base[:len(cluster_ids_sorted)]
            cid_to_idx = {cid: idx for idx, cid in enumerate(cluster_ids_sorted)}

            def build_discrete_colorscale(colors):
                if len(colors) <= 1:
                    c = colors[0] if colors else "#b91c1c"
                    return [[0.0, c], [1.0, c]]
                n = len(colors)
                cs = []
                for i, c in enumerate(colors):
                    start = i / n
                    end = (i + 1) / n
                    cs.append([start, c])
                    cs.append([end, c])
                return cs

            cluster_colorscale = build_discrete_colorscale(cluster_palette)
            node_ids = list(G.nodes())
            cluster_color_map = {cid: cluster_palette[cid_to_idx.get(cid, 0)] for cid in cluster_ids_sorted}
            edge_weight_palette = ["#B91C1C", "#D97706", "#0F766E", "#2563EB"]

            def edge_color_by_weight(_u, _v, _d=None, w_norm=0.0):
                color_idx = int(np.clip(np.floor(float(w_norm) * len(edge_weight_palette)), 0, len(edge_weight_palette) - 1))
                return rgba_from_hex(edge_weight_palette[color_idx], 0.36)

            def edge_color_by_interaction(u, v, _d=None, _w_norm=0.0):
                cu = partition.get(u, -1)
                cv = partition.get(v, -1)
                if cu == cv:
                    return rgba_from_hex(cluster_color_map.get(cu, "#64748b"), 0.46)
                iu, iv = sorted([cid_to_idx.get(cu, 0), cid_to_idx.get(cv, 0)])
                pair_idx = ((iu + 1) * 7 + (iv + 1) * 13) % len(CONTRAST_COLORS)
                return rgba_from_hex(CONTRAST_COLORS[pair_idx], 0.34)

            def node_meta(nid):
                n_attr = G.nodes[nid]
                nama = n_attr.get("nama", "-")
                usia = n_attr.get("usia", n_attr.get("usia (y)", "-"))
                profesi = n_attr.get("profesi pekerjaan", n_attr.get("profesi_pekerjaan", "-"))
                f_ikr = n_attr.get("f_ikr_dari_rekap_kk", "-")
                cluster_id = partition.get(nid, -1)
                return [str(nama), str(usia), str(profesi), str(f_ikr), int(cluster_id)]

            node_customdata = [node_meta(n) for n in node_ids]

            fig_base = go.Figure()
            add_network_edge_traces(
                fig_base,
                visible_edges_focus,
                pos_focus,
                edge_min,
                edge_span,
                color_fn=edge_color_by_weight,
                base_width=0.28,
                width_scale=0.82,
                hover=True,
            )
            fig_base.add_trace(
                go.Scatter(
                    x=[pos_focus[n][0] for n in node_ids],
                    y=[pos_focus[n][1] for n in node_ids],
                    mode="markers",
                    marker=dict(
                        size=node_size_main,
                        color="#0ea5e9",
                        opacity=0.86,
                        line=dict(color=NETWORK_NODE_LINE, width=node_line_width),
                    ),
                    customdata=node_customdata,
                    hovertemplate=(
                        "Nama: %{customdata[0]}<br>"
                        "Usia: %{customdata[1]}<br>"
                        "Profesi: %{customdata[2]}<br>"
                        "IKR Agregat: %{customdata[3]}<br>"
                        "Klaster: %{customdata[4]}<extra></extra>"
                    ),
                    name="Node KK",
                )
            )
            style_network_figure(
                fig_base,
                title="Jaringan Kemiripan Rumah Tangga Sebelum Deteksi Komunitas",
                height=650,
            )
            if graph_spatial_mode == "Layout Jaringan":
                st.plotly_chart(fig_base, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
            else:
                base_hover = [
                    (
                        f"Nama: {cd[0]}<br>Usia: {cd[1]}<br>Profesi: {cd[2]}"
                        f"<br>IKR Agregat: {cd[3]}<br>Klaster: {cd[4]}"
                    )
                    for cd in node_customdata
                ]
                fig_base_spatial = build_spatial_node_figure(
                    G,
                    node_ids=node_ids,
                    node_color_vals=[0.0 for _ in node_ids],
                    node_hover_text=base_hover,
                    title="Sebaran Spasial Node pada Jaringan Kemiripan",
                    spatial_mode=graph_spatial_mode,
                    marker_size=10,
                    colorscale=[[0.0, "#0ea5e9"], [1.0, "#0ea5e9"]],
                    cmin=0.0,
                    cmax=1.0,
                    colorbar=dict(title="Node"),
                )
                if fig_base_spatial is not None:
                    st.plotly_chart(fig_base_spatial, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                else:
                    st.warning("Mode spasial aktif, tetapi kolom lat/lon belum valid. Ditampilkan mode layout jaringan.")
                    st.plotly_chart(fig_base, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

            fig_louvain_focus = go.Figure()
            add_network_edge_traces(
                fig_louvain_focus,
                visible_edges_focus,
                pos_focus,
                edge_min,
                edge_span,
                color_fn=edge_color_by_interaction,
                base_width=0.3,
                width_scale=0.95,
                hover=True,
            )
            fig_louvain_focus.add_trace(
                go.Scatter(
                    x=[pos_focus[n][0] for n in node_ids],
                    y=[pos_focus[n][1] for n in node_ids],
                    mode="markers",
                    marker=dict(
                        size=node_size_main + 0.8,
                        color=[cid_to_idx.get(partition.get(n, -1), 0) for n in node_ids],
                        colorscale=cluster_colorscale,
                        cmin=-0.5,
                        cmax=max(len(cluster_ids_sorted) - 0.5, 0.5),
                        opacity=0.9,
                        line=dict(color=NETWORK_NODE_LINE, width=node_line_width),
                        showscale=True,
                        colorbar=dict(
                            title="Klaster Louvain",
                            tickmode="array",
                            tickvals=list(range(len(cluster_ids_sorted))),
                            ticktext=[f"Klaster {cid}" for cid in cluster_ids_sorted],
                        ),
                    ),
                    customdata=node_customdata,
                    hovertemplate=(
                        "Nama: %{customdata[0]}<br>"
                        "Usia: %{customdata[1]}<br>"
                        "Profesi: %{customdata[2]}<br>"
                        "IKR Agregat: %{customdata[3]}<br>"
                        "Klaster: %{customdata[4]}<extra></extra>"
                    ),
                    name="Node KK",
                )
            )
            style_network_figure(
                fig_louvain_focus,
                title="Jaringan Komunitas Louvain Berdasarkan Kemiripan Dimensi Kesejahteraan",
                height=670,
            )
            if graph_spatial_mode == "Layout Jaringan":
                st.plotly_chart(fig_louvain_focus, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
            else:
                louvain_hover = [
                    (
                        f"Nama: {cd[0]}<br>Usia: {cd[1]}<br>Profesi: {cd[2]}"
                        f"<br>IKR Agregat: {cd[3]}<br>Klaster: {cd[4]}"
                    )
                    for cd in node_customdata
                ]
                louvain_color_vals = [cid_to_idx.get(partition.get(n, -1), 0) for n in node_ids]
                fig_louvain_spatial = build_spatial_node_figure(
                    G,
                    node_ids=node_ids,
                    node_color_vals=louvain_color_vals,
                    node_hover_text=louvain_hover,
                    title="Sebaran Spasial Komunitas Louvain",
                    spatial_mode=graph_spatial_mode,
                    marker_size=12,
                    colorscale=cluster_colorscale,
                    cmin=-0.5,
                    cmax=max(len(cluster_ids_sorted) - 0.5, 0.5),
                    colorbar=dict(
                        title="Klaster Louvain",
                        tickmode="array",
                        tickvals=list(range(len(cluster_ids_sorted))),
                        ticktext=[f"Klaster {cid}" for cid in cluster_ids_sorted],
                    ),
                )
                if fig_louvain_spatial is not None:
                    st.plotly_chart(fig_louvain_spatial, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                else:
                    st.warning("Mode spasial aktif, tetapi kolom lat/lon belum valid. Ditampilkan mode layout jaringan.")
                    st.plotly_chart(fig_louvain_focus, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

        with subbab_dropdown("Proporsi Klaster Louvain per Dusun", expanded=True):
            dusun_attr_cluster = "dusun" if "dusun" in df_v.columns else col_spasial
            df_dusun_cluster, df_dusun_cluster_wide, df_cluster_overall = build_dusun_cluster_composition(
                G,
                dusun_attr=dusun_attr_cluster,
                partition=partition,
            )
            if df_dusun_cluster.empty:
                st.info("Komposisi klaster per dusun belum dapat dihitung karena atribut dusun atau node graf belum tersedia.")
            else:
                st.caption(
                    "Proporsi dihitung dari KK yang masuk graf aktif. Persentase pada diagram dusun dibaca terhadap total KK di dusun masing-masing."
                )
                dominant_cluster_row = df_cluster_overall.sort_values("Jumlah KK", ascending=False).iloc[0]
                m_prop1, m_prop2, m_prop3, m_prop4 = st.columns(4)
                m_prop1.metric("KK Terpetakan", f"{int(df_cluster_overall['Jumlah KK'].sum())}")
                m_prop2.metric("Jumlah Klaster", f"{int(df_cluster_overall['Klaster Louvain'].nunique())}")
                m_prop3.metric("Jumlah Dusun", f"{int(df_dusun_cluster['Dusun'].nunique())}")
                m_prop4.metric(
                    "Klaster Dominan",
                    dominant_cluster_row["Klaster Louvain"],
                    f"{float(dominant_cluster_row['Persentase KK (%)']):.1f}%",
                )

                cluster_label_order = df_cluster_overall["Klaster Louvain"].tolist()
                cluster_label_color_map = {}
                for _, row_cluster in df_cluster_overall.iterrows():
                    cid = int(row_cluster["ID Klaster Internal"])
                    label = row_cluster["Klaster Louvain"]
                    cluster_label_color_map[label] = (
                        "#94A3B8"
                        if cid < 0
                        else cluster_color_map.get(cid, cluster_palette[cid_to_idx.get(cid, 0) % len(cluster_palette)])
                    )

                plot_prop_1, plot_prop_2 = st.columns([0.9, 1.35])
                with plot_prop_1:
                    fig_cluster_overall = px.bar(
                        df_cluster_overall,
                        x="Klaster Louvain",
                        y="Persentase KK (%)",
                        color="Klaster Louvain",
                        color_discrete_map=cluster_label_color_map,
                        text="Label Batang",
                        hover_data={
                            "Jumlah KK": True,
                            "Persentase KK (%)": ":.2f",
                            "ID Klaster Internal": True,
                            "Klaster Louvain": False,
                        },
                        category_orders={"Klaster Louvain": cluster_label_order},
                    )
                    fig_cluster_overall.update_traces(
                        textposition="outside",
                        cliponaxis=False,
                        marker_line_color="#111827",
                        marker_line_width=0.5,
                    )
                    style_publication_figure(
                        fig_cluster_overall,
                        title="Proporsi KK Menurut Klaster",
                        height=430,
                        xaxis_title="",
                        yaxis_title="Persentase KK (%)",
                        showlegend=False,
                    )
                    fig_cluster_overall.update_yaxes(range=[0, 100], ticksuffix="%")
                    st.plotly_chart(fig_cluster_overall, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                with plot_prop_2:
                    dusun_order_for_plot = (
                        df_dusun_cluster.groupby("Dusun")["Total KK Dusun"]
                        .max()
                        .sort_values(ascending=True)
                        .index
                        .tolist()
                    )
                    fig_dusun_cluster = px.bar(
                        df_dusun_cluster,
                        x="Persentase dalam Dusun (%)",
                        y="Dusun",
                        color="Klaster Louvain",
                        orientation="h",
                        barmode="stack",
                        text="Label Persen",
                        color_discrete_map=cluster_label_color_map,
                        category_orders={
                            "Dusun": dusun_order_for_plot,
                            "Klaster Louvain": cluster_label_order,
                        },
                        hover_data={
                            "Jumlah KK": True,
                            "Total KK Dusun": True,
                            "Persentase dalam Dusun (%)": ":.2f",
                            "Persentase dari Total Graf (%)": ":.2f",
                            "ID Klaster Internal": True,
                        },
                    )
                    fig_dusun_cluster.update_traces(
                        textposition="inside",
                        insidetextanchor="middle",
                        marker_line_color="#FFFFFF",
                        marker_line_width=0.6,
                    )
                    style_publication_figure(
                        fig_dusun_cluster,
                        title=f"Proporsi Klaster dalam Setiap {dusun_attr_cluster.title()}",
                        height=max(430, min(820, 230 + (28 * len(dusun_order_for_plot)))),
                        xaxis_title="Proporsi dalam dusun (%)",
                        yaxis_title="",
                        legend_title="Klaster",
                    )
                    fig_dusun_cluster.update_xaxes(range=[0, 100], ticksuffix="%")
                    st.plotly_chart(fig_dusun_cluster, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                pct_cols = [c for c in df_dusun_cluster_wide.columns if c.endswith("Persentase (%)")]
                fmt_cols = {c: "{:.1f}" for c in pct_cols}
                count_cols = [c for c in df_dusun_cluster_wide.columns if c.endswith("Jumlah KK") or c == "Total KK Dusun"]
                fmt_cols.update({c: "{:,.0f}" for c in count_cols})
                st.markdown("##### Tabel Proporsi Klaster per Dusun")
                tab_prop_wide, tab_prop_long = st.tabs(["Ringkas per Dusun", "Detail Dusun-Klaster"])
                with tab_prop_wide:
                    st.dataframe(
                        df_dusun_cluster_wide.style.format(fmt_cols).background_gradient(cmap="YlGnBu", subset=pct_cols),
                        use_container_width=True,
                    )
                with tab_prop_long:
                    detail_cols = [
                        "Dusun",
                        "Klaster Louvain",
                        "ID Klaster Internal",
                        "Jumlah KK",
                        "Total KK Dusun",
                        "Persentase dalam Dusun (%)",
                        "Persentase dari Total Graf (%)",
                        "Persentase dari Klaster (%)",
                    ]
                    st.dataframe(
                        df_dusun_cluster[detail_cols].style.format(
                            {
                                "Jumlah KK": "{:,.0f}",
                                "Total KK Dusun": "{:,.0f}",
                                "Persentase dalam Dusun (%)": "{:.2f}",
                                "Persentase dari Total Graf (%)": "{:.2f}",
                                "Persentase dari Klaster (%)": "{:.2f}",
                            }
                        ).background_gradient(cmap="YlGnBu", subset=["Persentase dalam Dusun (%)"]),
                        use_container_width=True,
                    )

        if selected_centrality_key != "none":
            centrality_name = {
                "degree": "Degree Centrality",
                "betweenness": "Betweenness Centrality",
                "closeness": "Closeness Centrality",
                "eigenvector": "Eigenvector Centrality",
            }.get(selected_centrality_key, "Centrality")
            all_centrality_specs = [
                ("Degree Centrality", "degree"),
                ("Betweenness Centrality", "betweenness"),
                ("Closeness Centrality", "closeness"),
                ("Eigenvector Centrality", "eigenvector"),
            ]
            centrality_metric_values = {
                metric_label: compute_centrality_on_similarity_graph(G, metric_key)
                for metric_label, metric_key in all_centrality_specs
            }
            centrality_vals = centrality_metric_values.get(centrality_name, {})
            if centrality_vals:
                st.markdown(f"### Analisis {centrality_name} pada Jaringan Louvain")
                st.caption(centrality_help_text(selected_centrality_key))
                publish_mode = st.toggle(
                    "Mode publikasi / anonimisasi",
                    value=True,
                    key=f"centrality_publish_mode_{selected_centrality_key}",
                    help="Jika aktif, nama, family_id, dusun asli, dan koordinat presisi tidak ditampilkan pada bagian centrality.",
                )
                highlight_roles = st.toggle(
                    "Highlight Peran Struktural",
                    value=False,
                    key=f"centrality_highlight_roles_{selected_centrality_key}",
                    help="Tampilkan visual tambahan yang mewarnai node berdasarkan peran struktural kebijakan.",
                )
                if publish_mode and graph_spatial_mode != "Layout Jaringan":
                    st.info("Mode publikasi aktif: visual centrality memakai layout jaringan agar koordinat rumah tangga tidak diekspos.")
                dusun_attr_centrality = "dusun" if "dusun" in df_v.columns else col_spasial
                anon_node_map = make_anonymized_node_mapping(node_ids)
                dusun_values_all = sorted(
                    {
                        str(G.nodes[n].get(dusun_attr_centrality, "Tidak tersedia"))
                        for n in node_ids
                    }
                )
                dusun_code_map = {val: f"Dusun-{idx + 1}" for idx, val in enumerate(dusun_values_all)}
                node_centrality_rows = []
                for n in node_ids:
                    n_attr = G.nodes[n]
                    profesi_raw = n_attr.get(
                        "profesi pekerjaan",
                        n_attr.get("profesi_pekerjaan", n_attr.get("pekerjaan", n_attr.get("profesi", "Tidak diketahui"))),
                    )
                    bansos_status = "Penerima" if int(_safe_float_metric(n_attr.get("bansos_num"), default=0.0) > 0) == 1 else "Tidak Menerima"
                    row = {
                        "family_id": n,
                        "Nama": n_attr.get("nama", "-"),
                        "Kode Node": anon_node_map.get(str(n), "N-000"),
                        "Klaster Louvain": int(partition.get(n, -1)),
                        "Dusun": n_attr.get(dusun_attr_centrality, "-"),
                        "Dusun/Kode Dusun": (
                            dusun_code_map.get(str(n_attr.get(dusun_attr_centrality, "Tidak tersedia")), "Dusun-0")
                            if publish_mode
                            else str(n_attr.get(dusun_attr_centrality, "-"))
                        ),
                        "Profesi/Pekerjaan": str(profesi_raw).strip() if pd.notnull(profesi_raw) else "Tidak diketahui",
                        "Status Bansos": bansos_status,
                        "IKR Agregat": _safe_float_metric(n_attr.get("f_ikr_dari_rekap_kk"), default=np.nan),
                        "internet_num": n_attr.get("internet_num", n_attr.get("digital_num", np.nan)),
                        "ponsel_num": n_attr.get("ponsel_num", np.nan),
                    }
                    for metric_label, _ in all_centrality_specs:
                        row[metric_label] = float(centrality_metric_values.get(metric_label, {}).get(n, 0.0))
                    row["Status BPS"] = n_attr.get("kategori_ikr", categorize_ikr_bps(row["IKR Agregat"])[0])
                    for dim_label, dim_col in IKR_DIMENSION_MAP:
                        row[dim_label] = _safe_float_metric(n_attr.get(dim_col), default=np.nan)
                    node_centrality_rows.append(row)
                df_centrality = pd.DataFrame(node_centrality_rows).sort_values(centrality_name, ascending=False).reset_index(drop=True)
                c_series_full = pd.to_numeric(df_centrality[centrality_name], errors="coerce").fillna(0.0)
                b_series_full = pd.to_numeric(df_centrality["Betweenness Centrality"], errors="coerce").fillna(0.0)
                c_q25 = float(c_series_full.quantile(0.25)) if not c_series_full.empty else 0.0
                c_q75 = float(c_series_full.quantile(0.75)) if not c_series_full.empty else 0.0
                b_q75 = float(b_series_full.quantile(0.75)) if not b_series_full.empty else 0.0
                df_centrality["_centrality_q25"] = c_q25
                df_centrality["_centrality_q75"] = c_q75
                df_centrality["_betweenness_q75"] = b_q75
                df_centrality["Level Centrality"] = df_centrality[centrality_name].map(
                    lambda v: centrality_level_from_quantile(v, c_q25, c_q75)
                )
                df_centrality["Level IKR"] = df_centrality.apply(
                    lambda r: ikr_level_from_value(r.get("IKR Agregat"), r.get("Status BPS")),
                    axis=1,
                )
                df_centrality["Akses Informasi"] = df_centrality.apply(access_info_label, axis=1)
                df_centrality["Peran Struktural"] = df_centrality.apply(
                    lambda r: classify_centrality_policy_role(
                        r,
                        centrality_col=centrality_name,
                        betweenness_col="Betweenness Centrality",
                    ),
                    axis=1,
                )
                df_centrality["Implikasi Program"] = df_centrality["Peran Struktural"].map(centrality_role_implication)
                df_centrality["Catatan Etika"] = df_centrality["Peran Struktural"].map(centrality_role_ethics_note)
                df_centrality["Centrality terpilih"] = df_centrality[centrality_name]
                df_centrality["Hover Aman"] = df_centrality.apply(lambda r: safe_hover_text(r, publish_mode=publish_mode), axis=1)

                dim_labels = [d[0] for d in IKR_DIMENSION_MAP]
                display_identity_cols = ["Kode Node", "Klaster Louvain", "Dusun/Kode Dusun"] if publish_mode else [
                    "Kode Node",
                    "Nama",
                    "family_id",
                    "Klaster Louvain",
                    "Dusun",
                ]
                display_cols = [
                    *display_identity_cols,
                    *dim_labels,
                    "IKR Agregat",
                    "Status BPS",
                    "Profesi/Pekerjaan",
                    "Status Bansos",
                    "Akses Informasi",
                    "Peran Struktural",
                    centrality_name,
                ]

                st.markdown("#### Filter Visual Jaringan Centrality")
                cluster_opts_all = sorted(df_centrality["Klaster Louvain"].dropna().unique().tolist())
                dusun_opts_all = sorted(df_centrality["Dusun"].fillna("Tidak Valid").astype(str).unique().tolist())
                f1, f2 = st.columns(2)
                with f1:
                    selected_clusters_view = st.multiselect(
                        "Pilih Klaster untuk Visual",
                        options=cluster_opts_all,
                        default=cluster_opts_all,
                        key=f"cent_filter_cluster_{selected_centrality_key}",
                    )
                with f2:
                    selected_dusun_view = st.multiselect(
                        "Pilih Dusun untuk Visual",
                        options=dusun_opts_all,
                        default=dusun_opts_all,
                        format_func=lambda x: dusun_code_map.get(str(x), str(x)) if publish_mode else str(x),
                        key=f"cent_filter_dusun_{selected_centrality_key}",
                    )

                df_centrality_view = df_centrality[
                    df_centrality["Klaster Louvain"].isin(selected_clusters_view)
                    & df_centrality["Dusun"].astype(str).isin([str(x) for x in selected_dusun_view])
                ].copy()
                if df_centrality_view.empty:
                    st.warning("Filter klaster/dusun tidak memiliki node. Silakan ubah filter.")
                else:
                    selected_node_set = set(df_centrality_view["family_id"].tolist())
                    G_view = G.subgraph(selected_node_set).copy()
                    centrality_view_metric_values = {
                        metric_label: compute_centrality_on_similarity_graph(G_view, metric_key)
                        for metric_label, metric_key in all_centrality_specs
                    }
                    for metric_label, _ in all_centrality_specs:
                        fallback_vals = centrality_metric_values.get(metric_label, {})
                        view_vals = centrality_view_metric_values.get(metric_label, {})
                        df_centrality_view[metric_label] = df_centrality_view["family_id"].map(
                            lambda nid, vals=view_vals, fallback=fallback_vals: float(vals.get(nid, fallback.get(nid, 0.0)))
                        )
                    c_series_view = pd.to_numeric(df_centrality_view[centrality_name], errors="coerce").fillna(0.0)
                    b_series_view = pd.to_numeric(df_centrality_view["Betweenness Centrality"], errors="coerce").fillna(0.0)
                    c_q25_view = float(c_series_view.quantile(0.25)) if not c_series_view.empty else 0.0
                    c_q75_view = float(c_series_view.quantile(0.75)) if not c_series_view.empty else 0.0
                    b_q75_view = float(b_series_view.quantile(0.75)) if not b_series_view.empty else 0.0
                    df_centrality_view["_centrality_q25"] = c_q25_view
                    df_centrality_view["_centrality_q75"] = c_q75_view
                    df_centrality_view["_betweenness_q75"] = b_q75_view
                    df_centrality_view["Level Centrality"] = df_centrality_view[centrality_name].map(
                        lambda v: centrality_level_from_quantile(v, c_q25_view, c_q75_view)
                    )
                    df_centrality_view["Level IKR"] = df_centrality_view.apply(
                        lambda r: ikr_level_from_value(r.get("IKR Agregat"), r.get("Status BPS")),
                        axis=1,
                    )
                    df_centrality_view["Akses Informasi"] = df_centrality_view.apply(access_info_label, axis=1)
                    df_centrality_view["Peran Struktural"] = df_centrality_view.apply(
                        lambda r: classify_centrality_policy_role(
                            r,
                            centrality_col=centrality_name,
                            betweenness_col="Betweenness Centrality",
                        ),
                        axis=1,
                    )
                    df_centrality_view["Implikasi Program"] = df_centrality_view["Peran Struktural"].map(centrality_role_implication)
                    df_centrality_view["Catatan Etika"] = df_centrality_view["Peran Struktural"].map(centrality_role_ethics_note)
                    df_centrality_view["Centrality terpilih"] = df_centrality_view[centrality_name]
                    df_centrality_view["Hover Aman"] = df_centrality_view.apply(lambda r: safe_hover_text(r, publish_mode=publish_mode), axis=1)
                    df_centrality_view = df_centrality_view.sort_values(centrality_name, ascending=False).reset_index(drop=True)

                    m_cent1, m_cent2, m_cent3 = st.columns(3)
                    m_cent1.metric("Node Terpilih", f"{int(df_centrality_view.shape[0])}")
                    m_cent2.metric("Edge Terpilih", f"{int(G_view.number_of_edges())}")
                    m_cent3.metric("Nilai Tertinggi", f"{float(df_centrality_view[centrality_name].max()):.6f}")

                    st.markdown(f"#### Visual Jaringan Louvain Dinamis ({centrality_name})")
                    if G_view.number_of_nodes() >= 1:
                        fig_cent = go.Figure()
                        edge_weights_view = [_safe_float_metric(d.get("weight"), default=0.0) for _, _, d in G_view.edges(data=True)]
                        edge_min_v = float(min(edge_weights_view)) if edge_weights_view else 0.0
                        edge_max_v = float(max(edge_weights_view)) if edge_weights_view else 1.0
                        edge_span_v = max(edge_max_v - edge_min_v, 1e-9)
                        visible_edges_view = select_representative_edges(
                            G_view,
                            max_edges=int(np.clip(G_view.number_of_nodes() * 1.25, 120, 650)),
                            per_node=1,
                        )
                        add_network_edge_traces(
                            fig_cent,
                            visible_edges_view,
                            pos_focus,
                            edge_min_v,
                            edge_span_v,
                            color_fn=edge_color_by_interaction,
                            base_width=0.26,
                            width_scale=0.78,
                            hover=False,
                        )
                        node_order = list(G_view.nodes())
                        df_cent_lookup = df_centrality_view.set_index("family_id")
                        node_val_arr = np.array(
                            [float(df_cent_lookup.loc[n, centrality_name]) for n in node_order],
                            dtype=float,
                        )
                        cmin_n = float(np.nanmin(node_val_arr)) if len(node_val_arr) else 0.0
                        cmax_n = float(np.nanmax(node_val_arr)) if len(node_val_arr) else 1.0
                        size_vals = centrality_marker_sizes(node_val_arr, len(node_order))
                        cent_hover_text = [
                            safe_hover_text(df_cent_lookup.loc[n], publish_mode=publish_mode)
                            for n in node_order
                        ]
                        fig_cent.add_trace(
                            go.Scatter(
                                x=[pos_focus[n][0] for n in node_order],
                                y=[pos_focus[n][1] for n in node_order],
                                mode="markers",
                                marker=dict(
                                    size=size_vals,
                                    color=node_val_arr.tolist(),
                                    colorscale="Viridis",
                                    showscale=True,
                                    cmin=cmin_n,
                                    cmax=cmax_n if cmax_n > cmin_n else (cmin_n + 1e-6),
                                    colorbar=dict(title=centrality_name),
                                    opacity=0.82,
                                    line=dict(color=NETWORK_NODE_LINE, width=0.4),
                                ),
                                text=cent_hover_text,
                                hoverinfo="text",
                                showlegend=False,
                            )
                        )
                        top_ring_n = int(min(14, max(5, round(len(node_order) * 0.025))))
                        top_ring_nodes = df_centrality_view.head(top_ring_n)["family_id"].tolist()
                        top_ring_size = {
                            n: min(float(size_vals[idx]) + 4.0, max(size_vals) + 5.0)
                            for idx, n in enumerate(node_order)
                        }
                        if top_ring_nodes:
                            fig_cent.add_trace(
                                go.Scatter(
                                    x=[pos_focus[n][0] for n in top_ring_nodes if n in pos_focus],
                                    y=[pos_focus[n][1] for n in top_ring_nodes if n in pos_focus],
                                    mode="markers",
                                    marker=dict(
                                        size=[top_ring_size.get(n, 13.0) for n in top_ring_nodes if n in pos_focus],
                                        color="rgba(255,255,255,0)",
                                        line=dict(color="#111827", width=1.4),
                                    ),
                                    hoverinfo="skip",
                                    showlegend=False,
                                )
                            )
                        style_network_figure(
                            fig_cent,
                            title=f"Jaringan Louvain Terfilter Menurut {centrality_name}",
                            height=690,
                        )
                        if graph_spatial_mode == "Layout Jaringan" or publish_mode:
                            st.plotly_chart(fig_cent, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                        else:
                            fig_cent_spatial = build_spatial_node_figure(
                                G_view,
                                node_ids=node_order,
                                node_color_vals=node_val_arr.tolist(),
                                node_hover_text=cent_hover_text,
                                title=f"Sebaran Spasial Louvain Terfilter Menurut {centrality_name}",
                                spatial_mode=graph_spatial_mode,
                                marker_size=13,
                                colorscale="Viridis",
                                cmin=cmin_n,
                                cmax=cmax_n if cmax_n > cmin_n else (cmin_n + 1e-6),
                                colorbar=dict(title=centrality_name),
                            )
                            if fig_cent_spatial is not None:
                                st.plotly_chart(fig_cent_spatial, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                            else:
                                st.warning("Mode spasial aktif, tetapi kolom lat/lon belum valid. Ditampilkan mode layout jaringan.")
                                st.plotly_chart(fig_cent, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                    role_narrative = build_centrality_policy_narrative(df_centrality_view, centrality_name)
                    st.markdown("#### Interpretasi Struktural dan Implikasi Program")
                    st.info(role_narrative)

                    with subbab_dropdown("Catatan Etika dan Batasan Interpretasi", expanded=publish_mode):
                        st.markdown(
                            """
                            - Data mikro rumah tangga adalah data sensitif.
                            - Identitas individu/KK perlu disamarkan dalam visualisasi publik.
                            - Centrality tidak boleh dimaknai sebagai status sosial seseorang.
                            - Hasil centrality tidak membuktikan inclusion error atau exclusion error.
                            - Hasil hanya mendukung proses verifikasi, evaluasi, dan diskusi kebijakan.
                            - Verifikasi lapangan dan persetujuan penggunaan data tetap diperlukan.
                            - Hindari penyebutan nama orang, alamat spesifik, atau koordinat presisi pada materi presentasi/publikasi.
                            """
                        )

                    if highlight_roles and G_view.number_of_nodes() >= 1:
                        st.markdown("#### Network Highlight Node Strategis")
                        fig_role = go.Figure()
                        role_edge_weights = [_safe_float_metric(d.get("weight"), default=0.0) for _, _, d in G_view.edges(data=True)]
                        role_edge_min = float(min(role_edge_weights)) if role_edge_weights else 0.0
                        role_edge_max = float(max(role_edge_weights)) if role_edge_weights else 1.0
                        role_edge_span = max(role_edge_max - role_edge_min, 1e-9)
                        role_visible_edges = select_representative_edges(
                            G_view,
                            max_edges=int(np.clip(G_view.number_of_nodes() * 1.25, 120, 650)),
                            per_node=1,
                        )
                        add_network_edge_traces(
                            fig_role,
                            role_visible_edges,
                            pos_focus,
                            role_edge_min,
                            role_edge_span,
                            color_fn=lambda *_args, **_kwargs: "rgba(148, 163, 184, 0.18)",
                            base_width=0.22,
                            width_scale=0.55,
                            hover=False,
                        )
                        role_lookup = df_centrality_view.set_index("family_id")
                        role_node_order = [n for n in G_view.nodes() if n in role_lookup.index and n in pos_focus]
                        role_values = np.array([float(role_lookup.loc[n, centrality_name]) for n in role_node_order], dtype=float)
                        role_sizes = centrality_marker_sizes(role_values, len(role_node_order))
                        size_lookup = {n: role_sizes[idx] for idx, n in enumerate(role_node_order)}
                        present_roles = [
                            role for role in CENTRALITY_ROLE_ORDER
                            if role in set(df_centrality_view["Peran Struktural"].astype(str))
                        ]
                        for role in present_roles:
                            role_nodes = [
                                n for n in role_node_order
                                if str(role_lookup.loc[n, "Peran Struktural"]) == role
                            ]
                            if not role_nodes:
                                continue
                            is_general = role == "Node umum"
                            fig_role.add_trace(
                                go.Scatter(
                                    x=[pos_focus[n][0] for n in role_nodes],
                                    y=[pos_focus[n][1] for n in role_nodes],
                                    mode="markers",
                                    marker=dict(
                                        size=[size_lookup.get(n, node_size_main) for n in role_nodes],
                                        color=CENTRALITY_ROLE_COLORS.get(role, "#94A3B8"),
                                        opacity=0.26 if is_general else 0.9,
                                        line=dict(
                                            color="rgba(71, 85, 105, 0.35)" if is_general else NETWORK_NODE_LINE,
                                            width=0.35 if is_general else 0.85,
                                        ),
                                    ),
                                    text=[safe_hover_text(role_lookup.loc[n], publish_mode=publish_mode) for n in role_nodes],
                                    hoverinfo="text",
                                    name=role,
                                )
                            )
                        top5_role_nodes = df_centrality_view.head(5)["family_id"].tolist()
                        top5_role_nodes = [n for n in top5_role_nodes if n in pos_focus]
                        if top5_role_nodes:
                            fig_role.add_trace(
                                go.Scatter(
                                    x=[pos_focus[n][0] for n in top5_role_nodes],
                                    y=[pos_focus[n][1] for n in top5_role_nodes],
                                    mode="markers",
                                    marker=dict(
                                        size=[size_lookup.get(n, node_size_main) + 5.0 for n in top5_role_nodes],
                                        color="rgba(255,255,255,0)",
                                        line=dict(color="#111827", width=1.8),
                                    ),
                                    hoverinfo="skip",
                                    name="Top 5 centrality",
                                    showlegend=True,
                                )
                            )
                        style_network_figure(
                            fig_role,
                            title=f"Highlight Peran Struktural Berdasarkan {centrality_name}",
                            height=690,
                            showlegend=True,
                        )
                        st.plotly_chart(fig_role, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                    st.markdown("#### Hubungan Centrality dengan Kesejahteraan Rumah Tangga")
                    scatter_cols = [
                        "Kode Node",
                        "Klaster Louvain",
                        "Dusun/Kode Dusun",
                        "IKR Agregat",
                        "Status BPS",
                        "Status Bansos",
                        "Akses Informasi",
                        "Peran Struktural",
                        "Hover Aman",
                        centrality_name,
                        "Degree Centrality",
                        "Betweenness Centrality",
                    ]
                    if not publish_mode:
                        scatter_cols.extend(["Nama", "family_id", "Dusun"])
                    scatter_cols = unique_existing_columns(df_centrality_view, scatter_cols)
                    scatter_df = df_centrality_view[scatter_cols].copy()
                    scatter_df = scatter_df.dropna(subset=[centrality_name, "IKR Agregat"])
                    if scatter_df.empty:
                        st.info("Scatter centrality dan IKR belum dapat ditampilkan karena data IKR atau centrality tidak lengkap.")
                    else:
                        size_source = "Betweenness Centrality"
                        if float(pd.to_numeric(scatter_df[size_source], errors="coerce").fillna(0.0).max()) <= 0:
                            size_source = "Degree Centrality"
                        scatter_df["Ukuran Visual"] = pd.to_numeric(scatter_df[size_source], errors="coerce").fillna(0.0)
                        scatter_df["Ukuran Visual"] = scatter_df["Ukuran Visual"] + max(float(scatter_df["Ukuran Visual"].max()) * 0.05, 1e-6)
                        fig_scatter = px.scatter(
                            scatter_df,
                            x=centrality_name,
                            y="IKR Agregat",
                            color="Status Bansos",
                            size="Ukuran Visual",
                            symbol="Peran Struktural",
                            hover_name="Kode Node",
                            custom_data=["Hover Aman"],
                            title="Hubungan Centrality dengan Kesejahteraan Rumah Tangga",
                            labels={centrality_name: centrality_name, "IKR Agregat": "IKR Agregat"},
                            color_discrete_map={"Penerima": "#2563EB", "Tidak Menerima": "#B91C1C"},
                            category_orders={"Peran Struktural": CENTRALITY_ROLE_ORDER},
                        )
                        fig_scatter.update_traces(hovertemplate="%{customdata[0]}<extra></extra>", marker=dict(line=dict(color="#111827", width=0.45)))
                        median_centrality = float(pd.to_numeric(scatter_df[centrality_name], errors="coerce").median())
                        ikr_cutoff = 60.0 if pd.to_numeric(scatter_df["IKR Agregat"], errors="coerce").notna().any() else float(pd.to_numeric(scatter_df["IKR Agregat"], errors="coerce").median())
                        fig_scatter.add_vline(x=median_centrality, line_dash="dash", line_color="#111827", annotation_text="Median centrality")
                        fig_scatter.add_hline(y=ikr_cutoff, line_dash="dash", line_color="#B91C1C", annotation_text=f"Ambang IKR {ikr_cutoff:.0f}")
                        fig_scatter.add_annotation(x=0.77, y=0.20, xref="paper", yref="paper", text="Prioritas verifikasi", showarrow=False, font=dict(size=12, color="#92400E"))
                        fig_scatter.add_annotation(x=0.77, y=0.83, xref="paper", yref="paper", text="Penghubung informasi", showarrow=False, font=dict(size=12, color="#1D4ED8"))
                        fig_scatter.add_annotation(x=0.20, y=0.20, xref="paper", yref="paper", text="Rentan terisolasi", showarrow=False, font=dict(size=12, color="#B91C1C"))
                        fig_scatter.add_annotation(x=0.24, y=0.83, xref="paper", yref="paper", text="Relatif stabil, tidak dominan jaringan", showarrow=False, font=dict(size=12, color="#475569"))
                        style_publication_figure(
                            fig_scatter,
                            title="Hubungan Centrality dengan Kesejahteraan Rumah Tangga",
                            height=560,
                            xaxis_title=centrality_name,
                            yaxis_title="IKR Agregat",
                            legend_title="Status / Peran",
                        )
                        st.plotly_chart(fig_scatter, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                    st.markdown("#### Matriks Posisi Struktural dan Kondisi Kesejahteraan")
                    matrix_df = df_centrality_view.dropna(subset=[centrality_name, "IKR Agregat"]).copy()
                    if matrix_df.empty:
                        st.info("Matriks belum dapat dihitung karena data IKR atau centrality tidak lengkap.")
                    else:
                        matrix_df["Kelompok IKR"] = np.where(
                            matrix_df["Level IKR"].isin(["Rendah", "Sedang"]),
                            "IKR rendah/sedang",
                            "IKR tinggi/sangat tinggi",
                        )
                        matrix_df["Kelompok Centrality"] = np.where(
                            matrix_df["Level Centrality"].eq("Tinggi"),
                            "Centrality tinggi",
                            "Centrality rendah/sedang",
                        )
                        y_order_matrix = ["IKR rendah/sedang", "IKR tinggi/sangat tinggi"]
                        x_order_matrix = ["Centrality rendah/sedang", "Centrality tinggi"]
                        policy_meaning = {
                            ("IKR rendah/sedang", "Centrality tinggi"): "Strategis untuk verifikasi/pendampingan",
                            ("IKR rendah/sedang", "Centrality rendah/sedang"): "Rentan dan perlu pendekatan langsung",
                            ("IKR tinggi/sangat tinggi", "Centrality tinggi"): "Penghubung informasi potensial",
                            ("IKR tinggi/sangat tinggi", "Centrality rendah/sedang"): "Relatif stabil, bukan prioritas jaringan",
                        }
                        total_matrix = max(int(len(matrix_df)), 1)
                        z_vals, text_vals, table_rows = [], [], []
                        for y_label in y_order_matrix:
                            z_row, text_row = [], []
                            for x_label in x_order_matrix:
                                count_val = int(((matrix_df["Kelompok IKR"] == y_label) & (matrix_df["Kelompok Centrality"] == x_label)).sum())
                                pct_val = float((count_val / total_matrix) * 100.0)
                                meaning = policy_meaning[(y_label, x_label)]
                                z_row.append(count_val)
                                text_row.append(f"{count_val} node<br>{pct_val:.1f}%<br>{meaning}")
                                table_rows.append(
                                    {
                                        "Kelompok IKR": y_label,
                                        "Kelompok Centrality": x_label,
                                        "Jumlah Node": count_val,
                                        "Persentase Node (%)": pct_val,
                                        "Makna Kebijakan": meaning,
                                    }
                                )
                            z_vals.append(z_row)
                            text_vals.append(text_row)
                        fig_matrix = go.Figure(
                            go.Heatmap(
                                z=z_vals,
                                x=x_order_matrix,
                                y=y_order_matrix,
                                text=text_vals,
                                texttemplate="%{text}",
                                colorscale="YlGnBu",
                                colorbar=dict(title="Jumlah node"),
                                hovertemplate="%{y}<br>%{x}<br>%{text}<extra></extra>",
                            )
                        )
                        style_publication_figure(
                            fig_matrix,
                            title="Matriks Posisi Struktural dan Kondisi Kesejahteraan",
                            height=420,
                            xaxis_title="Centrality",
                            yaxis_title="Kondisi kesejahteraan",
                        )
                        st.plotly_chart(fig_matrix, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                        st.dataframe(
                            pd.DataFrame(table_rows).style.format({"Persentase Node (%)": "{:.2f}"}),
                            use_container_width=True,
                        )

                    st.markdown("#### Profil Node Strategis dan Implikasi Program")
                    top_n_profile = st.slider(
                        "Jumlah node pada tabel profil",
                        min_value=5,
                        max_value=30,
                        value=15,
                        step=5,
                        key=f"centrality_profile_topn_{selected_centrality_key}",
                    )
                    role_rank = {role: idx for idx, role in enumerate(CENTRALITY_ROLE_ORDER)}
                    df_profile = df_centrality_view.copy()
                    df_profile["_role_rank"] = df_profile["Peran Struktural"].map(role_rank).fillna(len(role_rank))
                    df_profile = (
                        df_profile.sort_values(["_role_rank", centrality_name], ascending=[True, False])
                        .head(int(top_n_profile))
                        .copy()
                    )
                    profile_cols = [
                        "Kode Node",
                        "Klaster Louvain",
                        "Dusun/Kode Dusun",
                        "Centrality terpilih",
                        "Degree Centrality",
                        "Betweenness Centrality",
                        "Closeness Centrality",
                        "Eigenvector Centrality",
                        "IKR Agregat",
                        "Status BPS",
                        "Status Bansos",
                        "Akses Informasi",
                        "Peran Struktural",
                        "Implikasi Program",
                        "Catatan Etika",
                    ]
                    if not publish_mode:
                        profile_cols = ["Nama", "family_id", "Dusun", *profile_cols]
                    profile_display = df_profile[[c for c in profile_cols if c in df_profile.columns]].copy()
                    st.dataframe(
                        profile_display.style.format(
                            {
                                "Centrality terpilih": "{:.6f}",
                                "Degree Centrality": "{:.6f}",
                                "Betweenness Centrality": "{:.6f}",
                                "Closeness Centrality": "{:.6f}",
                                "Eigenvector Centrality": "{:.6f}",
                                "IKR Agregat": "{:.3f}",
                            }
                        ),
                        use_container_width=True,
                    )
                    anonymous_download_cols = [c for c in profile_cols if c not in {"Nama", "family_id", "Dusun"}]
                    anonymous_source_cols = list(dict.fromkeys(["family_id", "Nama", "Dusun", *anonymous_download_cols]))
                    anonymous_download_df = apply_privacy_view(
                        df_profile[[c for c in anonymous_source_cols if c in df_profile.columns]],
                        publish_mode=True,
                    )
                    anonymous_download_df = anonymous_download_df[
                        [c for c in anonymous_download_cols if c in anonymous_download_df.columns]
                    ].copy()
                    st.download_button(
                        "Unduh Tabel Anonim",
                        data=anonymous_download_df.to_csv(index=False).encode("utf-8"),
                        file_name=f"profil_node_strategis_anonim_{selected_centrality_key}.csv",
                        mime="text/csv",
                        key=f"download_centrality_profile_{selected_centrality_key}",
                    )

                    st.markdown("#### Komposisi Peran Struktural per Klaster dan Dusun")
                    role_cluster_df = (
                        df_centrality_view.groupby(["Klaster Louvain", "Peran Struktural"], as_index=False)
                        .size()
                        .rename(columns={"size": "Jumlah Node"})
                    )
                    role_dusun_col = "Dusun/Kode Dusun" if publish_mode else "Dusun"
                    role_dusun_df = (
                        df_centrality_view.groupby([role_dusun_col, "Peran Struktural"], as_index=False)
                        .size()
                        .rename(columns={"size": "Jumlah Node"})
                    )
                    rc1, rc2 = st.columns(2)
                    with rc1:
                        if not role_cluster_df.empty:
                            fig_role_cluster = px.bar(
                                role_cluster_df,
                                x="Klaster Louvain",
                                y="Jumlah Node",
                                color="Peran Struktural",
                                barmode="stack",
                                title="Komposisi Peran Struktural per Klaster",
                                color_discrete_map=CENTRALITY_ROLE_COLORS,
                                category_orders={"Peran Struktural": CENTRALITY_ROLE_ORDER},
                            )
                            style_publication_figure(
                                fig_role_cluster,
                                title="Komposisi Peran Struktural per Klaster",
                                height=430,
                                xaxis_title="Klaster Louvain",
                                yaxis_title="Jumlah node",
                                legend_title="Peran Struktural",
                            )
                            st.plotly_chart(fig_role_cluster, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                    with rc2:
                        if not role_dusun_df.empty:
                            fig_role_dusun = px.bar(
                                role_dusun_df,
                                x=role_dusun_col,
                                y="Jumlah Node",
                                color="Peran Struktural",
                                barmode="stack",
                                title="Komposisi Peran Struktural per Dusun",
                                color_discrete_map=CENTRALITY_ROLE_COLORS,
                                category_orders={"Peran Struktural": CENTRALITY_ROLE_ORDER},
                            )
                            style_publication_figure(
                                fig_role_dusun,
                                title="Komposisi Peran Struktural per Dusun",
                                height=max(430, min(760, 320 + (18 * role_dusun_df[role_dusun_col].nunique()))),
                                xaxis_title="Dusun/Kode Dusun",
                                yaxis_title="Jumlah node",
                                legend_title="Peran Struktural",
                            )
                            st.plotly_chart(fig_role_dusun, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                    st.markdown("#### Top 5 Centrality per Pilar (Filter Aktif)")
                    st.caption(
                        "Tabel ini menampilkan 5 node dengan skor tertinggi untuk setiap metrik centrality. "
                        "Gunakan ikon kamera pada kanan atas tabel untuk mengunduh PNG."
                    )
                    all_centrality_specs = [
                        ("Degree Centrality", "degree"),
                        ("Betweenness Centrality", "betweenness"),
                        ("Closeness Centrality", "closeness"),
                        ("Eigenvector Centrality", "eigenvector"),
                    ]
                    top_table_config = {
                        **PLOTLY_DRAW_CONFIG,
                        "toImageButtonOptions": {
                            "format": "png",
                            "filename": "top-centrality-table",
                            "height": 700,
                            "width": 1400,
                            "scale": 2,
                        },
                    }
                    df_centrality_all = df_centrality_view[
                        [
                            "family_id",
                            "Kode Node",
                            "Nama",
                            "Dusun",
                            "Dusun/Kode Dusun",
                            "Profesi/Pekerjaan",
                            "IKR Agregat",
                            "Degree Centrality",
                            "Betweenness Centrality",
                            "Closeness Centrality",
                            "Eigenvector Centrality",
                        ]
                    ].copy()
                    rename_metric_map = {}
                    for metric_label_all, metric_key_all in all_centrality_specs:
                        metric_vals_all = compute_centrality_on_similarity_graph(G_view, metric_key_all)
                        df_centrality_all[metric_label_all] = df_centrality_all["family_id"].map(
                            lambda nid, vals=metric_vals_all: float(vals.get(nid, 0.0))
                        )
                        rename_metric_map[metric_label_all] = f"Skor {metric_label_all}"

                    top_tabs = st.tabs([label.replace(" Centrality", "") for label, _ in all_centrality_specs])
                    for tab_obj, (metric_label_all, _) in zip(top_tabs, all_centrality_specs):
                        score_col = rename_metric_map[metric_label_all]
                        node_display_col = "Kode Node" if publish_mode else "Nama"
                        dusun_display_col = "Dusun/Kode Dusun" if publish_mode else "Dusun"
                        df_top_table = (
                            df_centrality_all[[node_display_col, dusun_display_col, "Profesi/Pekerjaan", "IKR Agregat", metric_label_all]]
                            .rename(columns={node_display_col: "Nama", dusun_display_col: "Dusun", "Profesi/Pekerjaan": "Pekerjaan", metric_label_all: score_col})
                            .sort_values(score_col, ascending=False)
                            .head(5)
                            .reset_index(drop=True)
                        )
                        with tab_obj:
                            fig_top_table = build_centrality_top_table_figure(
                                df_top_table,
                                title=f"Top 5 {metric_label_all}",
                                score_col=score_col,
                            )
                            if fig_top_table is not None:
                                st.plotly_chart(fig_top_table, use_container_width=True, config=top_table_config)

                    st.markdown(f"#### Top 10 (Filter Aktif): {centrality_name}")
                    st.dataframe(
                        df_centrality_view[display_cols].head(10).style.format({centrality_name: "{:.6f}", "IKR Agregat": "{:.3f}"}),
                        use_container_width=True,
                    )

                if not df_centrality_view.empty:
                    st.markdown("#### Analisis per Klaster dan per Dusun")
                    centrality_dusun_summary_col = "Dusun/Kode Dusun" if publish_mode else "Dusun"
                    c_tab1, c_tab2, c_tab3, c_tab4 = st.tabs(
                        ["Ringkasan Klaster", "Top 10 per Klaster", "Ringkasan Dusun", "Top 10 per Dusun"]
                    )
                    with c_tab1:
                        df_cluster_cent = (
                            df_centrality_view.groupby("Klaster Louvain", as_index=False)
                            .agg(
                                Jumlah_Node=("family_id", "count"),
                                Rerata_Centrality=(centrality_name, "mean"),
                                Maks_Centrality=(centrality_name, "max"),
                                Rerata_IKR_Agregat=("IKR Agregat", "mean"),
                            )
                            .sort_values("Rerata_Centrality", ascending=False)
                            .reset_index(drop=True)
                        )
                        st.dataframe(
                            df_cluster_cent.style.format(
                                {"Rerata_Centrality": "{:.6f}", "Maks_Centrality": "{:.6f}", "Rerata_IKR_Agregat": "{:.3f}"}
                            ),
                            use_container_width=True,
                        )
                    with c_tab2:
                        cluster_opts = sorted(df_centrality_view["Klaster Louvain"].dropna().unique().tolist())
                        selected_cluster_c = st.selectbox(
                            "Pilih Klaster",
                            options=cluster_opts,
                            key=f"centrality_cluster_{selected_centrality_key}",
                        )
                        st.dataframe(
                            df_centrality_view[df_centrality_view["Klaster Louvain"] == selected_cluster_c][display_cols]
                            .head(10)
                            .style.format({centrality_name: "{:.6f}", "IKR Agregat": "{:.3f}"}),
                            use_container_width=True,
                        )
                    with c_tab3:
                        df_dusun_cent = (
                            df_centrality_view.groupby(centrality_dusun_summary_col, as_index=False)
                            .agg(
                                Jumlah_Node=("family_id", "count"),
                                Rerata_Centrality=(centrality_name, "mean"),
                                Maks_Centrality=(centrality_name, "max"),
                                Rerata_IKR_Agregat=("IKR Agregat", "mean"),
                            )
                            .sort_values("Rerata_Centrality", ascending=False)
                            .reset_index(drop=True)
                        )
                        st.dataframe(
                            df_dusun_cent.style.format(
                                {"Rerata_Centrality": "{:.6f}", "Maks_Centrality": "{:.6f}", "Rerata_IKR_Agregat": "{:.3f}"}
                            ),
                            use_container_width=True,
                        )
                    with c_tab4:
                        dusun_opts = sorted(df_centrality_view[centrality_dusun_summary_col].fillna("Tidak Valid").astype(str).unique().tolist())
                        selected_dusun_c = st.selectbox(
                            "Pilih Dusun/Kode Dusun",
                            options=dusun_opts,
                            key=f"centrality_dusun_{selected_centrality_key}",
                        )
                        st.dataframe(
                            df_centrality_view[df_centrality_view[centrality_dusun_summary_col].astype(str) == str(selected_dusun_c)][display_cols]
                            .head(10)
                            .style.format({centrality_name: "{:.6f}", "IKR Agregat": "{:.3f}"}),
                            use_container_width=True,
                        )
            else:
                st.info("Nilai centrality belum bisa dihitung untuk graf saat ini.")

        with subbab_dropdown("Assortativity per Klaster Louvain", expanded=False):
            st.caption(
                "Perhitungan ini memakai subgraf tiap klaster (hanya node dan edge di dalam klaster tersebut), "
                "untuk melihat kekuatan homogenitas internal masing-masing klaster."
            )
            cluster_assort_rows = []
            for cid in cluster_ids_sorted:
                nodes_c = [n for n in node_ids if partition.get(n, -1) == cid]
                g_c = G.subgraph(nodes_c).copy()
                r_f_ikr_c = safe_numeric_assortativity(g_c, "f_ikr_dari_rekap_kk", default=0.0)
                r_fa_c = safe_numeric_assortativity(g_c, "f_a_dari_rekap_kk", default=0.0)
                r_fb_c = safe_numeric_assortativity(g_c, "f_b_dari_rekap_kk", default=0.0)
                r_fc_c = safe_numeric_assortativity(g_c, "f_c_dari_rekap_kk", default=0.0)
                r_fd_c = safe_numeric_assortativity(g_c, "f_d_dari_rekap_kk", default=0.0)
                r_fe_c = safe_numeric_assortativity(g_c, "f_e_dari_rekap_kk", default=0.0)
                r_dim_mean_c = float(np.nanmean([r_fa_c, r_fb_c, r_fc_c, r_fd_c, r_fe_c]))
                r_bansos_c = safe_attribute_assortativity(g_c, "bansos_num", default=0.0)
                r_internet_c = safe_attribute_assortativity(g_c, "internet_num", default=0.0)
                r_ponsel_c = safe_attribute_assortativity(g_c, "ponsel_num", default=0.0)
                r_spatial_c = safe_attribute_assortativity(g_c, col_spasial, default=0.0) if col_spasial in df_v.columns else np.nan
                dir_c, lvl_c = interpret_assortativity_value(r_f_ikr_c)
                cluster_assort_rows.append(
                    {
                        "Klaster": int(cid),
                        "Node": int(g_c.number_of_nodes()),
                        "Edge Internal": int(g_c.number_of_edges()),
                        "Density Internal": float(nx.density(g_c)) if g_c.number_of_nodes() > 1 else 0.0,
                        "r IKR Agregat": float(r_f_ikr_c),
                        "Arah IKR Agregat": dir_c,
                        "Kekuatan IKR Agregat": lvl_c,
                        "r Rata-rata Lima Dimensi": float(r_dim_mean_c),
                        "r Bansos": float(r_bansos_c),
                        "r Internet": float(r_internet_c),
                        "r Ponsel": float(r_ponsel_c),
                        "r Spasial": float(r_spatial_c) if pd.notnull(r_spatial_c) else np.nan,
                    }
                )
            df_cluster_assort = pd.DataFrame(cluster_assort_rows).sort_values("Klaster").reset_index(drop=True)
            st.dataframe(
                df_cluster_assort.style.format(
                    {
                        "Density Internal": "{:.4f}",
                        "r IKR Agregat": "{:.4f}",
                        "r Rata-rata Lima Dimensi": "{:.4f}",
                        "r Bansos": "{:.4f}",
                        "r Internet": "{:.4f}",
                        "r Ponsel": "{:.4f}",
                        "r Spasial": "{:.4f}",
                    }
                ),
                use_container_width=True,
            )
            fig_cluster_r = px.bar(
                df_cluster_assort,
                x="Klaster",
                y="r IKR Agregat",
                color="r IKR Agregat",
                color_continuous_scale="RdYlGn",
                range_color=[-1, 1],
                title="Perbandingan Assortativity IKR Agregat per Klaster Louvain",
                hover_data=["Node", "Edge Internal", "Density Internal", "Arah IKR Agregat", "Kekuatan IKR Agregat"],
            )
            fig_cluster_r.add_hline(y=0.0, line_dash="dash", line_color="#475569")
            fig_cluster_r.update_layout(template="plotly_white", xaxis_title="Klaster", yaxis_title="r IKR Agregat")
            st.plotly_chart(fig_cluster_r, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
            cluster_metric_long = df_cluster_assort.melt(
                id_vars=["Klaster"],
                value_vars=["r Rata-rata Lima Dimensi", "r Bansos", "r Internet", "r Ponsel", "r Spasial"],
                var_name="Metrik",
                value_name="Nilai r",
            ).dropna(subset=["Nilai r"])
            if not cluster_metric_long.empty:
                fig_cluster_metric = px.bar(
                    cluster_metric_long,
                    x="Klaster",
                    y="Nilai r",
                    color="Metrik",
                    barmode="group",
                    title="Ringkasan r per Klaster (Dimensi Rata-rata & Atribut Audit)",
                )
                fig_cluster_metric.add_hline(y=0.0, line_dash="dash", line_color="#475569")
                fig_cluster_metric.update_layout(template="plotly_white", xaxis_title="Klaster", yaxis_title="Nilai r")
                st.plotly_chart(fig_cluster_metric, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

        with subbab_dropdown("Profil Deskriptif Tiap Klaster Louvain", expanded=False):
            node_profile_rows = []
            for n in node_ids:
                n_attr = G.nodes[n]
                usia_raw = n_attr.get("usia", n_attr.get("usia (y)", n_attr.get("umur", np.nan)))
                profesi_raw = n_attr.get(
                    "profesi pekerjaan",
                    n_attr.get("profesi_pekerjaan", n_attr.get("pekerjaan", n_attr.get("profesi", "Tidak diketahui"))),
                )
                node_profile_rows.append(
                    {
                        "family_id": n,
                        "Klaster Louvain": int(partition.get(n, -1)),
                        "Usia": pd.to_numeric(pd.Series([usia_raw]), errors="coerce").iloc[0],
                        "Profesi/Pekerjaan": str(profesi_raw).strip() if pd.notnull(profesi_raw) else "Tidak diketahui",
                        "IKR Agregat": _safe_float_metric(n_attr.get("f_ikr_dari_rekap_kk"), default=np.nan),
                        "Weighted Degree": float(G.degree(n, weight="weight")),
                    }
                )
            df_cluster_profile = pd.DataFrame(node_profile_rows)

            ccp1, ccp2, ccp3 = st.columns(3)
            ccp1.metric("Jumlah Klaster Terbentuk", f"{int(df_cluster_profile['Klaster Louvain'].nunique())}")
            ccp2.metric("Node Terpetakan", f"{int(len(df_cluster_profile))}")
            usia_valid_n = int(df_cluster_profile["Usia"].notna().sum())
            ccp3.metric("Node dengan Data Usia", f"{usia_valid_n}")

            summary_cluster = (
                df_cluster_profile.groupby("Klaster Louvain", as_index=False)
                .agg(
                    Jumlah_Node=("family_id", "count"),
                    Rerata_Usia=("Usia", "mean"),
                    Median_Usia=("Usia", "median"),
                    Rerata_IKR_Agregat=("IKR Agregat", "mean"),
                    Rerata_Weighted_Degree=("Weighted Degree", "mean"),
                )
                .sort_values("Klaster Louvain")
                .reset_index(drop=True)
            )
            top_prof_series = (
                df_cluster_profile.groupby("Klaster Louvain")["Profesi/Pekerjaan"]
                .agg(lambda s: ", ".join(s.value_counts().head(3).index.astype(str).tolist()))
                .rename("Top 3 Profesi/Pekerjaan")
                .reset_index()
            )
            summary_cluster = summary_cluster.merge(top_prof_series, on="Klaster Louvain", how="left")

            st.dataframe(
                summary_cluster.style.format(
                    {
                        "Rerata_Usia": "{:.1f}",
                        "Median_Usia": "{:.1f}",
                        "Rerata_IKR_Agregat": "{:.2f}",
                        "Rerata_Weighted_Degree": "{:.2f}",
                    }
                ),
                use_container_width=True,
            )

            vis_c1, vis_c2 = st.columns(2)
            with vis_c1:
                df_age_plot = df_cluster_profile[df_cluster_profile["Usia"].notna()].copy()
                if not df_age_plot.empty:
                    fig_age = px.histogram(
                        df_age_plot,
                        x="Usia",
                        color="Klaster Louvain",
                        nbins=18,
                        barmode="overlay",
                        opacity=0.65,
                        title="Distribusi Usia per Klaster",
                    )
                    fig_age.update_layout(template="plotly_white", xaxis_title="Usia", yaxis_title="Frekuensi")
                    st.plotly_chart(fig_age, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                else:
                    st.info("Data usia belum tersedia untuk histogram klaster.")
            with vis_c2:
                prof_counts = (
                    df_cluster_profile.groupby(["Klaster Louvain", "Profesi/Pekerjaan"], as_index=False)
                    .size()
                    .rename(columns={"size": "Jumlah"})
                    .sort_values(["Klaster Louvain", "Jumlah"], ascending=[True, False])
                )
                if not prof_counts.empty:
                    top_prof_plot = prof_counts.groupby("Klaster Louvain").head(5).copy()
                    fig_prof = px.bar(
                        top_prof_plot,
                        x="Klaster Louvain",
                        y="Jumlah",
                        color="Profesi/Pekerjaan",
                        barmode="stack",
                        title="Top Profesi/Pekerjaan per Klaster (Top 5)",
                    )
                    fig_prof.update_layout(template="plotly_white", xaxis_title="Klaster", yaxis_title="Jumlah Node")
                    st.plotly_chart(fig_prof, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                else:
                    st.info("Data profesi/pekerjaan belum tersedia untuk visual klaster.")

        with subbab_dropdown("Visual Jaringan Dimensi Kesejahteraan", expanded=False):
            graph_dim_label, graph_dim_col = selected_graph_dim
            raw_dim_vals = [G.nodes[n].get(graph_dim_col) for n in node_ids]
            dim_num = pd.to_numeric(pd.Series(raw_dim_vals), errors="coerce")
            dim_marker_cmin = None
            dim_marker_cmax = None
            if graph_dim_col == IKR_OVERALL_METRIC[1]:
                # Untuk IKR agregat, gunakan nilai asli lalu discretize berbasis rentang data (bukan kategori BPS).
                valid_vals = dim_num.dropna()
                if valid_vals.nunique() <= 1:
                    dim_marker_vals = [0 for _ in node_ids]
                    dim_colorscale = [[0.0, "#2563eb"], [1.0, "#2563eb"]]
                    dim_colorbar = dict(title="Kelas IKR Agregat")
                    dim_hover_vals = [f"{x:.3f}" if pd.notnull(x) else "NA" for x in dim_num]
                    dim_marker_cmin = 0
                    dim_marker_cmax = 1
                else:
                    n_bins = int(min(5, valid_vals.nunique()))
                    vmin = float(valid_vals.min())
                    vmax = float(valid_vals.max())
                    bin_edges = np.linspace(vmin, vmax, n_bins + 1)
                    bins = pd.cut(dim_num, bins=bin_edges, include_lowest=True, duplicates="drop")
                    uniq_bins = [b for b in bins.cat.categories]
                    bin_labels = [f"{float(iv.left):.2f} - {float(iv.right):.2f}" for iv in uniq_bins]
                    bin_map = {b: i for i, b in enumerate(uniq_bins)}
                    invalid_idx = len(uniq_bins)
                    dim_marker_vals = [bin_map.get(b, invalid_idx) for b in bins]
                    palette_main = ["#d73027", "#fc8d59", "#fee08b", "#d9ef8b", "#1a9850"][:len(uniq_bins)]
                    dim_palette = palette_main + ["#64748b"]
                    dim_colorscale = build_discrete_colorscale(dim_palette)
                    dim_colorbar = dict(
                        title="Rentang IKR Agregat",
                        tickvals=list(range(len(uniq_bins))),
                        ticktext=bin_labels,
                    )
                    dim_hover_vals = [
                        (
                            f"{_safe_float_metric(dim_num.iloc[idx], default=np.nan):.3f}"
                            f" ({bin_labels[bin_map[bins.iloc[idx]]]}" + ")"
                            if pd.notnull(dim_num.iloc[idx]) and bins.iloc[idx] in bin_map else "NA"
                        )
                        for idx in range(len(node_ids))
                    ]
                    dim_marker_cmin = 0
                    dim_marker_cmax = max(len(uniq_bins), 1)
            elif dim_num.notna().sum() >= 3:
                dim_marker_vals = dim_num.tolist()
                dim_colorscale = "Blues"
                dim_colorbar = dict(title=graph_dim_label)
                dim_hover_vals = [f"{x:.3f}" if pd.notnull(x) else "NA" for x in dim_num]
            else:
                dim_cat = pd.Series(raw_dim_vals).fillna("Tidak Valid").astype(str)
                dim_uniqs = sorted(dim_cat.unique().tolist())
                dim_map = {v: i for i, v in enumerate(dim_uniqs)}
                dim_marker_vals = [dim_map[v] for v in dim_cat]
                dim_colorscale = [[0.0, "#0ea5e9"], [1.0, "#0ea5e9"]] if len(dim_uniqs) == 1 else [[i / (len(dim_uniqs) - 1), CONTRAST_COLORS[i % len(CONTRAST_COLORS)]] for i in range(len(dim_uniqs))]
                dim_colorbar = dict(
                    title=graph_dim_label,
                    tickvals=list(range(len(dim_uniqs))),
                    ticktext=dim_uniqs,
                )
                dim_hover_vals = dim_cat.tolist()

            dim_node_text = [
                (
                    f"Nama: {G.nodes[n].get('nama', '-')}"
                    f"<br>{graph_dim_label}: {dim_hover_vals[idx]}"
                    f"<br>IKR Agregat: {_safe_float_metric(G.nodes[n].get('f_ikr_dari_rekap_kk'), default=np.nan):.3f}"
                    f"<br>Klaster Louvain: {partition.get(n, -1)}"
                )
                for idx, n in enumerate(node_ids)
            ]
            fig_dim = go.Figure()
            add_network_edge_traces(
                fig_dim,
                visible_edges_focus,
                pos_focus,
                edge_min,
                edge_span,
                color_fn=edge_color_by_interaction,
                base_width=0.28,
                width_scale=0.78,
                hover=False,
            )
            fig_dim.add_trace(
                go.Scatter(
                    x=[pos_focus[n][0] for n in node_ids],
                    y=[pos_focus[n][1] for n in node_ids],
                    mode="markers",
                    marker=dict(
                        size=node_size_main,
                        color=dim_marker_vals,
                        colorscale=dim_colorscale,
                        cmin=dim_marker_cmin,
                        cmax=dim_marker_cmax,
                        showscale=True,
                        colorbar=dim_colorbar,
                        opacity=0.86,
                        line=dict(color=NETWORK_NODE_LINE, width=node_line_width),
                    ),
                    text=dim_node_text,
                    hoverinfo="text",
                    showlegend=False,
                )
            )
            style_network_figure(
                fig_dim,
                title=f"Jaringan Kemiripan Rumah Tangga Menurut {graph_dim_label}",
                height=690,
            )
            if graph_spatial_mode == "Layout Jaringan":
                st.plotly_chart(fig_dim, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
            else:
                fig_dim_spatial = build_spatial_node_figure(
                    G,
                    node_ids=node_ids,
                    node_color_vals=dim_marker_vals,
                    node_hover_text=dim_node_text,
                    title=f"Sebaran Spasial Rumah Tangga Menurut {graph_dim_label}",
                    spatial_mode=graph_spatial_mode,
                    marker_size=11,
                    colorscale=dim_colorscale,
                    colorbar=dim_colorbar,
                    cmin=dim_marker_cmin,
                    cmax=dim_marker_cmax,
                )
                if fig_dim_spatial is not None:
                    st.plotly_chart(fig_dim_spatial, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                else:
                    st.warning("Mode spasial aktif, tetapi kolom lat/lon belum valid. Ditampilkan mode layout jaringan.")
                    st.plotly_chart(fig_dim, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

            with subbab_dropdown("Graf Louvain dengan Warna Node IKR Agregat", expanded=False):
                ikr_vals = pd.to_numeric(
                    pd.Series([G.nodes[n].get("f_ikr_dari_rekap_kk") for n in node_ids]),
                    errors="coerce",
                )
                ikr_hover_vals = [f"{x:.3f}" if pd.notnull(x) else "NA" for x in ikr_vals]
                fig_ikr_focus = go.Figure()
                add_network_edge_traces(
                    fig_ikr_focus,
                    visible_edges_focus,
                    pos_focus,
                    edge_min,
                    edge_span,
                    color_fn=edge_color_by_interaction,
                    base_width=0.28,
                    width_scale=0.78,
                    hover=False,
                )
                marker_ikr = dict(
                    size=node_size_main,
                    line=dict(color=NETWORK_NODE_LINE, width=node_line_width),
                    opacity=0.86,
                    showscale=True,
                    colorbar=dict(title="IKR Agregat"),
                )
                if ikr_vals.notna().sum() >= 1:
                    valid_ikr = ikr_vals.dropna()
                    ikr_min = float(valid_ikr.min())
                    ikr_max = float(valid_ikr.max())
                    marker_ikr["color"] = ikr_vals.tolist()
                    marker_ikr["colorscale"] = "RdYlGn"
                    marker_ikr["cmin"] = ikr_min
                    marker_ikr["cmax"] = ikr_max
                    if ikr_max > ikr_min:
                        ikr_edges = np.linspace(ikr_min, ikr_max, 6)
                        ikr_mids = ((ikr_edges[:-1] + ikr_edges[1:]) / 2.0).tolist()
                        ikr_labels = [f"{ikr_edges[i]:.2f} - {ikr_edges[i+1]:.2f}" for i in range(len(ikr_edges) - 1)]
                        marker_ikr["colorbar"] = dict(
                            title="IKR Agregat<br>Rentang Nilai",
                            tickvals=ikr_mids,
                            ticktext=ikr_labels,
                        )
                    else:
                        marker_ikr["colorbar"] = dict(
                            title="IKR Agregat",
                            tickvals=[ikr_min],
                            ticktext=[f"{ikr_min:.2f}"],
                        )
                else:
                    marker_ikr["color"] = "#0ea5e9"
                    marker_ikr["showscale"] = False
                fig_ikr_focus.add_trace(
                    go.Scatter(
                        x=[pos_focus[n][0] for n in node_ids],
                        y=[pos_focus[n][1] for n in node_ids],
                        mode="markers",
                        marker=marker_ikr,
                        text=[
                            (
                                f"Nama: {G.nodes[n].get('nama', '-')}"
                                f"<br>IKR Agregat: {ikr_hover_vals[idx]}"
                                f"<br>{graph_dim_label}: {dim_hover_vals[idx]}"
                                f"<br>Klaster Louvain: {partition.get(n, -1)}"
                            )
                            for idx, n in enumerate(node_ids)
                        ],
                        hoverinfo="text",
                        showlegend=False,
                    )
                )
                style_network_figure(
                    fig_ikr_focus,
                    title="Jaringan Louvain dengan Pewarnaan IKR Agregat",
                    height=690,
                )
                ikr_node_text = [
                    (
                        f"Nama: {G.nodes[n].get('nama', '-')}"
                        f"<br>IKR Agregat: {ikr_hover_vals[idx]}"
                        f"<br>{graph_dim_label}: {dim_hover_vals[idx]}"
                        f"<br>Klaster Louvain: {partition.get(n, -1)}"
                    )
                    for idx, n in enumerate(node_ids)
                ]
                if graph_spatial_mode == "Layout Jaringan":
                    st.plotly_chart(fig_ikr_focus, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                else:
                    fig_ikr_spatial = build_spatial_node_figure(
                        G,
                        node_ids=node_ids,
                        node_color_vals=marker_ikr.get("color", [0.0 for _ in node_ids]) if isinstance(marker_ikr.get("color", None), list) else [0.0 for _ in node_ids],
                        node_hover_text=ikr_node_text,
                        title="Sebaran Spasial Louvain Berdasarkan IKR Agregat",
                        spatial_mode=graph_spatial_mode,
                        marker_size=11,
                        colorscale=marker_ikr.get("colorscale", "RdYlGn"),
                        colorbar=marker_ikr.get("colorbar", dict(title="IKR Agregat")),
                        cmin=marker_ikr.get("cmin"),
                        cmax=marker_ikr.get("cmax"),
                    )
                    if fig_ikr_spatial is not None:
                        st.plotly_chart(fig_ikr_spatial, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                    else:
                        st.warning("Mode spasial aktif, tetapi kolom lat/lon belum valid. Ditampilkan mode layout jaringan.")
                        st.plotly_chart(fig_ikr_focus, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

        with subbab_dropdown("Assortativity Numerik per Dimensi Kesejahteraan", expanded=False):
            st.caption(
                "Sesuai Newman (2003), nilai r tiap dimensi dihitung sebagai korelasi Pearson antar nilai atribut pada pasangan node yang terhubung (edge)."
            )
            st.caption(
                "Tujuan: meskipun graf dibangun dari gabungan 5 dimensi IKR, asortativitas dihitung per dimensi untuk melihat dimensi yang paling berkontribusi terhadap sekat sosial."
            )
            df_assort_ikr = build_ikr_assortativity_table(G, IKR_DIMENSION_MAP)
            if not df_assort_ikr.empty:
                summary_base_row = compute_base_five_dimension_summary(df_assort_ikr)
                df_assort_dims = (
                    df_assort_ikr[df_assort_ikr["Jenis"] == "Dimensi"]
                    .sort_values("Assortativity r", ascending=False)
                    .reset_index(drop=True)
                )
                df_assort_agg = (
                    df_assort_ikr[df_assort_ikr["Jenis"] == "Agregat"]
                    .sort_values("Assortativity r", ascending=False)
                    .reset_index(drop=True)
                )
                if summary_base_row:
                    df_assort_agg = pd.concat([df_assort_agg, pd.DataFrame([summary_base_row])], ignore_index=True)
                    df_assort_agg = df_assort_agg.sort_values("Assortativity r", ascending=False).reset_index(drop=True)

                top_ikr = df_assort_dims.iloc[0] if not df_assort_dims.empty else df_assort_agg.iloc[0]

                st.markdown("#### Assortativity per Dimensi Kesejahteraan")
                df_assort_dims_display = df_assort_dims.drop(columns=["Kolom Internal"], errors="ignore")
                st.dataframe(
                    df_assort_dims_display.style.background_gradient(cmap="RdYlGn", subset=["Assortativity r"]),
                    use_container_width=True,
                )
                fig_assort_ikr_dim = px.bar(
                    df_assort_dims,
                    x="Assortativity r",
                    y="Dimensi IKR",
                    orientation="h",
                    color="Assortativity r",
                    color_continuous_scale="RdYlGn",
                    range_color=[-1, 1],
                    hover_data=["Sumber Skor", "Arah", "Kekuatan"],
                    title="Perbandingan Assortativity per Dimensi Kesejahteraan",
                )
                fig_assort_ikr_dim.add_vline(x=0.0, line_dash="dash", line_color="#64748b")
                fig_assort_ikr_dim.update_layout(height=420, yaxis_title="", xaxis_title="Koefisien Assortativity (r)")
                st.plotly_chart(fig_assort_ikr_dim, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                with subbab_dropdown("IKR Agregat dan Ringkasan Lima Dimensi", expanded=False):
                    df_assort_agg_display = df_assort_agg.drop(columns=["Kolom Internal"], errors="ignore")
                    st.dataframe(
                        df_assort_agg_display.style.background_gradient(cmap="RdYlGn", subset=["Assortativity r"]),
                        use_container_width=True,
                    )
                    fig_assort_ikr_agg = px.bar(
                        df_assort_agg,
                        x="Assortativity r",
                        y="Dimensi IKR",
                        orientation="h",
                        color="Assortativity r",
                        color_continuous_scale="RdYlGn",
                        range_color=[-1, 1],
                        hover_data=["Sumber Skor", "Arah", "Kekuatan"],
                        title="Perbandingan IKR Agregat dan Ringkasan Lima Dimensi",
                    )
                    fig_assort_ikr_agg.add_vline(x=0.0, line_dash="dash", line_color="#64748b")
                    fig_assort_ikr_agg.update_layout(height=320, yaxis_title="", xaxis_title="Koefisien Assortativity (r)")
                    st.plotly_chart(fig_assort_ikr_agg, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                with subbab_dropdown("Drill-Down Analitik: Dimensi -> Variabel", expanded=False):
                    dim_cfg = DRILLDOWN_DIMENSIONS[selected_dim_key]
                    dim_label = dim_cfg["label"]
                    dim_col = dim_cfg["aggregate_col"]
                    var_list = dim_cfg["variables"]

                    if dim_col in df_v.columns:
                        r_dim, method_dim = compute_assortativity_for_column(G, dim_col)
                        dir_dim, lvl_dim = interpret_assortativity_value(r_dim)
                    else:
                        r_dim, method_dim, dir_dim, lvl_dim = np.nan, "n/a", "Tidak tersedia", "-"

                    c_dr1, c_dr2, c_dr3 = st.columns(3)
                    c_dr1.metric("r Dimensi Terpilih", f"{_safe_float_metric(r_dim, default=0.0):.4f}", f"{dir_dim} | {lvl_dim}")
                    c_dr2.metric("Jumlah Variabel Penyusun", f"{len(var_list)}")
                    c_dr3.metric("Metode Layer Dimensi", method_dim)
                    r_ikr_agg, _ = compute_assortativity_for_column(G, "f_ikr_dari_rekap_kk")
                    dir_ikr_agg, lvl_ikr_agg = interpret_assortativity_value(r_ikr_agg)
                    c_ag1, c_ag2 = st.columns(2)
                    c_ag1.metric("r IKR Agregat", f"{_safe_float_metric(r_ikr_agg, default=0.0):.4f}", f"{dir_ikr_agg} | {lvl_ikr_agg}")
                    if summary_base_row:
                        c_ag2.metric(
                            "r Gabungan 5 Dimensi",
                            f"{float(summary_base_row['Assortativity r']):.4f}",
                            f"{summary_base_row['Arah']} | {summary_base_row['Kekuatan']}",
                        )
                    else:
                        c_ag2.metric("r Gabungan 5 Dimensi", "NA", "-")
                    st.caption(
                        f"Layer Struktural: {dim_label} | metode={method_dim}. "
                        "Layer Investigatif: seluruh variabel penyusun dimensi ditampilkan otomatis tanpa pemilihan variabel manual."
                    )
                    drill_rows = []
                    resolved_for_plot = []
                    for vcfg in var_list:
                        vcol = resolve_first_existing_column(df_v.columns, vcfg["candidates"])
                        if not vcol:
                            drill_rows.append(
                                {
                                    "Kode": vcfg["code"],
                                    "Variabel": vcfg["label"],
                                    "Kolom": "Tidak ditemukan",
                                    "r": np.nan,
                                    "Arah": "Tidak tersedia",
                                    "Kekuatan": "-",
                                    "Metode": "n/a",
                                    "Keterangan": vcfg["description"],
                                }
                            )
                            continue
                        r_v, m_v = compute_assortativity_for_column(G, vcol)
                        d_v, l_v = interpret_assortativity_value(r_v)
                        drill_rows.append(
                            {
                                "Kode": vcfg["code"],
                                "Variabel": vcfg["label"],
                                "Kolom": vcol,
                                "r": float(r_v),
                                "Arah": d_v,
                                "Kekuatan": l_v,
                                "Metode": m_v,
                                "Keterangan": vcfg["description"],
                            }
                        )
                        resolved_for_plot.append((vcfg, vcol, float(r_v)))

                    df_drill = pd.DataFrame(drill_rows)
                    st.dataframe(df_drill.style.background_gradient(cmap="RdYlGn", subset=["r"]), use_container_width=True)
                    if df_drill["r"].notna().any():
                        fig_drill_bar = px.bar(
                            df_drill[df_drill["r"].notna()].sort_values("r", ascending=False),
                            x="r",
                            y="Variabel",
                            orientation="h",
                            color="r",
                            color_continuous_scale="RdYlGn",
                            range_color=[-1, 1],
                            hover_data=["Kode", "Kolom", "Arah", "Kekuatan", "Metode"],
                            title=f"Perbandingan Assortativity Variabel Penyusun - {dim_label}",
                        )
                        fig_drill_bar.add_vline(x=0.0, line_dash="dash", line_color="#64748b")
                        fig_drill_bar.update_layout(height=420, yaxis_title="", xaxis_title="Koefisien Assortativity (r)")
                        st.plotly_chart(fig_drill_bar, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                    missing_vars = df_drill[df_drill["Kolom"] == "Tidak ditemukan"]["Kode"].tolist()
                    if missing_vars:
                        st.warning(f"Kolom belum terdeteksi untuk variabel: {', '.join(missing_vars)}")

                        with subbab_dropdown("Graf Pendukung per Variabel", expanded=False):
                            for vcfg, vcol, r_v in resolved_for_plot:
                                fig_var = go.Figure()
                                add_network_edge_traces(
                                    fig_var,
                                    visible_edges_focus,
                                    pos_focus,
                                    edge_min,
                                    edge_span,
                                    color_fn=edge_color_by_interaction,
                                    base_width=0.26,
                                    width_scale=0.72,
                                    hover=False,
                                )
                            raw_var_vals = [G.nodes[n].get(vcol) for n in node_ids]
                            num_var = pd.to_numeric(pd.Series(raw_var_vals), errors="coerce")
                            marker_cmin = None
                            marker_cmax = None
                            if num_var.notna().sum() >= 3:
                                marker_vals = num_var.tolist()
                                marker_scale = "RdYlGn"
                                marker_cbar = dict(title=vcfg["code"])
                                hover_vals = [f"{x:.3f}" if pd.notnull(x) else "NA" for x in num_var]
                                marker_cmin = float(num_var.min()) if num_var.notna().sum() > 0 else None
                                marker_cmax = float(num_var.max()) if num_var.notna().sum() > 0 else None
                            else:
                                cat_vals = pd.Series(raw_var_vals).fillna("Tidak Valid").astype(str)
                                uniq = sorted(cat_vals.unique().tolist())
                                cat_map = {v: i for i, v in enumerate(uniq)}
                                marker_vals = [cat_map[v] for v in cat_vals]
                                marker_scale = [[0.0, "#0ea5e9"], [1.0, "#0ea5e9"]] if len(uniq) == 1 else [[i / (len(uniq) - 1), CONTRAST_COLORS[i % len(CONTRAST_COLORS)]] for i in range(len(uniq))]
                                marker_cbar = dict(title=vcfg["code"], tickvals=list(range(len(uniq))), ticktext=uniq)
                                hover_vals = cat_vals.tolist()
                                marker_cmin = 0
                                marker_cmax = max(len(uniq) - 1, 1)
                                fig_var.add_trace(
                                    go.Scatter(
                                        x=[pos_focus[n][0] for n in node_ids],
                                        y=[pos_focus[n][1] for n in node_ids],
                                        mode="markers",
                                        marker=dict(
                                            size=node_size_main,
                                            color=marker_vals,
                                            colorscale=marker_scale,
                                            showscale=True,
                                            colorbar=marker_cbar,
                                            opacity=0.86,
                                            line=dict(color=NETWORK_NODE_LINE, width=node_line_width),
                                        ),
                                        text=[f"{G.nodes[n].get('nama','-')}<br>{vcfg['label']}: {hover_vals[idx]}<br>Klaster: {partition.get(n, -1)}" for idx, n in enumerate(node_ids)],
                                        hoverinfo="text",
                                        showlegend=False,
                                )
                            )
                                style_network_figure(
                                    fig_var,
                                    title=f"Jaringan Variabel {vcfg['label']} | r={r_v:.4f}",
                                    height=660,
                                )
                            var_hover_text = [
                                f"{G.nodes[n].get('nama','-')}<br>{vcfg['label']}: {hover_vals[idx]}<br>Klaster: {partition.get(n, -1)}"
                                for idx, n in enumerate(node_ids)
                            ]
                            if graph_spatial_mode == "Layout Jaringan":
                                st.plotly_chart(fig_var, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                            else:
                                fig_var_spatial = build_spatial_node_figure(
                                    G,
                                    node_ids=node_ids,
                                    node_color_vals=marker_vals,
                                    node_hover_text=var_hover_text,
                                    title=f"Sebaran Spasial Variabel {vcfg['label']} | r={r_v:.4f}",
                                    spatial_mode=graph_spatial_mode,
                                    marker_size=11,
                                    colorscale=marker_scale,
                                    cmin=marker_cmin,
                                    cmax=marker_cmax,
                                    colorbar=marker_cbar,
                                )
                                if fig_var_spatial is not None:
                                    st.plotly_chart(fig_var_spatial, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                                else:
                                    st.warning("Mode spasial aktif, tetapi kolom lat/lon belum valid. Ditampilkan mode layout jaringan.")
                                    st.plotly_chart(fig_var, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                    glossary_rows = []
                    for _, dcfg in DRILLDOWN_DIMENSIONS.items():
                        for vcfg in dcfg["variables"]:
                            glossary_rows.append(
                                {
                                    "Dimensi": dcfg["label"],
                                    "Variabel": vcfg["label"],
                                    "Deskripsi": vcfg["description"],
                                }
                            )
                    with st.expander("Kamus Variabel Penyusun Dimensi", expanded=False):
                        st.dataframe(pd.DataFrame(glossary_rows), use_container_width=True)
                        st.caption(
                            "Metode Assortativity dihitung berdasarkan Newman (2002) dan konteks segregasi merujuk pada Montes et al. (2018)."
                        )
                    st.markdown(
                        f"<div class='soft-card'><b>Interpretasi Dominan:</b><br>"
                        f"Dimensi dengan nilai r tertinggi saat ini adalah <b>{top_ikr['Dimensi IKR']}</b> "
                        f"({top_ikr['Sumber Skor']}) dengan r = <b>{float(top_ikr['Assortativity r']):.4f}</b> "
                        f"({top_ikr['Arah']} | {top_ikr['Kekuatan']}). "
                        f"Ini menunjukkan dimensi tersebut paling kuat berkontribusi pada pola sekat sosial dalam jaringan desa terpilih."
                        f"</div>",
                        unsafe_allow_html=True,
                    )
            with subbab_dropdown("Audit Kebijakan: Assortativity Variabel Biner (Newman, 2003)", expanded=False):
                st.caption(
                    "Base graph tetap dibentuk dari lima dimensi kesejahteraan. Variabel Bansos, Internet, Ponsel, dan Spasial (dusun) hanya dipakai pada tahap audit assortativity atribut."
                )
                st.caption(
                    "Variabel dusun tidak diikutkan ke pembobotan graf, sehingga hasil audit spasial tetap objektif terhadap struktur graf dasar."
                )
                dusun_attr = "dusun" if "dusun" in df_v.columns else col_spasial
                dusun_codes = (
                    pd.Series([G.nodes[n].get(dusun_attr) for n in G.nodes()])
                    .fillna("Tidak Valid")
                    .astype("category")
                    .cat.codes
                    .tolist()
                )
                dusun_code_attr = "__audit_dusun_code"
                nx.set_node_attributes(G, {n: int(dusun_codes[idx]) for idx, n in enumerate(list(G.nodes()))}, dusun_code_attr)

                audit_specs = [
                    {"metric": "Assortativity Bansos", "col": "bansos_num", "kind": "binary"},
                    {"metric": "Assortativity Internet", "col": "internet_num", "kind": "binary"},
                    {"metric": "Assortativity Ponsel", "col": "ponsel_num", "kind": "binary"},
                    {"metric": f"Assortativity Spasial ({dusun_attr})", "col": dusun_attr, "kind": "categorical", "code_col": dusun_code_attr},
                ]

                biner_rows = []
                for spec in audit_specs:
                    r_attr = safe_attribute_assortativity(G, spec["col"], default=0.0)
                    direction_attr, strength_attr = interpret_assortativity_value(r_attr)
                    q_source_col = spec["col"] if spec["kind"] == "binary" else spec["code_col"]
                    montes_attr = compute_montes_within_between_assortativity(
                        G,
                        category_attr=q_source_col,
                        group_attr="cluster",
                        invalid_category_values=None,
                    )
                    biner_rows.append(
                        {
                            "Metrik": spec["metric"],
                            "Kolom": spec["col"],
                            "r": float(r_attr),
                            "Qw*": float(montes_attr["q_w_star"]),
                            "Qb*": float(montes_attr["q_b_star"]),
                            "Arah": direction_attr,
                            "Kekuatan": strength_attr,
                            "Label Steinley": steinley_segregation_label(r_attr),
                        }
                    )
                df_assort_biner = pd.DataFrame(biner_rows)
                fig_biner = px.bar(
                    df_assort_biner,
                    x="Metrik",
                    y="r",
                    color="r",
                    color_continuous_scale="RdYlGn",
                    range_color=[-1, 1],
                    title="Perbandingan Assortativity Audit Kebijakan & Spasial",
                    hover_data=["Kolom", "Qw*", "Qb*", "Arah", "Kekuatan", "Label Steinley"],
                )
                fig_biner.add_hline(y=0.0, line_dash="dash", line_color="#475569")
                fig_biner.update_traces(marker_line_color="#111827", marker_line_width=0.45)
                style_publication_figure(
                    fig_biner,
                    title="Perbandingan Assortativity Audit Kebijakan & Spasial",
                    height=420,
                    xaxis_title="",
                    yaxis_title="Koefisien Assortativity (r)",
                    showlegend=False,
                )
                st.plotly_chart(fig_biner, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                st.dataframe(
                    df_assort_biner.style.background_gradient(cmap="RdYlGn", subset=["r"]),
                    use_container_width=True,
                )
                with subbab_dropdown("Sebaran Dimensi dan IKR Agregat", expanded=False):
                    st.caption(
                        "Distribusi ini ditempatkan di bawah audit kebijakan karena dipakai untuk membaca konteks bansos/spasial terhadap kondisi dimensi dan IKR agregat desa."
                    )
                    df_graph_dims = pd.DataFrame(
                        [
                            {
                                "family_id": n,
                                "Klaster Louvain": int(partition.get(n, -1)),
                                "Sandang, Pangan, dan Papan": _safe_float_metric(G.nodes[n].get("f_a_dari_rekap_kk"), default=np.nan),
                                "Pendidikan": _safe_float_metric(G.nodes[n].get("f_b_dari_rekap_kk"), default=np.nan),
                                "Sosial, Hukum, dan HAM": _safe_float_metric(G.nodes[n].get("f_c_dari_rekap_kk"), default=np.nan),
                                "Kesehatan dan Pekerjaan": _safe_float_metric(G.nodes[n].get("f_d_dari_rekap_kk"), default=np.nan),
                                "Lingkungan dan Infrastruktur": _safe_float_metric(G.nodes[n].get("f_e_dari_rekap_kk"), default=np.nan),
                                "IKR Agregat": _safe_float_metric(G.nodes[n].get("f_ikr_dari_rekap_kk"), default=np.nan),
                            }
                            for n in node_ids
                        ]
                    )
                    dim_long = df_graph_dims.melt(
                        id_vars=["family_id", "Klaster Louvain"],
                        value_vars=list(PSEUDO_DIMENSION_COLS),
                        var_name="Dimensi",
                        value_name="Skor",
                    ).dropna(subset=["Skor"])

                    dist_tab1, dist_tab2, dist_tab3 = st.tabs(
                        ["Distribusi Lima Dimensi", "Per Dimensi per Klaster", "IKR Agregat Database"]
                    )
                    with dist_tab1:
                        if not dim_long.empty:
                            fig_dim_hist = px.histogram(
                                dim_long,
                                x="Skor",
                                color="Dimensi",
                                nbins=22,
                                barmode="overlay",
                                opacity=0.62,
                                title="Histogram Sebaran Skor Lima Dimensi (Node Graf)",
                            )
                            fig_dim_hist.update_layout(template="plotly_white", xaxis_title="Skor", yaxis_title="Frekuensi")
                            st.plotly_chart(fig_dim_hist, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                            fig_dim_box = px.box(
                                dim_long,
                                x="Dimensi",
                                y="Skor",
                                color="Dimensi",
                                title="Ringkasan Sebaran Lima Dimensi",
                            )
                            fig_dim_box.update_layout(template="plotly_white")
                            st.plotly_chart(fig_dim_box, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                            dim_summary = (
                                dim_long.groupby("Dimensi", as_index=False)
                                .agg(
                                    N=("Skor", "count"),
                                    Mean=("Skor", "mean"),
                                    Median=("Skor", "median"),
                                    Min=("Skor", "min"),
                                    Max=("Skor", "max"),
                                    Std=("Skor", "std"),
                                )
                                .sort_values("Dimensi")
                                .reset_index(drop=True)
                            )
                            st.dataframe(
                                dim_summary.style.format(
                                    {"Mean": "{:.2f}", "Median": "{:.2f}", "Min": "{:.2f}", "Max": "{:.2f}", "Std": "{:.2f}"}
                                ),
                                use_container_width=True,
                            )
                        else:
                            st.info("Data dimensi node graf belum cukup untuk visual distribusi.")

                    with dist_tab2:
                        if not dim_long.empty:
                            fig_cluster_violin = px.violin(
                                dim_long,
                                x="Dimensi",
                                y="Skor",
                                color="Klaster Louvain",
                                box=True,
                                points="outliers",
                                title="Sebaran Tiap Dimensi Menurut Klaster Louvain",
                            )
                            fig_cluster_violin.update_layout(template="plotly_white")
                            st.plotly_chart(fig_cluster_violin, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                        else:
                            st.info("Data per klaster untuk dimensi belum cukup.")

                    with dist_tab3:
                        df_db_ikr = df_v.copy()
                        if "f_ikr_dari_rekap_kk" in df_db_ikr.columns:
                            ikr_series = pd.to_numeric(df_db_ikr["f_ikr_dari_rekap_kk"], errors="coerce").dropna()
                            if not ikr_series.empty:
                                d1, d2, d3, d4 = st.columns(4)
                                d1.metric("N Data IKR Agregat", f"{int(ikr_series.shape[0])}")
                                d2.metric("Mean IKR Agregat", f"{float(ikr_series.mean()):.2f}")
                                d3.metric("Median IKR Agregat", f"{float(ikr_series.median()):.2f}")
                                d4.metric("Std IKR Agregat", f"{float(ikr_series.std()):.2f}")

                                fig_ikr_hist = px.histogram(
                                    x=ikr_series,
                                    nbins=24,
                                    title="Histogram IKR Agregat Keseluruhan (Database Desa Terpilih)",
                                    labels={"x": "IKR Agregat"},
                                )
                                fig_ikr_hist.add_vline(x=float(ikr_series.mean()), line_dash="dash", line_color="#1d4ed8", annotation_text="Mean")
                                fig_ikr_hist.add_vline(x=float(ikr_series.median()), line_dash="dot", line_color="#0f766e", annotation_text="Median")
                                fig_ikr_hist.update_layout(template="plotly_white", yaxis_title="Frekuensi")
                                st.plotly_chart(fig_ikr_hist, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                                cat_df = add_bps_ikr_category(df_db_ikr, ikr_col="f_ikr_dari_rekap_kk")
                                cat_order = ordered_existing_categories(cat_df["kategori_ikr"], BPS_CATEGORY_ORDER)
                                cat_count = (
                                    cat_df["kategori_ikr"]
                                    .value_counts()
                                    .reindex(cat_order, fill_value=0)
                                    .rename_axis("Kategori BPS")
                                    .reset_index(name="Jumlah")
                                )
                                if cat_count.empty:
                                    st.info("Kategori BPS valid belum tersedia pada data desa terpilih.")
                                else:
                                    fig_cat = px.bar(
                                        cat_count,
                                        x="Kategori BPS",
                                        y="Jumlah",
                                        color="Kategori BPS",
                                        color_discrete_map=BPS_CATEGORY_COLORS,
                                        title="Komposisi Kategori BPS dari IKR Agregat Database",
                                    )
                                    fig_cat.update_layout(template="plotly_white", showlegend=False)
                                    st.plotly_chart(fig_cat, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                                    st.dataframe(cat_count, use_container_width=True)

                                    top_cat_row = cat_count.iloc[cat_count["Jumlah"].idxmax()]
                                    st.markdown(
                                        f"<div class='soft-card'><b>Interpretasi IKR Agregat Database:</b><br>"
                                        f"Distribusi IKR agregat desa ini memiliki rerata <b>{float(ikr_series.mean()):.2f}</b> "
                                        f"dan median <b>{float(ikr_series.median()):.2f}</b>. "
                                        f"Kategori BPS paling dominan adalah <b>{top_cat_row['Kategori BPS']}</b> "
                                        f"dengan jumlah <b>{int(top_cat_row['Jumlah'])}</b> rumah tangga.</div>",
                                        unsafe_allow_html=True,
                                    )
                            else:
                                st.info("Kolom IKR agregat tersedia, tetapi nilainya belum valid untuk distribusi.")
                        else:
                            st.info("Kolom IKR agregat tidak ditemukan di database desa terpilih.")
            with subbab_dropdown("Visualisasi Jaringan Audit Kebijakan", expanded=False):
                raw_bansos_col = resolve_first_existing_column(df_v.columns, ["bansos", "keikutsertaan program bantuan"])
                raw_media_col = resolve_first_existing_column(df_v.columns, ["media informasi", "media_informasi", "wifi", "medsos"])
                raw_ponsel_col = resolve_first_existing_column(df_v.columns, ["kepemilikan ponsel", "kepemilikan_ponsel", "ponsel", "hp"])
                audit_graph_specs = [
                    {
                        "title": "Jaringan Audit Bantuan Sosial",
                        "col": "bansos_num",
                        "label": "Status Bansos",
                        "yes_label": "Penerima Bantuan",
                        "no_label": "Tidak Menerima Bantuan",
                        "raw_col": raw_bansos_col,
                    },
                    {
                        "title": "Jaringan Audit Akses Internet dan Media Informasi",
                        "col": "internet_num",
                        "label": "Akses Internet/Media Informasi",
                        "yes_label": "Memiliki Akses Informasi",
                        "no_label": "Tidak Memiliki Akses Informasi",
                        "raw_col": raw_media_col,
                    },
                    {
                        "title": "Jaringan Audit Kepemilikan Ponsel",
                        "col": "ponsel_num",
                        "label": "Kepemilikan Ponsel",
                        "yes_label": "Memiliki Ponsel",
                        "no_label": "Tidak Memiliki Ponsel",
                        "raw_col": raw_ponsel_col,
                    },
                    {
                        "title": f"Jaringan Audit Spasial ({dusun_attr})",
                        "col": dusun_attr,
                        "label": f"Wilayah {dusun_attr}",
                        "kind": "categorical",
                        "raw_col": dusun_attr,
                    },
                ]
                tabs_audit = st.tabs([s["title"] for s in audit_graph_specs])
                for idx_tab, spec in enumerate(audit_graph_specs):
                    with tabs_audit[idx_tab]:
                        row_m = df_assort_biner[df_assort_biner["Kolom"] == spec["col"]]
                        if row_m.empty and spec.get("kind") == "categorical":
                            row_m = df_assort_biner[df_assort_biner["Kolom"] == dusun_attr]
                        if not row_m.empty:
                            m1, m2, m3 = st.columns(3)
                            m1.metric("r", f"{float(row_m.iloc[0]['r']):.4f}")
                            m2.metric("Qw*", f"{float(row_m.iloc[0]['Qw*']):.4f}")
                            m3.metric("Qb*", f"{float(row_m.iloc[0]['Qb*']):.4f}")
                        fig_audit_graph = go.Figure()
                        add_network_edge_traces(
                            fig_audit_graph,
                            visible_edges_focus,
                            pos_focus,
                            edge_min,
                            edge_span,
                            color_fn=edge_color_by_interaction,
                            base_width=0.26,
                            width_scale=0.72,
                            hover=False,
                        )
                        if spec.get("kind") == "categorical":
                            cat_vals = pd.Series([G.nodes[n].get(spec["col"]) for n in node_ids]).fillna("Tidak Valid").astype(str)
                            if spec["col"] == dusun_attr and graph_spatial_mode != "Layout Jaringan":
                                node_color_vals = [cid_to_idx.get(partition.get(n, -1), 0) for n in node_ids]
                                node_colorscale = cluster_colorscale
                                colorbar_cfg = dict(
                                    title="Klaster Louvain",
                                    tickvals=list(range(len(cluster_ids_sorted))),
                                    ticktext=[f"Klaster {cid}" for cid in cluster_ids_sorted],
                                )
                            else:
                                uniq = sorted(cat_vals.unique().tolist())
                                cmap = {v: i for i, v in enumerate(uniq)}
                                node_color_vals = [cmap[v] for v in cat_vals]
                                node_colorscale = [[0.0, "#0ea5e9"], [1.0, "#0ea5e9"]] if len(uniq) == 1 else [[i / (len(uniq) - 1), CONTRAST_COLORS[i % len(CONTRAST_COLORS)]] for i in range(len(uniq))]
                                colorbar_cfg = dict(
                                    title=spec["label"],
                                    tickvals=list(range(len(uniq))),
                                    ticktext=uniq,
                                )
                            state_text = cat_vals.tolist()
                        else:
                            bin_vals = [int(_safe_float_metric(G.nodes[n].get(spec["col"]), default=0.0) > 0) for n in node_ids]
                            node_color_vals = bin_vals
                            node_colorscale = [[0.0, DDP_RED], [1.0, BINARY_COLOR_MAP["YA"]]]
                            colorbar_cfg = dict(
                                title=spec["label"],
                                tickvals=[0, 1],
                                ticktext=[spec["no_label"], spec["yes_label"]],
                            )
                            state_text = [spec["yes_label"] if v == 1 else spec["no_label"] for v in bin_vals]
                        fig_audit_graph.add_trace(
                            go.Scatter(
                                x=[pos_focus[n][0] for n in node_ids],
                                y=[pos_focus[n][1] for n in node_ids],
                                mode="markers",
                                    marker=dict(
                                        size=node_size_main,
                                        color=node_color_vals,
                                        colorscale=node_colorscale,
                                        cmin=0,
                                        cmax=1 if spec.get("kind") != "categorical" else max(len(set(node_color_vals)) - 1, 1),
                                        showscale=True,
                                        colorbar=colorbar_cfg,
                                        opacity=0.86,
                                        line=dict(color=NETWORK_NODE_LINE, width=node_line_width),
                                    ),
                                text=[
                                    (
                                        f"Nama: {G.nodes[n].get('nama', '-')}"
                                        f"<br>{graph_dim_label}: {_safe_float_metric(G.nodes[n].get(graph_dim_col), default=np.nan):.3f}"
                                        f"<br>IKR Agregat: {_safe_float_metric(G.nodes[n].get('f_ikr_dari_rekap_kk'), default=np.nan):.3f}"
                                        f"<br>{spec['label']}: {state_text[i]}"
                                        f"<br>Jenis/Detail: {G.nodes[n].get(spec['raw_col'], '-') if spec['raw_col'] else '-'}"
                                    )
                                    for i, n in enumerate(node_ids)
                                ],
                                hoverinfo="text",
                                showlegend=False,
                            )
                        )
                        audit_node_text = [
                            (
                                f"Nama: {G.nodes[n].get('nama', '-')}"
                                f"<br>{graph_dim_label}: {_safe_float_metric(G.nodes[n].get(graph_dim_col), default=np.nan):.3f}"
                                f"<br>IKR Agregat: {_safe_float_metric(G.nodes[n].get('f_ikr_dari_rekap_kk'), default=np.nan):.3f}"
                                f"<br>{spec['label']}: {state_text[i]}"
                                f"<br>Jenis/Detail: {G.nodes[n].get(spec['raw_col'], '-') if spec['raw_col'] else '-'}"
                            )
                            for i, n in enumerate(node_ids)
                        ]
                        style_network_figure(fig_audit_graph, title=spec["title"], height=660)
                        if graph_spatial_mode == "Layout Jaringan":
                            st.plotly_chart(fig_audit_graph, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                        else:
                            fig_audit_spatial = build_spatial_node_figure(
                                G,
                                node_ids=node_ids,
                                node_color_vals=node_color_vals,
                                node_hover_text=audit_node_text,
                                title=f"{spec['title']} (Sebaran Spasial Node)",
                                spatial_mode=graph_spatial_mode,
                                marker_size=11,
                                colorscale=node_colorscale,
                                cmin=0,
                                cmax=1 if spec.get("kind") != "categorical" else max(len(set(node_color_vals)) - 1, 1),
                                colorbar=colorbar_cfg,
                            )
                            if fig_audit_spatial is not None:
                                st.plotly_chart(fig_audit_spatial, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                            else:
                                st.warning("Mode spasial aktif, tetapi kolom lat/lon belum valid. Ditampilkan mode layout jaringan.")
                                st.plotly_chart(fig_audit_graph, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                        if spec.get("kind") == "categorical" and spec["col"] == dusun_attr:
                            st.markdown("##### Komposisi Klaster per Dusun (Mengacu Graf Base Louvain)")
                            if "df_dusun_cluster_wide" in locals() and not df_dusun_cluster_wide.empty:
                                pct_cols_tab = [c for c in df_dusun_cluster_wide.columns if c.endswith("Persentase (%)")]
                                fmt_cols_tab = {c: "{:.1f}" for c in pct_cols_tab}
                                count_cols_tab = [c for c in df_dusun_cluster_wide.columns if c.endswith("Jumlah KK") or c == "Total KK Dusun"]
                                fmt_cols_tab.update({c: "{:,.0f}" for c in count_cols_tab})
                                st.dataframe(
                                    df_dusun_cluster_wide.style.format(fmt_cols_tab).background_gradient(cmap="YlGnBu", subset=pct_cols_tab),
                                    use_container_width=True,
                                )
                            else:
                                st.info("Tabel proporsi klaster per dusun belum tersedia untuk graf aktif.")

            with subbab_dropdown("Rincian Persentase Keterhubungan Audit Biner", expanded=False):
                st.caption(
                    "Bagian ini memecah nilai audit bansos, internet, dan ponsel menjadi pasangan `YA-YA`, `YA-TIDAK`, dan `TIDAK-TIDAK`, "
                    "lalu dipisah ke ruang `Within` dan `Between` agar nilai r, Qw*, dan Qb* lebih mudah dijelaskan."
                )
                binary_breakdown_specs = [
                    {
                        "title": "Bansos",
                        "col": "bansos_num",
                        "yes_label": "Penerima",
                        "no_label": "Non-Penerima",
                    },
                    {
                        "title": "Internet",
                        "col": "internet_num",
                        "yes_label": "Punya Akses",
                        "no_label": "Tidak Punya Akses",
                    },
                    {
                        "title": "Ponsel",
                        "col": "ponsel_num",
                        "yes_label": "Punya Ponsel",
                        "no_label": "Tidak Punya Ponsel",
                    },
                ]
                tabs_binary_detail = st.tabs([f"Rincian {spec['title']}" for spec in binary_breakdown_specs])
                for spec, tab in zip(binary_breakdown_specs, tabs_binary_detail):
                    with tab:
                        _, df_bin_summary, df_bin_matrix = build_labeled_attribute_connection_breakdown(
                            G,
                            attr_name=spec["col"],
                            value_map={
                                1: spec["yes_label"],
                                0: spec["no_label"],
                                "1": spec["yes_label"],
                                "0": spec["no_label"],
                                "1.0": spec["yes_label"],
                                "0.0": spec["no_label"],
                            },
                            group_attr="cluster",
                            category_order=[spec["yes_label"], spec["no_label"], "Tidak Valid"],
                            invalid_label="Tidak Valid",
                        )
                        if df_bin_summary.empty:
                            st.info(f"Belum ada edge yang cukup untuk audit rinci {spec['title']}.")
                            continue

                        same_share_bin = (
                            df_bin_summary[df_bin_summary["Jenis Pasangan"] == "Sama"]
                            .groupby("Ruang")["Persentase Bobot (%)"]
                            .sum()
                            .to_dict()
                        )
                        top_within_bin = (
                            df_bin_summary[df_bin_summary["Ruang"] == "Within"]
                            .sort_values("Persentase Bobot (%)", ascending=False)
                            .head(1)
                        )
                        top_between_bin = (
                            df_bin_summary[df_bin_summary["Ruang"] == "Between"]
                            .sort_values("Persentase Bobot (%)", ascending=False)
                            .head(1)
                        )
                        c_bd_1, c_bd_2, c_bd_3, c_bd_4 = st.columns(4)
                        c_bd_1.metric("Share Sama Within", f"{float(same_share_bin.get('Within', 0.0)):.2f}%")
                        c_bd_2.metric("Share Sama Between", f"{float(same_share_bin.get('Between', 0.0)):.2f}%")
                        c_bd_3.metric(
                            "Dominan Within",
                            top_within_bin.iloc[0]["Pasangan"] if not top_within_bin.empty else "-",
                            f"{float(top_within_bin.iloc[0]['Persentase Bobot (%)']):.2f}%" if not top_within_bin.empty else None,
                        )
                        c_bd_4.metric(
                            "Dominan Between",
                            top_between_bin.iloc[0]["Pasangan"] if not top_between_bin.empty else "-",
                            f"{float(top_between_bin.iloc[0]['Persentase Bobot (%)']):.2f}%" if not top_between_bin.empty else None,
                        )

                        scope_tabs = st.tabs(["Within", "Between"])
                        for scope_name, scope_tab in zip(["Within", "Between"], scope_tabs):
                            with scope_tab:
                                df_scope_bin = df_bin_summary[df_bin_summary["Ruang"] == scope_name].copy()
                                if df_scope_bin.empty:
                                    st.info(f"Tidak ada edge pada ruang {scope_name}.")
                                    continue
                                st.dataframe(
                                    df_scope_bin[
                                        [
                                            "Pasangan",
                                            "Jenis Pasangan",
                                            "Bobot Edge",
                                            "Persentase Bobot (%)",
                                            "Jumlah Edge",
                                            "Persentase Edge (%)",
                                        ]
                                    ].style.background_gradient(cmap="YlGnBu", subset=["Persentase Bobot (%)", "Persentase Edge (%)"]),
                                    use_container_width=True,
                                )
                                df_scope_bin_matrix = df_bin_matrix[df_bin_matrix["Ruang"] == scope_name].copy()
                                if not df_scope_bin_matrix.empty:
                                    heat_bin = (
                                        df_scope_bin_matrix.pivot_table(
                                            index="Kategori Baris",
                                            columns="Kategori Kolom",
                                            values="Persentase Bobot (%)",
                                            aggfunc="sum",
                                            fill_value=0.0,
                                        )
                                        .reindex(index=[spec["yes_label"], spec["no_label"], "Tidak Valid"], columns=[spec["yes_label"], spec["no_label"], "Tidak Valid"], fill_value=0.0)
                                    )
                                    fig_bin_heat = px.imshow(
                                        heat_bin,
                                        text_auto=".1f",
                                        color_continuous_scale="YlGnBu",
                                        aspect="auto",
                                        title=f"Heatmap Persentase Bobot Edge - {spec['title']} ({scope_name})",
                                        labels=dict(x="Kategori Kolom", y="Kategori Baris", color="% Bobot"),
                                    )
                                    fig_bin_heat.update_layout(height=380)
                                    st.plotly_chart(fig_bin_heat, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                                same_scope_bin = df_scope_bin[df_scope_bin["Jenis Pasangan"] == "Sama"]["Persentase Bobot (%)"].sum()
                                diff_scope_bin = df_scope_bin[df_scope_bin["Jenis Pasangan"] == "Beda"]["Persentase Bobot (%)"].sum()
                                st.markdown(
                                    f"<div class='soft-card'><b>Interpretasi {spec['title']} - {scope_name}:</b><br>"
                                    f"Pasangan status yang sama menyumbang <b>{same_scope_bin:.2f}% bobot edge</b>, "
                                    f"sedangkan pasangan campuran menyumbang <b>{diff_scope_bin:.2f}%</b>. "
                                    f"Pasangan dominan adalah <b>{df_scope_bin.iloc[0]['Pasangan']}</b> "
                                    f"dengan kontribusi <b>{float(df_scope_bin.iloc[0]['Persentase Bobot (%)']):.2f}%</b>."
                                    f"</div>",
                                    unsafe_allow_html=True,
                                )

            with subbab_dropdown("Audit Spasial per Dusun untuk Bansos, Internet, dan Ponsel", expanded=False):
                st.caption(
                    "Bagian ini melihat dusun sebagai unit wilayah. Hasilnya menunjukkan komposisi `YA` per dusun "
                    "serta seberapa besar bobot koneksi internal dusun yang terjadi pada pasangan `YA-YA`, `YA-TIDAK`, dan `TIDAK-TIDAK`."
                )
                spatial_indicator_specs = [
                    {"label": "Bansos", "col": "bansos_num"},
                    {"label": "Internet", "col": "internet_num"},
                    {"label": "Ponsel", "col": "ponsel_num"},
                ]
                df_spatial_profile = build_spatial_indicator_profile(
                    G,
                    dusun_attr=dusun_attr,
                    indicator_specs=spatial_indicator_specs,
                )
                if df_spatial_profile.empty:
                    st.info("Profil spasial per dusun belum dapat dihitung karena data dusun atau edge internal belum mencukupi.")
                else:
                    st.dataframe(
                        df_spatial_profile.style.background_gradient(
                            cmap="YlGnBu",
                            subset=[
                                "Bansos - Persentase YA (%)",
                                "Internet - Persentase YA (%)",
                                "Ponsel - Persentase YA (%)",
                                "Bansos - YA-YA Bobot (%)",
                                "Internet - YA-YA Bobot (%)",
                                "Ponsel - YA-YA Bobot (%)",
                            ],
                        ),
                        use_container_width=True,
                    )

                    heat_share = (
                        df_spatial_profile.set_index("Dusun")[
                            [
                                "Bansos - Persentase YA (%)",
                                "Internet - Persentase YA (%)",
                                "Ponsel - Persentase YA (%)",
                            ]
                        ]
                        .rename(
                            columns={
                                "Bansos - Persentase YA (%)": "Bansos (YA)",
                                "Internet - Persentase YA (%)": "Internet (YA)",
                                "Ponsel - Persentase YA (%)": "Ponsel (YA)",
                            }
                        )
                    )
                    fig_spatial_share = px.imshow(
                        heat_share,
                        text_auto=".1f",
                        color_continuous_scale="Blues",
                        aspect="auto",
                        title="Heatmap Persentase Status YA per Dusun",
                        labels=dict(x="Indikator", y="Dusun", color="% YA"),
                    )
                    fig_spatial_share.update_layout(height=420)
                    st.plotly_chart(fig_spatial_share, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                    heat_yy = (
                        df_spatial_profile.set_index("Dusun")[
                            [
                                "Bansos - YA-YA Bobot (%)",
                                "Internet - YA-YA Bobot (%)",
                                "Ponsel - YA-YA Bobot (%)",
                            ]
                        ]
                        .rename(
                            columns={
                                "Bansos - YA-YA Bobot (%)": "Bansos (YA-YA)",
                                "Internet - YA-YA Bobot (%)": "Internet (YA-YA)",
                                "Ponsel - YA-YA Bobot (%)": "Ponsel (YA-YA)",
                            }
                        )
                    )
                    fig_spatial_yy = px.imshow(
                        heat_yy,
                        text_auto=".1f",
                        color_continuous_scale="YlGnBu",
                        aspect="auto",
                        title="Heatmap Persentase Bobot Edge YA-YA Internal per Dusun",
                        labels=dict(x="Indikator", y="Dusun", color="% Bobot"),
                    )
                    fig_spatial_yy.update_layout(height=420)
                    st.plotly_chart(fig_spatial_yy, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                    dusun_rank_tabs = st.tabs(["Bansos per Dusun", "Internet per Dusun", "Ponsel per Dusun"])
                    for spec, rank_tab in zip(spatial_indicator_specs, dusun_rank_tabs):
                        with rank_tab:
                            label = spec["label"]
                            show_cols = [
                                "Dusun",
                                "Jumlah KK",
                                "Jumlah Edge Internal",
                                f"{label} - Jumlah YA",
                                f"{label} - Persentase YA (%)",
                                f"{label} - YA-YA Bobot (%)",
                                f"{label} - YA-TIDAK Bobot (%)",
                                f"{label} - TIDAK-TIDAK Bobot (%)",
                                f"{label} - YA-YA Edge (%)",
                            ]
                            df_rank = df_spatial_profile[show_cols].sort_values(f"{label} - YA-YA Bobot (%)", ascending=False).reset_index(drop=True)
                            st.dataframe(
                                df_rank.style.background_gradient(cmap="YlGnBu", subset=[f"{label} - Persentase YA (%)", f"{label} - YA-YA Bobot (%)"]),
                                use_container_width=True,
                            )
                            top_row = df_rank.iloc[0]
                            st.markdown(
                                f"<div class='soft-card'><b>Ringkasan {label} per Dusun:</b><br>"
                                f"Dusun dengan kekuatan koneksi internal `YA-YA` tertinggi adalah <b>{top_row['Dusun']}</b> "
                                f"dengan kontribusi <b>{float(top_row[f'{label} - YA-YA Bobot (%)']):.2f}% bobot edge internal</b>. "
                                f"Proporsi warga berstatus `YA` di dusun ini adalah <b>{float(top_row[f'{label} - Persentase YA (%)']):.2f}%</b>."
                                f"</div>",
                                unsafe_allow_html=True,
                            )

            with subbab_dropdown("Evaluasi Ketepatan Targeting (Layak = IKR Agregat Rendah + Sedang)", expanded=False):
                eval_cols_needed = {"family_id", "f_ikr_dari_rekap_kk", "kategori_ikr"}
                if not eval_cols_needed.issubset(set(df_v.columns)):
                    st.warning(
                        "Kolom evaluasi targeting belum lengkap. Pastikan tersedia family_id, IKR agregat, dan kategori IKR."
                    )
                else:
                    node_set_eval = set(node_ids)
                    df_eval = df_v[df_v["family_id"].isin(node_set_eval)].copy()
                    dropped_rows = int(df_v.shape[0] - df_eval.shape[0])
                    if dropped_rows > 0:
                        st.caption(
                            f"Catatan: {dropped_rows} KK berada di luar graf analisis aktif sehingga tidak masuk evaluasi targeting."
                        )
                    df_eval["IKR Agregat"] = pd.to_numeric(df_eval["f_ikr_dari_rekap_kk"], errors="coerce")
                    df_eval["Layak_Target"] = df_eval["kategori_ikr"].isin(["Rendah", "Sedang"]).astype(int)
                    df_eval["Klaster Louvain"] = df_eval["family_id"].map(
                        lambda fid: int(partition.get(fid, -1)) if fid in partition else -1
                    )
                    if "nama" not in df_eval.columns:
                        df_eval["nama"] = df_eval["family_id"].astype(str)
                    if "bansos_num" not in df_eval.columns:
                        st.info("Kolom `bansos_num` belum tersedia untuk evaluasi targeting bansos.")
                    else:
                        status = (pd.to_numeric(df_eval["bansos_num"], errors="coerce").fillna(0) > 0).astype(int)
                        layak = df_eval["Layak_Target"].astype(int)
                        tp = int(((layak == 1) & (status == 1)).sum())
                        fn = int(((layak == 1) & (status == 0)).sum())
                        fp = int(((layak == 0) & (status == 1)).sum())
                        tn = int(((layak == 0) & (status == 0)).sum())
                        coverage = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0
                        exclusion = (fn / (tp + fn)) if (tp + fn) > 0 else 0.0
                        inclusion = (fp / (tp + fp)) if (tp + fp) > 0 else 0.0
                        st.dataframe(
                            pd.DataFrame(
                                [{
                                    "Audit": "Bansos",
                                    "TP (Layak & Targeted)": tp,
                                    "FN (Layak & Tidak Targeted)": fn,
                                    "FP (Tidak Layak & Targeted)": fp,
                                    "TN (Tidak Layak & Tidak Targeted)": tn,
                                    "Coverage Layak (%)": coverage * 100.0,
                                    "Exclusion Error (%)": exclusion * 100.0,
                                    "Inclusion Error (%)": inclusion * 100.0,
                                }]
                            ).style.format(
                                {
                                    "Coverage Layak (%)": "{:.2f}",
                                    "Exclusion Error (%)": "{:.2f}",
                                    "Inclusion Error (%)": "{:.2f}",
                                }
                            ),
                            use_container_width=True,
                        )

                        show_cols = ["family_id", "nama", "Klaster Louvain", "IKR Agregat", "kategori_ikr"]
                        if dusun_attr in df_eval.columns:
                            show_cols.append(dusun_attr)
                        temp = df_eval.copy()
                        temp["status_target"] = status.values
                        exclusion_df = temp[(temp["Layak_Target"] == 1) & (temp["status_target"] == 0)].copy().sort_values("IKR Agregat", ascending=True)
                        inclusion_df = temp[(temp["Layak_Target"] == 0) & (temp["status_target"] == 1)].copy().sort_values("IKR Agregat", ascending=False)

                        c_ex, c_in = st.columns(2)
                        with c_ex:
                            st.markdown("**Bansos: 10 Teratas Exclusion (paling kritis)**")
                            st.dataframe(exclusion_df[show_cols].head(10), use_container_width=True)
                        with c_in:
                            st.markdown("**Bansos: 10 Teratas Inclusion (paling kritis)**")
                            st.dataframe(inclusion_df[show_cols].head(10), use_container_width=True)

            top_audit_row = df_assort_biner.iloc[df_assort_biner["r"].abs().idxmax()]
            audit_auto_lines = build_audit_auto_narrative(df_assort_biner)
            st.markdown(
                f"<div class='soft-card'><b>Narasi Otomatis Audit (r, Qw*, Qb*):</b><br>"
                f"{audit_auto_lines}<br><br>"
                f"<b>Ringkasan Dominan:</b> atribut dengan pola paling kuat saat ini adalah "
                f"<b>{top_audit_row['Metrik']}</b> (|r|={abs(float(top_audit_row['r'])):.3f})."
                f"</div>",
                unsafe_allow_html=True,
            )
            with subbab_dropdown("Within-Between Assortativity (Montes et al., 2018) dengan Kategori BPS 2014", expanded=False):
                if "f_ikr_dari_rekap_kk" not in df_v.columns:
                    st.warning("Kolom IKR agregat tidak tersedia, sehingga audit Within-Between Montes belum dapat dihitung.")
                else:
                    ikr_cat_lookup = (
                        df_v[["family_id", "kategori_ikr", "kategori_ikr_code"]]
                        .dropna(subset=["family_id"])
                        .drop_duplicates("family_id")
                        .set_index("family_id")
                        .to_dict("index")
                    )
                    nx.set_node_attributes(
                        G,
                        {
                            fid: {
                                "kategori_ikr": vals.get("kategori_ikr", "Tidak Valid"),
                                "kategori_ikr_code": int(vals.get("kategori_ikr_code", 0)),
                            }
                            for fid, vals in ikr_cat_lookup.items()
                            if fid in G.nodes()
                        },
                    )

                    cat_order = ordered_existing_categories(df_v["kategori_ikr"], BPS_CATEGORY_ORDER)
                    cat_dist = (
                        df_v["kategori_ikr"]
                        .value_counts()
                        .reindex(cat_order, fill_value=0)
                        .rename_axis("Kategori BPS")
                        .reset_index(name="Jumlah KK")
                    )
                    cat_dist["Persentase (%)"] = np.where(
                        cat_dist["Jumlah KK"].sum() > 0,
                        (cat_dist["Jumlah KK"] / cat_dist["Jumlah KK"].sum()) * 100.0,
                        0.0,
                    )
                    if cat_dist.empty:
                        st.info("Kategori BPS valid belum tersedia pada data desa terpilih.")
                    else:
                        st.dataframe(cat_dist, use_container_width=True)

                    montes_res = compute_montes_within_between_assortativity(
                        G,
                        category_attr="kategori_ikr_code",
                        group_attr="cluster",
                        invalid_category_values={0},
                    )
                    q_w_star = float(montes_res["q_w_star"])
                    q_b_star = float(montes_res["q_b_star"])

                    st.markdown("##### Visual Jaringan Louvain dengan Pewarnaan Kategori BPS")
                    st.caption(
                        "Node mengikuti hasil graf Louvain yang sama, tetapi warna node kini ditempelkan berdasarkan "
                        "kategori BPS (`kategori_ikr`) agar pola stratifikasi lebih mudah dilihat sebelum membaca Qw* dan Qb*. "
                        f"Mode aktif mengikuti sidebar: `{graph_spatial_mode}`."
                    )
                    if G.number_of_nodes() > 0 and cat_order:
                        fig_montes_graph = go.Figure()
                        node_ids_montes = list(G.nodes())
                        if "pos_focus" in locals() and isinstance(pos_focus, dict) and len(pos_focus) == G.number_of_nodes():
                            pos_montes = pos_focus
                        else:
                            pos_montes = build_clustered_network_layout(
                                G,
                                partition=partition,
                                layout_spread=layout_spread if "layout_spread" in locals() else 2.2,
                                seed=42,
                            )

                        edge_weights_montes = [
                            _safe_float_metric(d.get("weight"), default=0.0) for _, _, d in G.edges(data=True)
                        ]
                        edge_min_montes = float(min(edge_weights_montes)) if edge_weights_montes else 0.0
                        edge_max_montes = float(max(edge_weights_montes)) if edge_weights_montes else 1.0
                        edge_span_montes = max(edge_max_montes - edge_min_montes, 1e-9)

                        visible_edges_montes = (
                            visible_edges_focus
                            if "visible_edges_focus" in locals()
                            else select_representative_edges(G, max_edges=int(np.clip(G.number_of_nodes() * 1.45, 180, 950)), per_node=1)
                        )
                        add_network_edge_traces(
                            fig_montes_graph,
                            visible_edges_montes,
                            pos_montes,
                            edge_min_montes,
                            edge_span_montes,
                            color_fn=edge_color_by_interaction if "edge_color_by_interaction" in locals() else edge_color_by_weight,
                            base_width=0.26,
                            width_scale=0.72,
                            hover=True,
                        )

                        montes_hover_map = {
                            n: (
                                f"Nama: {G.nodes[n].get('nama', '-')}"
                                f"<br>family_id: {n}"
                                f"<br>Kategori BPS: {G.nodes[n].get('kategori_ikr', 'Tidak Valid')}"
                                f"<br>Kode BPS: {G.nodes[n].get('kategori_ikr_code', 0)}"
                                f"<br>IKR Agregat: {_safe_float_metric(G.nodes[n].get('f_ikr_dari_rekap_kk'), default=np.nan):.3f}"
                                f"<br>Klaster Louvain: {G.nodes[n].get('cluster', '-')}"
                            )
                            for n in node_ids_montes
                        }

                        for cat_label in cat_order:
                            cat_nodes = [
                                n for n in node_ids_montes
                                if str(G.nodes[n].get("kategori_ikr", "Tidak Valid")).strip() == cat_label
                            ]
                            if not cat_nodes:
                                continue
                            fig_montes_graph.add_trace(
                                go.Scatter(
                                    x=[pos_montes[n][0] for n in cat_nodes],
                                    y=[pos_montes[n][1] for n in cat_nodes],
                                    mode="markers",
                                        name=cat_label,
                                        marker=dict(
                                            size=node_size_main if "node_size_main" in locals() else network_marker_size(len(node_ids_montes), base=9.0),
                                            color=BPS_CATEGORY_COLORS.get(cat_label, BPS_FALLBACK_COLOR),
                                            opacity=0.86,
                                            line=dict(color=NETWORK_NODE_LINE, width=node_line_width if "node_line_width" in locals() else 0.45),
                                        ),
                                    text=[montes_hover_map[n] for n in cat_nodes],
                                    hoverinfo="text",
                                )
                            )

                        montes_hover_text = [montes_hover_map[n] for n in node_ids_montes]

                        style_network_figure(
                            fig_montes_graph,
                            title="Jaringan Louvain Menurut Kategori BPS",
                            height=660,
                            showlegend=True,
                        )
                        fig_montes_graph.update_layout(
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="left",
                                x=0.0,
                                title="Kategori BPS",
                            )
                        )
                        if graph_spatial_mode == "Layout Jaringan":
                            st.plotly_chart(fig_montes_graph, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                        else:
                            category_to_idx = {cat: idx for idx, cat in enumerate(cat_order)}
                            montes_color_vals = [
                                category_to_idx.get(str(G.nodes[n].get("kategori_ikr", "Tidak Valid")).strip(), len(cat_order) - 1)
                                for n in node_ids_montes
                            ]
                            bps_colorscale = build_discrete_colorscale(
                                [BPS_CATEGORY_COLORS.get(cat, BPS_FALLBACK_COLOR) for cat in cat_order]
                            )
                            fig_montes_spatial = build_spatial_node_figure(
                                G,
                                node_ids=node_ids_montes,
                                node_color_vals=montes_color_vals,
                                node_hover_text=montes_hover_text,
                                title="Sebaran Spasial Louvain Menurut Kategori BPS",
                                spatial_mode=graph_spatial_mode,
                                marker_size=12,
                                colorscale=bps_colorscale,
                                cmin=-0.5,
                                cmax=max(len(cat_order) - 0.5, 0.5),
                                colorbar=dict(
                                    title="Kategori BPS",
                                    tickmode="array",
                                    tickvals=list(range(len(cat_order))),
                                    ticktext=cat_order,
                                ),
                            )
                            if fig_montes_spatial is not None:
                                st.plotly_chart(fig_montes_spatial, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                            else:
                                st.warning("Mode spasial aktif, tetapi kolom lat/lon belum valid. Ditampilkan mode layout jaringan.")
                                st.plotly_chart(fig_montes_graph, use_container_width=True, config=PLOTLY_DRAW_CONFIG)
                    elif not cat_order:
                        st.info("Tidak ada kategori BPS valid untuk pewarnaan graf; visual kategori tidak ditampilkan.")
                    else:
                        st.info("Graf Louvain belum memiliki node yang cukup untuk divisualisasikan.")

                    m_m1, m_m2, m_m3, m_m4 = st.columns(4)
                    m_m1.metric("Qw*", f"{q_w_star:.5f}")
                    m_m2.metric("Qb*", f"{q_b_star:.5f}")
                    m_m3.metric("m_w (within weight)", f"{float(montes_res['m_w']):.4f}")
                    m_m4.metric("m_b (between weight)", f"{float(montes_res['m_b']):.4f}")

                    if q_w_star >= 0.40:
                        qw_interp = "Di dalam kelompok, warga sangat kompak pada strata IKR yang sama."
                    elif q_w_star >= 0.10:
                        qw_interp = "Di dalam kelompok, ada kecenderungan kompak pada strata IKR yang sama."
                    elif q_w_star > -0.10:
                        qw_interp = "Di dalam kelompok, kekompakan strata IKR masih campuran/lemah."
                    else:
                        qw_interp = "Di dalam kelompok, justru lebih banyak keterhubungan lintas strata IKR."

                    if q_b_star <= -0.40:
                        qb_interp = "Hampir tidak ada warga lintas klaster dari strata IKR yang sama saling terhubung."
                    elif q_b_star < -0.10:
                        qb_interp = "Hubungan lintas klaster cenderung terpisah menurut strata IKR."
                    elif q_b_star < 0.10:
                        qb_interp = "Hubungan lintas klaster untuk strata IKR bersifat campuran/netral."
                    else:
                        qb_interp = "Hubungan lintas klaster menunjukkan kemiripan strata IKR yang relatif kuat."

                    st.markdown(
                        f"<div class='soft-card'><b>Penjelasan Otomatis Within-Between:</b><br>"
                        f"<b>Qw (Within)</b>: Mengukur seberapa sering warga dalam satu klaster memiliki kategori IKR yang sama. "
                        f"(Hasil <b>{q_w_star:.2f}</b> berarti: {qw_interp})<br><br>"
                        f"<b>Qb (Between)</b>: Mengukur kemiripan strata IKR pada hubungan lintas klaster. "
                        f"(Hasil <b>{q_b_star:.2f}</b> berarti: {qb_interp})"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                    valid_cat_dist = cat_dist.copy()
                    if not valid_cat_dist.empty and valid_cat_dist["Jumlah KK"].sum() > 0:
                        dominant_idx = valid_cat_dist["Jumlah KK"].idxmax()
                        dominant_cat = str(valid_cat_dist.loc[dominant_idx, "Kategori BPS"])
                        dominant_share = float(valid_cat_dist.loc[dominant_idx, "Persentase (%)"])
                    else:
                        dominant_cat = "Belum tersedia"
                        dominant_share = 0.0

                    if q_w_star >= 0.30 and q_b_star >= 0.30:
                        strat_joint = "Stratifikasi IKR kuat baik intra maupun antar-klaster; kesamaan strata IKR terbawa lintas komunitas."
                    elif q_w_star >= 0.30 and q_b_star < 0.10:
                        strat_joint = "Stratifikasi IKR kuat di dalam klaster, tetapi melemah saat lintas klaster; ada batas antarkomunitas."
                    elif q_w_star < 0.10 and q_b_star >= 0.30:
                        strat_joint = "Di dalam klaster masih campuran, tetapi lintas klaster justru memperlihatkan kesamaan strata yang kuat."
                    else:
                        strat_joint = "Pola stratifikasi IKR cenderung campuran; tidak ada pemisahan yang sangat tegas pada level klaster."

                    st.markdown(
                        f"<div class='soft-card'><b>Narasi Otomatis Stratifikasi BPS:</b><br>"
                        f"Kategori BPS dominan saat ini adalah <b>{dominant_cat}</b> "
                        f"dengan proporsi <b>{dominant_share:.2f}%</b> dari data valid.<br><br>"
                        f"{strat_joint}"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                    df_montes_plot = pd.DataFrame(
                        [
                            {"Komponen": "Qw* (Within)", "Nilai": q_w_star},
                            {"Komponen": "Qb* (Between)", "Nilai": q_b_star},
                        ]
                    )
                    fig_montes = px.bar(
                        df_montes_plot,
                        x="Komponen",
                        y="Nilai",
                        color="Nilai",
                        color_continuous_scale="RdYlGn",
                        title="Skor Normalized Within-Between Assortativity",
                    )
                    fig_montes.add_hline(y=0.0, line_dash="dash", line_color="#475569")
                    fig_montes.update_layout(height=380, yaxis_title="Nilai Q*")
                    st.plotly_chart(fig_montes, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                    st.caption(
                        "Implementasi delta(x_i, x_j) menggunakan kategori BPS 2014 dari IKR agregat; "
                        "delta(h_i, h_j) menggunakan keanggotaan klaster Louvain."
                    )

                    with subbab_dropdown("Rincian Persentase Keterhubungan per Pasangan Kategori BPS", expanded=False):
                        st.caption(
                            "Bagian ini memecah nilai Qw*/Qb* ke level pasangan kategori: misalnya `Rendah-Rendah`, `Rendah-Sedang`, "
                            "dan seterusnya. Persentase dihitung dari total bobot edge dalam ruang `Within` atau `Between`."
                        )
                        _, df_pair_summary, df_pair_matrix = build_category_connection_breakdown(
                            G,
                            category_attr="kategori_ikr",
                            group_attr="cluster",
                            category_order=cat_order,
                            invalid_label="Tidak Valid",
                        )
                        if df_pair_summary.empty:
                            st.info("Belum ada edge yang cukup untuk merinci pasangan kategori BPS pada level within/between.")
                        else:
                            same_share = (
                                df_pair_summary[df_pair_summary["Jenis Pasangan"] == "Sama"]
                                .groupby("Ruang")["Persentase Bobot (%)"]
                                .sum()
                                .to_dict()
                            )
                            top_within = df_pair_summary[df_pair_summary["Ruang"] == "Within"].sort_values("Bobot Edge", ascending=False).head(1)
                            top_between = df_pair_summary[df_pair_summary["Ruang"] == "Between"].sort_values("Bobot Edge", ascending=False).head(1)
                            c_pair_1, c_pair_2, c_pair_3, c_pair_4 = st.columns(4)
                            c_pair_1.metric("Share Sama Within", f"{float(same_share.get('Within', 0.0)):.2f}%")
                            c_pair_2.metric("Share Sama Between", f"{float(same_share.get('Between', 0.0)):.2f}%")
                            c_pair_3.metric(
                                "Pasangan Dominan Within",
                                top_within.iloc[0]["Pasangan"] if not top_within.empty else "-",
                                f"{float(top_within.iloc[0]['Persentase Bobot (%)']):.2f}%" if not top_within.empty else None,
                            )
                            c_pair_4.metric(
                                "Pasangan Dominan Between",
                                top_between.iloc[0]["Pasangan"] if not top_between.empty else "-",
                                f"{float(top_between.iloc[0]['Persentase Bobot (%)']):.2f}%" if not top_between.empty else None,
                            )

                            tabs_pair = st.tabs(["Within Klaster", "Between Klaster"])
                            for scope_name, tab in zip(["Within", "Between"], tabs_pair):
                                with tab:
                                    df_scope = df_pair_summary[df_pair_summary["Ruang"] == scope_name].copy()
                                    if df_scope.empty:
                                        st.info(f"Tidak ada edge untuk ruang {scope_name}.")
                                        continue
                                    df_scope_display = df_scope[
                                        [
                                            "Pasangan",
                                            "Jenis Pasangan",
                                            "Bobot Edge",
                                            "Persentase Bobot (%)",
                                            "Jumlah Edge",
                                            "Persentase Edge (%)",
                                        ]
                                    ].copy()
                                    st.dataframe(
                                        df_scope_display.style.background_gradient(cmap="YlGnBu", subset=["Persentase Bobot (%)", "Persentase Edge (%)"]),
                                        use_container_width=True,
                                    )

                                    df_scope_matrix = df_pair_matrix[df_pair_matrix["Ruang"] == scope_name].copy()
                                    if not df_scope_matrix.empty:
                                        heatmap_df = (
                                            df_scope_matrix.pivot_table(
                                                index="Kategori Baris",
                                                columns="Kategori Kolom",
                                                values="Persentase Bobot (%)",
                                                aggfunc="sum",
                                                fill_value=0.0,
                                            )
                                            .reindex(index=cat_order, columns=cat_order, fill_value=0.0)
                                        )
                                        fig_pair_heat = px.imshow(
                                            heatmap_df,
                                            text_auto=".1f",
                                            color_continuous_scale="YlGnBu",
                                            aspect="auto",
                                            title=f"Heatmap Persentase Bobot Edge - {scope_name}",
                                            labels=dict(x="Kategori Kolom", y="Kategori Baris", color="% Bobot"),
                                        )
                                        fig_pair_heat.update_layout(height=430)
                                        st.plotly_chart(fig_pair_heat, use_container_width=True, config=PLOTLY_DRAW_CONFIG)

                                    same_scope = df_scope[df_scope["Jenis Pasangan"] == "Sama"]["Persentase Bobot (%)"].sum()
                                    diff_scope = df_scope[df_scope["Jenis Pasangan"] == "Beda"]["Persentase Bobot (%)"].sum()
                                    st.markdown(
                                        f"<div class='soft-card'><b>Interpretasi {scope_name}:</b><br>"
                                        f"Pasangan kategori yang sama menyumbang <b>{same_scope:.2f}% bobot edge</b>, "
                                        f"sedangkan pasangan beda kategori menyumbang <b>{diff_scope:.2f}%</b>. "
                                        f"Pasangan dominan adalah <b>{df_scope.iloc[0]['Pasangan']}</b> "
                                        f"dengan kontribusi <b>{float(df_scope.iloc[0]['Persentase Bobot (%)']):.2f}%</b>."
                                        f"</div>",
                                        unsafe_allow_html=True,
                                    )

        st.info("Mode fokus aktif: proses dibatasi sampai graf base, Louvain, graf hasil, audit assortativity 5 dimensi IKR, audit kebijakan biner, dan audit Within-Between Montes (BPS 2014).")
        st.stop()

    else: st.error("Data tidak mencukupi untuk wilayah ini.")
else: st.info("Selamat Datang. Silakan unggah database desa untuk memulai Audit SNA.")





