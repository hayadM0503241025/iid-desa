import base64
import os
from io import BytesIO
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

try:
    from community import community_louvain
except Exception:  # pragma: no cover - fallback dipakai bila python-louvain belum tersedia.
    community_louvain = None


# =========================================================
# 1. KONFIGURASI DASAR DAN TEMA
# =========================================================
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_PATH = BASE_DIR / "batu_putih.xlsx"
LOGO_PATH = BASE_DIR / "assets" / "logo-banner2.png"
HEADER_PATH = next(
    (
        path
        for path in [
            BASE_DIR / "assets" / "header.png",
            BASE_DIR / "assets" / "header.jpg",
            BASE_DIR / "assets" / "header.jpeg",
        ]
        if path.exists()
    ),
    None,
)

DDP_BLUE = "#111827"
DDP_RED = "#B91C1C"
PLOT_TEXT_COLOR = "#111827"
PLOT_GRID_COLOR = "#E2E8F0"
PUBLICATION_TEMPLATE = "batu_putih_clarity"
PUBLICATION_FONT = '"Source Sans Pro", "Segoe UI", Arial, sans-serif'
COLOR_SEQUENCE = [
    "#2563EB",
    "#B91C1C",
    "#0F766E",
    "#D97706",
    "#7C3AED",
    "#0891B2",
    "#BE123C",
    "#4D7C0F",
    "#9333EA",
    "#475569",
    "#EA580C",
]
CONTINUOUS_SCALE = [[0.0, "#B91C1C"], [0.5, "#F59E0B"], [1.0, "#0F766E"]]
PLOTLY_DRAW_CONFIG = {
    "scrollZoom": True,
    "displayModeBar": True,
    "displaylogo": False,
}

PPWP_COLS = ["PPWP 01", "PPWP 02", "PPWP 03"]
EDUCATION_LABELS = [
    "Tidak Punya Ijazah",
    "SD/sederajat",
    "SMP/sederajat",
    "SMA/sederajat",
    "D1-S1",
    "S2-S3",
]
NETWORK_OPTIONS = ["Politik", "Sosial-Demografis", "Gabungan"]
SIMILARITY_METHOD_OPTIONS = ["Pembobotan komponen", "Akumulasi nilai ternormalisasi"]
SIMILARITY_COMPONENTS = {
    "DPR": {"column": "sim_dpr", "label": "Suara DPR"},
    "PPWP": {"column": "sim_ppwp", "label": "Suara PPWP"},
    "Pendidikan": {"column": "sim_pendidikan", "label": "Pendidikan"},
    "Pekerjaan": {"column": "sim_pekerjaan", "label": "Pekerjaan"},
    "Demografi": {"column": "sim_demografi", "label": "DPT/Partisipasi"},
}


if LOGO_PATH.exists():
    page_icon = str(LOGO_PATH)
else:
    page_icon = "SNA"

st.set_page_config(
    page_title="SNA Desa Batu Putih",
    page_icon=page_icon,
    layout="wide",
)


pio.templates[PUBLICATION_TEMPLATE] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(color=PLOT_TEXT_COLOR, size=13, family=PUBLICATION_FONT),
        title=dict(font=dict(color=PLOT_TEXT_COLOR, size=18, family=PUBLICATION_FONT), x=0.02, xanchor="left"),
        legend=dict(
            bgcolor="rgba(255,255,255,0.94)",
            bordercolor="#E2E8F0",
            borderwidth=1,
            font=dict(color=PLOT_TEXT_COLOR),
        ),
        xaxis=dict(
            color=PLOT_TEXT_COLOR,
            gridcolor=PLOT_GRID_COLOR,
            zerolinecolor="#CBD5E1",
            linecolor="#CBD5E1",
            ticks="outside",
        ),
        yaxis=dict(
            color=PLOT_TEXT_COLOR,
            gridcolor=PLOT_GRID_COLOR,
            zerolinecolor="#CBD5E1",
            linecolor="#CBD5E1",
            ticks="outside",
        ),
    )
)
pio.templates.default = PUBLICATION_TEMPLATE
pio.templates["plotly_white"] = pio.templates[PUBLICATION_TEMPLATE]


def get_image_data_uri(path):
    if not path or not Path(path).exists():
        return None
    ext = Path(path).suffix.lower()
    mime = "image/png" if ext == ".png" else "image/jpeg" if ext in {".jpg", ".jpeg"} else "application/octet-stream"
    with open(path, "rb") as file:
        encoded = base64.b64encode(file.read()).decode("utf-8")
    return f"data:{mime};base64,{encoded}"


HEADER_DATA_URI = get_image_data_uri(HEADER_PATH)
LOGO_DATA_URI = get_image_data_uri(LOGO_PATH)


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


def inject_css():
    st.markdown(
        f"""
        <style>
            :root {{
                --text-main: {DDP_BLUE};
                --accent: #2563EB;
                --accent-red: {DDP_RED};
                --surface: rgba(255, 255, 255, 0.94);
                --stroke: rgba(15, 23, 42, 0.12);
            }}
            .stApp {{
                background:
                    radial-gradient(circle at 18% 12%, rgba(37, 99, 235, 0.08), transparent 24%),
                    linear-gradient(180deg, #F8FAFC 0%, #EEF2F7 100%);
                color: var(--text-main);
            }}
            section[data-testid="stSidebar"] {{
                background: linear-gradient(180deg, #0F172A 0%, #111827 100%);
                border-right: 1px solid rgba(255,255,255,0.10);
            }}
            section[data-testid="stSidebar"] * {{
                color: #E5E7EB;
            }}
            section[data-testid="stSidebar"] .stSlider label,
            section[data-testid="stSidebar"] .stSelectbox label,
            section[data-testid="stSidebar"] .stRadio label,
            section[data-testid="stSidebar"] .stNumberInput label {{
                color: #F8FAFC !important;
                font-weight: 650;
            }}
            .sidebar-logo-shell {{
                display: flex;
                align-items: center;
                justify-content: center;
                width: 54px;
                height: 54px;
                border-radius: 14px;
                background: #FFFFFF;
                border: 1px solid rgba(255,255,255,0.22);
                overflow: hidden;
            }}
            .sidebar-logo-img {{
                width: 100%;
                height: 100%;
                object-fit: contain;
            }}
            .sidebar-logo-fallback {{
                color: #111827;
                font-weight: 800;
                font-size: 0.88rem;
            }}
            .global-header-wrap {{
                margin: -1.2rem -1.2rem 1.1rem -1.2rem;
                border-bottom: 1px solid rgba(15, 23, 42, 0.10);
                background: #FFFFFF;
            }}
            .global-header-img {{
                display: block;
                width: 100%;
                max-height: 150px;
                object-fit: cover;
            }}
            .main-header {{
                font-size: clamp(1.7rem, 3vw, 2.55rem);
                font-weight: 800;
                letter-spacing: 0;
                color: #0F172A;
                margin: 0.3rem 0 0.2rem 0;
            }}
            .section-title {{
                font-size: 1.12rem;
                font-weight: 800;
                color: #0F172A;
                margin: 0.2rem 0 0.65rem 0;
            }}
            .soft-card {{
                background: var(--surface);
                border: 1px solid var(--stroke);
                border-radius: 8px;
                padding: 1rem 1.05rem;
                box-shadow: 0 12px 30px rgba(15, 23, 42, 0.07);
            }}
            .analysis-note {{
                background: #FFFFFF;
                border-left: 4px solid #2563EB;
                border-radius: 6px;
                padding: 0.85rem 1rem;
                color: #334155;
                font-size: 0.95rem;
                line-height: 1.55;
            }}
            div[data-testid="stMetric"] {{
                background: rgba(255, 255, 255, 0.96);
                border: 1px solid rgba(15, 23, 42, 0.10);
                border-radius: 8px;
                padding: 0.8rem 0.85rem;
                box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
            }}
            div[data-testid="stMetric"] label {{
                color: #475569 !important;
                font-weight: 650;
            }}
            .stTabs [data-baseweb="tab-list"] {{
                gap: 0.4rem;
            }}
            .stTabs [data-baseweb="tab"] {{
                border-radius: 8px;
                border: 1px solid rgba(15,23,42,0.12);
                background: rgba(255,255,255,0.88);
                padding: 0.55rem 0.85rem;
                height: auto;
            }}
            .stTabs [aria-selected="true"] {{
                background: #0F172A !important;
                color: #FFFFFF !important;
            }}
            .dataframe {{
                font-size: 0.9rem;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


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
    fig.update_layout(
        template=PUBLICATION_TEMPLATE,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(color=PLOT_TEXT_COLOR, family=PUBLICATION_FONT, size=13),
        hoverlabel=dict(bgcolor="#FFFFFF", font_size=12, font_family=PUBLICATION_FONT),
        margin=margin or dict(l=48, r=24, t=72, b=48),
    )
    if title is not None:
        fig.update_layout(title=dict(text=title, x=0.02, xanchor="left"))
    if height is not None:
        fig.update_layout(height=height)
    if showlegend is not None:
        fig.update_layout(showlegend=showlegend)
    if legend_title is not None:
        fig.update_layout(legend_title_text=legend_title)
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


def style_network_figure(fig, title, height=650, showlegend=False):
    style_publication_figure(
        fig,
        title=title,
        height=height,
        showlegend=showlegend,
        margin=dict(l=12, r=12, t=72, b=12),
    )
    fig.update_xaxes(visible=False, showgrid=False, zeroline=False)
    fig.update_yaxes(visible=False, showgrid=False, zeroline=False)
    return fig


# =========================================================
# 2. UTILITAS DATA
# =========================================================
def clean_desa_name(value):
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split()).title()


def safe_divide(numerator, denominator):
    num = pd.to_numeric(numerator, errors="coerce").astype(float)
    den = pd.to_numeric(denominator, errors="coerce").replace(0, np.nan).astype(float)
    return (num / den).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def row_normalize(df):
    numeric = df.apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(float)
    denom = numeric.sum(axis=1).replace(0, np.nan)
    return numeric.div(denom, axis=0).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def minmax_normalize(df):
    numeric = df.apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(float)
    min_vals = numeric.min(axis=0)
    spans = (numeric.max(axis=0) - min_vals).replace(0, np.nan)
    scaled = (numeric - min_vals).div(spans, axis=1)
    return scaled.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def strip_prefix(label, prefix):
    return str(label).replace(prefix, "", 1)


def title_job(label):
    return str(label).strip().title()


def format_pct(value):
    try:
        return f"{float(value) * 100:.0f}%"
    except Exception:
        return "-"


def find_header_row(raw_df, needle):
    for idx in raw_df.index:
        values = raw_df.loc[idx].astype(str).str.strip().str.lower().tolist()
        if str(needle).lower() in values:
            return int(idx)
    raise ValueError(f"Header '{needle}' tidak ditemukan di workbook.")


def parse_main_sheet(xl):
    raw = xl.parse("Sheet1", header=None)
    header_idx = find_header_row(raw, "Kode Deskel")
    header = raw.iloc[header_idx].tolist()
    header_clean = [str(value).strip() if pd.notna(value) else f"kolom_{i}" for i, value in enumerate(header)]
    try:
        end_col = header_clean.index("Total DPR") + 1
    except ValueError:
        end_col = 37

    data_row_indices = []
    started = False
    for idx in range(header_idx + 1, len(raw)):
        first_cell = raw.iat[idx, 0]
        first_text = "" if pd.isna(first_cell) else str(first_cell).strip()
        if pd.Series([first_text]).astype(str).str.match(r"^\d{2}\.\d{2}\.\d{2}\.\d{4}$", na=False).iloc[0]:
            data_row_indices.append(idx)
            started = True
        elif started:
            # Sheet1 berisi beberapa tabel tambahan di bawah tabel utama.
            # Begitu blok desa pertama selesai, parser berhenti agar kode deskel
            # pada tabel tambahan tidak ikut menjadi node graf.
            break

    df = raw.loc[data_row_indices, : end_col - 1].copy()
    df.columns = header_clean[:end_col]
    df = df.rename(columns={"Kode Deskel": "kode", "Nama Deskel": "desa"})
    df["kode"] = df["kode"].astype(str).str.strip()
    df["desa"] = df["desa"].apply(clean_desa_name)

    first_party_idx = df.columns.get_loc("Partai Kebangkitan Bangsa")
    last_party_idx = df.columns.get_loc("Partai Ummat")
    dpr_cols = list(df.columns[first_party_idx : last_party_idx + 1])

    numeric_cols = [
        "pemilih_dpt_l",
        "pemilih_dpt_p",
        "pemilih_dpt_j",
        "pengguna_dpt_l",
        "pengguna_dpt_p",
        "pengguna_dpt_j",
        "pengguna_dptb_l",
        "pengguna_dptb_p",
        "pengguna_dptb_j",
        "pengguna_dpk_l",
        "pengguna_dpk_p",
        "pengguna_dpk_j",
        *PPWP_COLS,
        "Total PPWP",
        *dpr_cols,
        "Total DPR",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["pengguna_total_l"] = df["pengguna_dpt_l"] + df["pengguna_dptb_l"] + df["pengguna_dpk_l"]
    df["pengguna_total_p"] = df["pengguna_dpt_p"] + df["pengguna_dptb_p"] + df["pengguna_dpk_p"]
    df["pengguna_total"] = df["pengguna_dpt_j"] + df["pengguna_dptb_j"] + df["pengguna_dpk_j"]
    df["partisipasi_pemilih"] = safe_divide(df["pengguna_total"], df["pemilih_dpt_j"]).clip(0, 1.25)
    df["rasio_dpt_perempuan"] = safe_divide(df["pemilih_dpt_p"], df["pemilih_dpt_j"]).clip(0, 1)
    df["rasio_pengguna_perempuan"] = safe_divide(df["pengguna_total_p"], df["pengguna_total"]).clip(0, 1)
    df["ppwp_dominan"] = df[PPWP_COLS].idxmax(axis=1)
    df["partai_dominan"] = df[dpr_cols].idxmax(axis=1)

    return df.reset_index(drop=True), dpr_cols


def parse_education_sheet(xl, kode_to_desa):
    raw = xl.parse("pendidikan", header=None)
    mask = raw[0].astype(str).str.match(r"^\d{2}\.\d{2}\.\d{2}\.\d{4}$", na=False)
    edu = raw.loc[mask, list(range(8))].copy()
    edu.columns = ["kode", *EDUCATION_LABELS, "total_pendidikan"]
    edu["kode"] = edu["kode"].astype(str).str.strip()
    for col in EDUCATION_LABELS + ["total_pendidikan"]:
        edu[col] = pd.to_numeric(edu[col], errors="coerce").fillna(0.0)
    edu["desa"] = edu["kode"].map(kode_to_desa).fillna("")
    edu_count_cols = [f"edu_{col}" for col in EDUCATION_LABELS]
    edu = edu.rename(columns={old: new for old, new in zip(EDUCATION_LABELS, edu_count_cols)})
    return edu[["kode", "desa", *edu_count_cols, "total_pendidikan"]], edu_count_cols


def parse_job_sheet(xl, village_order):
    raw = xl.parse("pekerjaan dominan", header=None)
    n_villages = len(village_order)
    category_values = {}

    for idx in raw.index:
        raw_category = raw.iat[idx, 1] if raw.shape[1] > 1 else np.nan
        if pd.isna(raw_category):
            continue
        category = " ".join(str(raw_category).strip().lower().split())
        if not category or category == "nan":
            continue
        values = pd.to_numeric(raw.iloc[idx, 2 : 2 + n_villages], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        if values.size != n_villages:
            continue
        if category in category_values:
            category_values[category] = category_values[category] + values
        else:
            category_values[category] = values

    if not category_values:
        raise ValueError("Sheet pekerjaan dominan tidak memuat kategori pekerjaan yang dapat dibaca.")

    job_counts = pd.DataFrame(category_values, index=village_order)
    job_counts.index.name = "desa"
    job_counts = job_counts.reset_index()
    job_count_cols = [f"job_{col}" for col in category_values]
    job_counts = job_counts.rename(columns={old: new for old, new in zip(category_values.keys(), job_count_cols)})
    job_counts["total_pekerjaan"] = job_counts[job_count_cols].sum(axis=1)
    return job_counts, job_count_cols


def add_dominant_profiles(df, edu_count_cols, job_count_cols):
    result = df.copy()
    result["pendidikan_dominan"] = result[edu_count_cols].idxmax(axis=1).map(lambda col: strip_prefix(col, "edu_"))
    result["pekerjaan_dominan"] = result[job_count_cols].idxmax(axis=1).map(lambda col: title_job(strip_prefix(col, "job_")))

    def top_jobs(row):
        values = row[job_count_cols].sort_values(ascending=False).head(5)
        labels = [title_job(strip_prefix(col, "job_")) for col, val in values.items() if float(val) > 0]
        return ", ".join(labels)

    result["top_5_pekerjaan"] = result.apply(top_jobs, axis=1)
    return result


@st.cache_data(show_spinner=False)
def load_batu_putih_workbook(uploaded_bytes=None):
    if uploaded_bytes:
        source = BytesIO(uploaded_bytes)
    else:
        source = DEFAULT_DATA_PATH
    xl = pd.ExcelFile(source)
    main_df, dpr_cols = parse_main_sheet(xl)
    kode_to_desa = dict(zip(main_df["kode"], main_df["desa"]))
    edu_df, edu_count_cols = parse_education_sheet(xl, kode_to_desa)
    job_df, job_count_cols = parse_job_sheet(xl, main_df["desa"].tolist())

    merged = (
        main_df.merge(edu_df.drop(columns=["desa"]), on="kode", how="left")
        .merge(job_df, on="desa", how="left")
        .reset_index(drop=True)
    )
    for col in [*edu_count_cols, "total_pendidikan", *job_count_cols, "total_pekerjaan"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    merged = add_dominant_profiles(merged, edu_count_cols, job_count_cols)

    meta = {
        "dpr_cols": dpr_cols,
        "edu_count_cols": edu_count_cols,
        "job_count_cols": job_count_cols,
    }
    return merged, meta


def build_feature_matrices(profile_df, dpr_cols, edu_count_cols, job_count_cols):
    df = profile_df.set_index("desa").copy()
    ppwp_features = row_normalize(df[PPWP_COLS])
    dpr_features = row_normalize(df[dpr_cols])
    education_features = row_normalize(df[edu_count_cols])
    job_features = row_normalize(df[job_count_cols])
    demographic_features = minmax_normalize(
        pd.DataFrame(
            {
                "rasio_dpt_perempuan": df["rasio_dpt_perempuan"],
                "rasio_pengguna_perempuan": df["rasio_pengguna_perempuan"],
                "partisipasi_pemilih": df["partisipasi_pemilih"],
            },
            index=df.index,
        )
    )
    return {
        "PPWP": ppwp_features,
        "DPR": dpr_features,
        "Pendidikan": education_features,
        "Pekerjaan": job_features,
        "Demografi": demographic_features,
    }


def cosine_similarity_frame(feature_df):
    labels = feature_df.index.tolist()
    matrix = feature_df.to_numpy(dtype=float)
    norms = np.linalg.norm(matrix, axis=1)
    denom = np.outer(norms, norms)
    with np.errstate(divide="ignore", invalid="ignore"):
        sim = np.divide(matrix @ matrix.T, denom, out=np.zeros_like(denom, dtype=float), where=denom > 1e-12)
    sim = np.clip(sim, 0.0, 1.0)
    np.fill_diagonal(sim, 1.0)
    return pd.DataFrame(sim, index=labels, columns=labels)


def weighted_similarity(similarities, weights):
    labels = next(iter(similarities.values())).index
    result = pd.DataFrame(0.0, index=labels, columns=labels)
    total_weight = 0.0
    for key, weight in weights.items():
        clean_weight = float(weight)
        if clean_weight <= 0:
            continue
        result = result + similarities[key] * clean_weight
        total_weight += clean_weight
    if total_weight <= 0:
        return result
    result = result / total_weight
    np.fill_diagonal(result.values, 1.0)
    return result.clip(0, 1)


def accumulated_feature_similarity(features, components):
    labels = next(iter(features.values())).index
    active_components = [component for component in components if component in features]
    if not active_components:
        return pd.DataFrame(0.0, index=labels, columns=labels)
    combined = pd.concat(
        [features[component].add_prefix(f"{component}_") for component in active_components],
        axis=1,
    )
    return cosine_similarity_frame(combined)


def build_similarity_bundle(profile_df, meta, politics_dpr_weight, social_weights, combined_weights, similarity_method):
    features = build_feature_matrices(
        profile_df,
        meta["dpr_cols"],
        meta["edu_count_cols"],
        meta["job_count_cols"],
    )
    base_sims = {name: cosine_similarity_frame(matrix) for name, matrix in features.items()}
    politics_weights = {
        "DPR": politics_dpr_weight,
        "PPWP": 1.0 - politics_dpr_weight,
    }
    weighted_sims = {
        "Politik": weighted_similarity(base_sims, politics_weights),
        "Sosial-Demografis": weighted_similarity(base_sims, social_weights),
        "Gabungan": weighted_similarity(base_sims, combined_weights),
    }
    if similarity_method == "Akumulasi nilai ternormalisasi":
        network_sims = {
            "Politik": accumulated_feature_similarity(features, politics_weights.keys()),
            "Sosial-Demografis": accumulated_feature_similarity(features, social_weights.keys()),
            "Gabungan": accumulated_feature_similarity(features, combined_weights.keys()),
        }
    else:
        network_sims = weighted_sims
    return base_sims, network_sims, weighted_sims, features


# =========================================================
# 3. PEMBENTUKAN GRAF DAN METRIK SNA
# =========================================================
def get_pairwise_edges(sim_df):
    labels = sim_df.index.tolist()
    rows = []
    for i, source in enumerate(labels):
        for j in range(i + 1, len(labels)):
            target = labels[j]
            rows.append((source, target, float(sim_df.iloc[i, j])))
    return rows


def select_edges(sim_df, mode, top_k=3, threshold=0.90, threshold_sim_df=None):
    candidates = get_pairwise_edges(sim_df)
    if mode == "Threshold":
        threshold_lookup = threshold_sim_df if threshold_sim_df is not None else sim_df
        selected = [
            (u, v, w)
            for u, v, w in candidates
            if float(threshold_lookup.loc[u, v]) >= float(threshold)
        ]
    else:
        selected_keys = set()
        labels = sim_df.index.tolist()
        for source in labels:
            ranked = (
                sim_df.loc[source]
                .drop(index=source)
                .sort_values(ascending=False)
                .head(int(top_k))
            )
            for target in ranked.index:
                selected_keys.add(tuple(sorted((source, target))))
        lookup = {tuple(sorted((u, v))): (u, v, w) for u, v, w in candidates}
        selected = [lookup[key] for key in selected_keys if key in lookup]

    selected = sorted(selected, key=lambda item: item[2], reverse=True)
    return selected


def detect_communities(graph_obj):
    if graph_obj.number_of_nodes() == 0:
        return {}
    if graph_obj.number_of_edges() == 0:
        return {node: idx for idx, node in enumerate(graph_obj.nodes())}

    if community_louvain is not None:
        raw_partition = community_louvain.best_partition(graph_obj, weight="weight", random_state=42)
    else:
        communities = nx.algorithms.community.greedy_modularity_communities(graph_obj, weight="weight")
        raw_partition = {}
        for cid, community_nodes in enumerate(communities):
            for node in community_nodes:
                raw_partition[node] = cid

    cluster_strength = {}
    for cid in set(raw_partition.values()):
        members = [node for node, cluster in raw_partition.items() if cluster == cid]
        values = [graph_obj.degree(node, weight="weight") for node in members]
        cluster_strength[cid] = float(np.mean(values)) if values else 0.0
    reorder = {old: new for new, (old, _) in enumerate(sorted(cluster_strength.items(), key=lambda item: item[1], reverse=True), start=1)}
    return {node: reorder[cluster] for node, cluster in raw_partition.items()}


def build_graph(profile_df, sim_df, relation_name, edge_mode, top_k=3, threshold=0.90, threshold_sim_df=None):
    graph_obj = nx.Graph()
    for _, row in profile_df.iterrows():
        attrs = row.to_dict()
        graph_obj.add_node(row["desa"], **attrs)

    selected_edges = select_edges(
        sim_df,
        edge_mode,
        top_k=top_k,
        threshold=threshold,
        threshold_sim_df=threshold_sim_df,
    )
    for source, target, weight in selected_edges:
        graph_obj.add_edge(source, target, weight=float(weight), relation=relation_name)

    partition = detect_communities(graph_obj)
    nx.set_node_attributes(graph_obj, partition, "cluster")
    return graph_obj


def compute_centrality_table(graph_obj):
    if graph_obj.number_of_nodes() == 0:
        return pd.DataFrame()

    degree = dict(graph_obj.degree())
    weighted_degree = dict(graph_obj.degree(weight="weight"))
    graph_dist = graph_obj.copy()
    for _, _, data in graph_dist.edges(data=True):
        weight = float(data.get("weight", 0.0))
        data["distance"] = 1.0 / max(weight, 1e-9)

    if graph_obj.number_of_edges() > 0:
        betweenness = nx.betweenness_centrality(graph_dist, weight="distance", normalized=True)
        closeness = nx.closeness_centrality(graph_dist, distance="distance")
        try:
            eigenvector = nx.eigenvector_centrality(graph_obj, weight="weight", max_iter=2000, tol=1e-7)
        except Exception:
            eigenvector = {node: 0.0 for node in graph_obj.nodes()}
    else:
        betweenness = {node: 0.0 for node in graph_obj.nodes()}
        closeness = {node: 0.0 for node in graph_obj.nodes()}
        eigenvector = {node: 0.0 for node in graph_obj.nodes()}

    rows = []
    for node in graph_obj.nodes():
        attrs = graph_obj.nodes[node]
        rows.append(
            {
                "Desa": node,
                "Klaster": int(attrs.get("cluster", 0)),
                "Degree": float(degree.get(node, 0.0)),
                "Weighted Degree": float(weighted_degree.get(node, 0.0)),
                "Betweenness": float(betweenness.get(node, 0.0)),
                "Closeness": float(closeness.get(node, 0.0)),
                "Eigenvector": float(eigenvector.get(node, 0.0)),
                "Pemenang PPWP": attrs.get("ppwp_dominan", "-"),
                "Partai Dominan": attrs.get("partai_dominan", "-"),
                "Pekerjaan Dominan": attrs.get("pekerjaan_dominan", "-"),
                "Partisipasi": float(attrs.get("partisipasi_pemilih", 0.0)),
                "DPT": float(attrs.get("pemilih_dpt_j", 0.0)),
            }
        )
    return pd.DataFrame(rows).sort_values(["Weighted Degree", "Degree"], ascending=False).reset_index(drop=True)


def communities_from_graph(graph_obj):
    cluster_map = {}
    for node, attrs in graph_obj.nodes(data=True):
        cluster_map.setdefault(attrs.get("cluster", 0), set()).add(node)
    return [members for members in cluster_map.values() if members]


def compute_modularity_score(graph_obj):
    if graph_obj.number_of_nodes() == 0 or graph_obj.number_of_edges() == 0:
        return 0.0
    communities = communities_from_graph(graph_obj)
    if len(communities) <= 1:
        return 0.0
    try:
        value = nx.algorithms.community.quality.modularity(graph_obj, communities, weight="weight")
        return float(value) if np.isfinite(value) else 0.0
    except Exception:
        return 0.0


def interpret_modularity(value):
    q_value = float(value)
    if q_value < 0.10:
        return "lemah; pemisahan klaster belum kuat"
    if q_value < 0.30:
        return "cukup; ada struktur komunitas ringan"
    if q_value < 0.50:
        return "sedang; klaster cukup jelas"
    return "kuat; klaster sangat jelas"


def _safe_attribute_assortativity(graph_obj, attr_name):
    if graph_obj.number_of_nodes() < 2 or graph_obj.number_of_edges() == 0:
        return 0.0
    valid_nodes = [node for node in graph_obj.nodes() if pd.notna(graph_obj.nodes[node].get(attr_name))]
    if len(valid_nodes) < 2:
        return 0.0
    graph_sub = graph_obj.subgraph(valid_nodes).copy()
    values = [str(graph_sub.nodes[node].get(attr_name)) for node in graph_sub.nodes()]
    if len(set(values)) <= 1:
        return 0.0
    for node in graph_sub.nodes():
        graph_sub.nodes[node]["__assort_attr__"] = str(graph_sub.nodes[node].get(attr_name))
    try:
        value = nx.attribute_assortativity_coefficient(graph_sub, "__assort_attr__")
        return float(value) if np.isfinite(value) else 0.0
    except Exception:
        return 0.0


def _safe_numeric_assortativity(graph_obj, attr_name):
    if graph_obj.number_of_nodes() < 2 or graph_obj.number_of_edges() == 0:
        return 0.0
    raw = pd.Series({node: graph_obj.nodes[node].get(attr_name) for node in graph_obj.nodes()})
    numeric = pd.to_numeric(raw, errors="coerce")
    valid_nodes = numeric.dropna().index.tolist()
    if len(valid_nodes) < 2 or numeric.loc[valid_nodes].nunique() <= 1:
        return 0.0
    graph_sub = graph_obj.subgraph(valid_nodes).copy()
    for node in graph_sub.nodes():
        graph_sub.nodes[node]["__assort_num__"] = float(numeric.loc[node])
    try:
        value = nx.numeric_assortativity_coefficient(graph_sub, "__assort_num__")
        return float(value) if np.isfinite(value) else 0.0
    except Exception:
        return 0.0


def interpret_assortativity(value):
    r_value = float(value)
    abs_value = abs(r_value)
    if abs_value < 0.10:
        level = "sangat lemah/netral"
    elif abs_value < 0.30:
        level = "lemah"
    elif abs_value < 0.50:
        level = "sedang"
    else:
        level = "kuat"
    if r_value > 0.05:
        direction = "cenderung sama/mirip"
    elif r_value < -0.05:
        direction = "cenderung berbeda"
    else:
        direction = "tidak ada kecenderungan jelas"
    return f"{direction}; {level}"


def build_assortativity_table(graph_obj):
    specs = [
        ("Klaster Louvain", "cluster", "Kategorikal", "Apakah edge lebih banyak berada di dalam klaster yang sama?"),
        ("Pemenang PPWP", "ppwp_dominan", "Kategorikal", "Apakah desa terhubung cenderung punya pemenang PPWP yang sama?"),
        ("Partai DPR Dominan", "partai_dominan", "Kategorikal", "Apakah desa terhubung cenderung punya partai dominan yang sama?"),
        ("Pendidikan Dominan", "pendidikan_dominan", "Kategorikal", "Apakah desa terhubung cenderung punya pendidikan dominan yang sama?"),
        ("Pekerjaan Dominan", "pekerjaan_dominan", "Kategorikal", "Apakah desa terhubung cenderung punya pekerjaan dominan yang sama?"),
        ("Total DPT", "pemilih_dpt_j", "Numerik", "Apakah desa terhubung cenderung punya ukuran DPT mirip?"),
        ("Partisipasi Pemilih", "partisipasi_pemilih", "Numerik", "Apakah desa terhubung cenderung punya tingkat partisipasi mirip?"),
    ]
    rows = []
    for label, attr_name, data_type, question in specs:
        if data_type == "Numerik":
            score = _safe_numeric_assortativity(graph_obj, attr_name)
        else:
            score = _safe_attribute_assortativity(graph_obj, attr_name)
        rows.append(
            {
                "Metrik": label,
                "Jenis": data_type,
                "r": score,
                "Interpretasi": interpret_assortativity(score),
                "Pertanyaan Analisis": question,
            }
        )
    return pd.DataFrame(rows)


def graph_summary(graph_obj):
    n_nodes = graph_obj.number_of_nodes()
    n_edges = graph_obj.number_of_edges()
    weights = [float(data.get("weight", 0.0)) for _, _, data in graph_obj.edges(data=True)]
    modularity = compute_modularity_score(graph_obj)
    return {
        "Node": n_nodes,
        "Edge": n_edges,
        "Density": nx.density(graph_obj) if n_nodes > 1 else 0.0,
        "Rerata Bobot": float(np.mean(weights)) if weights else 0.0,
        "Bobot Maks": float(np.max(weights)) if weights else 0.0,
        "Modularity": modularity,
        "Komponen": nx.number_connected_components(graph_obj) if n_nodes else 0,
        "Klaster": len({data.get("cluster", 0) for _, data in graph_obj.nodes(data=True)}),
    }


def build_edge_table(graph_obj, base_sims):
    rows = []
    for source, target, data in graph_obj.edges(data=True):
        rows.append(
            {
                "source": source,
                "target": target,
                "weight": float(data.get("weight", 0.0)),
                "sim_ppwp": float(base_sims["PPWP"].loc[source, target]),
                "sim_dpr": float(base_sims["DPR"].loc[source, target]),
                "sim_pendidikan": float(base_sims["Pendidikan"].loc[source, target]),
                "sim_pekerjaan": float(base_sims["Pekerjaan"].loc[source, target]),
                "sim_demografi": float(base_sims["Demografi"].loc[source, target]),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["source", "target", "weight"])
    return pd.DataFrame(rows).sort_values("weight", ascending=False).reset_index(drop=True)


def active_component_weights(network_name, politics_dpr_weight, social_weights, combined_weights, similarity_method):
    if similarity_method == "Akumulasi nilai ternormalisasi":
        if network_name == "Politik":
            return {"DPR": 1.0, "PPWP": 1.0}
        if network_name == "Sosial-Demografis":
            return {component: 1.0 for component in social_weights}
        return {component: 1.0 for component in combined_weights}
    if network_name == "Politik":
        return {
            "DPR": float(politics_dpr_weight),
            "PPWP": float(1.0 - politics_dpr_weight),
        }
    if network_name == "Sosial-Demografis":
        return dict(social_weights)
    return dict(combined_weights)


def explain_edge_drivers(edge_df, component_weights):
    if edge_df is None or edge_df.empty:
        return pd.DataFrame()
    weights = normalized_weight_dict(component_weights)
    rows = []
    for _, row in edge_df.iterrows():
        contributions = {}
        similarities = {}
        for component, weight in weights.items():
            col = SIMILARITY_COMPONENTS[component]["column"]
            sim_value = float(row.get(col, 0.0))
            similarities[component] = sim_value
            contributions[component] = float(weight) * sim_value
        total_contribution = sum(contributions.values())
        strongest_component = max(contributions, key=contributions.get) if contributions else "-"
        strongest_share = contributions.get(strongest_component, 0.0) / max(total_contribution, 1e-12)
        top_components = sorted(contributions, key=contributions.get, reverse=True)[:2]
        reason = ", ".join(
            f"{SIMILARITY_COMPONENTS[component]['label']} {similarities[component]:.2f}"
            for component in top_components
        )
        output_row = {
            "Pasangan Desa": f"{row['source']} - {row['target']}",
            "Bobot Final": float(row.get("weight", 0.0)),
            "Faktor Utama": SIMILARITY_COMPONENTS.get(strongest_component, {}).get("label", strongest_component),
            "Kontribusi Faktor Utama": strongest_share,
            "Alasan Ringkas": reason,
        }
        for component in weights:
            output_row[SIMILARITY_COMPONENTS[component]["label"]] = similarities[component]
        rows.append(output_row)
    return pd.DataFrame(rows).sort_values("Bobot Final", ascending=False).reset_index(drop=True)


def summarize_edge_drivers(edge_df, component_weights):
    if edge_df is None or edge_df.empty:
        return pd.DataFrame()
    weights = normalized_weight_dict(component_weights)
    rows = []
    for component, weight in weights.items():
        col = SIMILARITY_COMPONENTS[component]["column"]
        avg_similarity = float(pd.to_numeric(edge_df[col], errors="coerce").mean())
        rows.append(
            {
                "Komponen": SIMILARITY_COMPONENTS[component]["label"],
                "Bobot Model": float(weight),
                "Rata-rata Similarity Edge Aktif": avg_similarity,
                "Kontribusi Rata-rata": float(weight) * avg_similarity,
            }
        )
    return pd.DataFrame(rows).sort_values("Kontribusi Rata-rata", ascending=False).reset_index(drop=True)


# =========================================================
# 4. VISUALISASI
# =========================================================
def scaled_values(values, min_size=14, max_size=28):
    arr = pd.to_numeric(pd.Series(values), errors="coerce").fillna(0.0).to_numpy(dtype=float)
    if len(arr) == 0:
        return []
    lo = float(np.nanmin(arr))
    hi = float(np.nanmax(arr))
    if hi <= lo:
        return [float((min_size + max_size) / 2)] * len(arr)
    norm = (arr - lo) / (hi - lo)
    return (min_size + (max_size - min_size) * np.sqrt(norm)).tolist()


def discrete_color_map(values):
    uniques = list(dict.fromkeys([str(value) for value in values]))
    return {value: COLOR_SEQUENCE[idx % len(COLOR_SEQUENCE)] for idx, value in enumerate(uniques)}


def node_color_values(node_df, color_mode):
    if color_mode == "Klaster Louvain":
        return node_df["cluster"].map(lambda value: f"Klaster {value}")
    if color_mode == "Pemenang PPWP":
        return node_df["ppwp"]
    if color_mode == "Partai DPR Dominan":
        return node_df["partai"]
    return node_df["pekerjaan"]


def node_size_values(node_df, size_mode):
    if size_mode == "Total DPT":
        return node_df["dpt"], "DPT", node_df["dpt"].map(lambda value: f"{value:,.0f}")
    if size_mode == "Partisipasi Pemilih":
        return node_df["partisipasi"], "Partisipasi", node_df["partisipasi"].map(lambda value: f"{value * 100:.0f}%")
    if size_mode == "Weighted Degree":
        return node_df["weighted_degree"], "Weighted degree", node_df["weighted_degree"].map(lambda value: f"{value:.2f}")
    return pd.Series([1.0] * len(node_df), index=node_df.index), "Ukuran", pd.Series(["Sama"] * len(node_df), index=node_df.index)


def make_network_figure(graph_obj, title, color_mode, size_mode, centrality_df, show_selected_labels=False, height=520):
    if graph_obj.number_of_nodes() == 0:
        return go.Figure()

    if graph_obj.number_of_edges() > 0:
        pos = nx.spring_layout(graph_obj, weight="weight", seed=42, k=1.2, iterations=500)
    else:
        pos = nx.circular_layout(graph_obj)

    edge_weights = [float(data.get("weight", 0.0)) for _, _, data in graph_obj.edges(data=True)]
    edge_min = min(edge_weights) if edge_weights else 0.0
    edge_span = (max(edge_weights) - edge_min) if edge_weights else 1.0

    fig = go.Figure()
    for source, target, data in sorted(graph_obj.edges(data=True), key=lambda item: item[2].get("weight", 0), reverse=True):
        weight = float(data.get("weight", 0.0))
        weight_norm = (weight - edge_min) / max(edge_span, 1e-9)
        fig.add_trace(
            go.Scatter(
                x=[pos[source][0], pos[target][0], None],
                y=[pos[source][1], pos[target][1], None],
                mode="lines",
                line=dict(width=0.8 + 2.3 * weight_norm, color=f"rgba(71, 85, 105, {0.20 + 0.32 * weight_norm:.3f})"),
                hovertemplate=f"{source} - {target}<br>Similarity: {weight:.2f}<extra></extra>",
                showlegend=False,
            )
        )

    node_rows = []
    centrality_lookup = centrality_df.set_index("Desa").to_dict("index") if not centrality_df.empty else {}
    for node, attrs in graph_obj.nodes(data=True):
        cdata = centrality_lookup.get(node, {})
        node_rows.append(
            {
                "desa": node,
                "kode": attrs.get("kode", "-"),
                "x": pos[node][0],
                "y": pos[node][1],
                "cluster": int(attrs.get("cluster", 0)),
                "ppwp": attrs.get("ppwp_dominan", "-"),
                "partai": attrs.get("partai_dominan", "-"),
                "pekerjaan": attrs.get("pekerjaan_dominan", "-"),
                "pendidikan": attrs.get("pendidikan_dominan", "-"),
                "dpt": float(attrs.get("pemilih_dpt_j", 0.0)),
                "partisipasi": float(attrs.get("partisipasi_pemilih", 0.0)),
                "weighted_degree": float(cdata.get("Weighted Degree", 0.0)),
                "degree": float(cdata.get("Degree", 0.0)),
            }
        )
    node_df = pd.DataFrame(node_rows)

    color_values = node_color_values(node_df, color_mode)
    cmap = discrete_color_map(color_values.tolist())
    node_colors = [cmap[str(value)] for value in color_values]

    size_raw, size_label, size_text = node_size_values(node_df, size_mode)
    if size_mode == "Total DPT":
        node_sizes = scaled_values(size_raw, min_size=14, max_size=28)
    elif size_mode == "Partisipasi Pemilih":
        node_sizes = scaled_values(size_raw, min_size=14, max_size=28)
    elif size_mode == "Weighted Degree":
        node_sizes = scaled_values(size_raw, min_size=14, max_size=28)
    else:
        node_sizes = [18] * len(node_df)
    visible_labels = (
        node_df["desa"] + "<br>" + color_values.astype(str) + " | " + size_text.astype(str)
        if show_selected_labels
        else node_df["desa"]
    )

    customdata = np.column_stack(
        [
            node_df["kode"],
            node_df["cluster"],
            node_df["ppwp"],
            node_df["partai"],
            node_df["pekerjaan"],
            node_df["pendidikan"],
            node_df["dpt"].map(lambda value: f"{value:,.0f}"),
            node_df["partisipasi"].map(lambda value: f"{value * 100:.0f}%"),
            node_df["weighted_degree"].map(lambda value: f"{value:.2f}"),
            color_values.astype(str),
            size_text.astype(str),
        ]
    )

    fig.add_trace(
        go.Scatter(
            x=node_df["x"],
            y=node_df["y"],
            mode="markers+text",
            text=visible_labels,
            textposition="top center",
            textfont=dict(size=9 if show_selected_labels else 10, color="#0F172A", family=PUBLICATION_FONT),
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(width=1.0, color="#0F172A"),
                opacity=0.94,
            ),
            customdata=customdata,
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Kode deskel: %{customdata[0]}<br>"
                f"Warna node ({color_mode}): " + "%{customdata[9]}<br>"
                f"Ukuran node ({size_mode}): " + "%{customdata[10]}<br>"
                "Klaster: %{customdata[1]}<br>"
                "PPWP dominan: %{customdata[2]}<br>"
                "Partai dominan: %{customdata[3]}<br>"
                "Pekerjaan dominan: %{customdata[4]}<br>"
                "Pendidikan dominan: %{customdata[5]}<br>"
                "DPT: %{customdata[6]}<br>"
                "Partisipasi: %{customdata[7]}<br>"
                "Weighted degree: %{customdata[8]}"
                "<extra></extra>"
            ),
            showlegend=False,
        )
    )

    if show_selected_labels:
        legend_rows = pd.DataFrame({"label": color_values.astype(str), "color": node_colors}).drop_duplicates()
        for _, row in legend_rows.iterrows():
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode="markers",
                    marker=dict(size=10, color=row["color"], line=dict(width=1, color="#0F172A")),
                    name=str(row["label"]),
                    hoverinfo="skip",
                )
            )
    return style_network_figure(fig, title=title, height=height, showlegend=show_selected_labels)


def make_similarity_heatmap(sim_df, title):
    fig = px.imshow(
        sim_df,
        zmin=0,
        zmax=1,
        color_continuous_scale=CONTINUOUS_SCALE,
        text_auto=".2f",
        aspect="auto",
    )
    fig.update_traces(textfont=dict(size=10))
    style_publication_figure(fig, title=title, height=460, margin=dict(l=48, r=24, t=72, b=48))
    fig.update_xaxes(side="top", tickangle=-35)
    fig.update_layout(coloraxis_colorbar=dict(title="Similarity"))
    return fig


def make_centrality_bar(centrality_df, metric):
    plot_df = centrality_df.sort_values(metric, ascending=True)
    fig = px.bar(
        plot_df,
        x=metric,
        y="Desa",
        orientation="h",
        color="Klaster",
        color_continuous_scale=CONTINUOUS_SCALE,
        hover_data=["Pemenang PPWP", "Partai Dominan", "Pekerjaan Dominan"],
    )
    return style_publication_figure(
        fig,
        title=f"Peringkat Desa Menurut {metric}",
        height=420,
        xaxis_title=metric,
        yaxis_title="",
        showlegend=False,
    )


def make_centrality_pie(centrality_df, color_mode):
    if centrality_df.empty:
        return go.Figure()
    if color_mode == "Klaster Louvain":
        values = centrality_df["Klaster"].map(lambda value: f"Klaster {value}")
        title = "Komposisi Node Menurut Klaster"
    elif color_mode == "Pemenang PPWP":
        values = centrality_df["Pemenang PPWP"]
        title = "Komposisi Node Menurut Pemenang PPWP"
    elif color_mode == "Partai DPR Dominan":
        values = centrality_df["Partai Dominan"]
        title = "Komposisi Node Menurut Partai Dominan"
    else:
        values = centrality_df["Pekerjaan Dominan"]
        title = "Komposisi Node Menurut Pekerjaan Dominan"
    plot_df = values.value_counts().rename_axis("Kategori").reset_index(name="Jumlah")
    fig = px.pie(
        plot_df,
        names="Kategori",
        values="Jumlah",
        color_discrete_sequence=COLOR_SEQUENCE,
        hole=0.35,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return style_publication_figure(fig, title=title, height=360, showlegend=True, margin=dict(l=18, r=18, t=68, b=18))


def make_centrality_grouped_bar(centrality_df):
    if centrality_df.empty:
        return go.Figure()
    metrics = ["Weighted Degree", "Degree", "Betweenness", "Closeness", "Eigenvector"]
    plot_df = centrality_df[["Desa", *metrics]].head(10).melt(id_vars="Desa", var_name="Metrik", value_name="Nilai")
    fig = px.bar(
        plot_df,
        x="Desa",
        y="Nilai",
        color="Metrik",
        barmode="group",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_xaxes(tickangle=-35)
    return style_publication_figure(fig, title="Perbandingan Metrik Centrality Top 10", height=430, xaxis_title="", yaxis_title="Nilai")


def make_centrality_scatter(centrality_df, metric):
    if centrality_df.empty:
        return go.Figure()
    fig = px.scatter(
        centrality_df,
        x="Degree",
        y=metric,
        size="Weighted Degree",
        color="Klaster",
        text="Desa",
        color_continuous_scale=CONTINUOUS_SCALE,
        hover_data=["Pemenang PPWP", "Partai Dominan", "Pekerjaan Dominan"],
    )
    fig.update_traces(textposition="top center")
    return style_publication_figure(
        fig,
        title=f"Hubungan Degree dan {metric}",
        height=390,
        xaxis_title="Degree",
        yaxis_title=metric,
        showlegend=False,
    )


def make_centrality_histogram(centrality_df, metric):
    if centrality_df.empty:
        return go.Figure()
    plot_df = centrality_df.copy()
    plot_df["Klaster Label"] = plot_df["Klaster"].map(lambda value: f"Klaster {value}")
    fig = px.histogram(
        plot_df,
        x=metric,
        nbins=min(8, max(3, len(centrality_df))),
        color="Klaster Label",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    return style_publication_figure(
        fig,
        title=f"Distribusi {metric}",
        height=360,
        xaxis_title=metric,
        yaxis_title="Jumlah Desa",
        showlegend=False,
    )


def upper_triangle_values(sim_df):
    values = []
    labels = sim_df.index.tolist()
    for i, source in enumerate(labels):
        for j in range(i + 1, len(labels)):
            target = labels[j]
            values.append({"source": source, "target": target, "value": float(sim_df.iloc[i, j])})
    return pd.DataFrame(values)


def make_similarity_scatter(politic_sim, social_sim):
    pol = upper_triangle_values(politic_sim).rename(columns={"value": "Similarity Politik"})
    soc = upper_triangle_values(social_sim).rename(columns={"value": "Similarity Sosial-Demografis"})
    pairs = pol.merge(soc, on=["source", "target"])
    pairs["Pasangan Desa"] = pairs["source"] + " - " + pairs["target"]
    fig = px.scatter(
        pairs,
        x="Similarity Sosial-Demografis",
        y="Similarity Politik",
        text="Pasangan Desa",
        color="Similarity Politik",
        color_continuous_scale=CONTINUOUS_SCALE,
        hover_name="Pasangan Desa",
    )
    x_vals = pairs["Similarity Sosial-Demografis"].to_numpy(dtype=float)
    y_vals = pairs["Similarity Politik"].to_numpy(dtype=float)
    if len(pairs) >= 3 and np.std(x_vals) > 1e-12 and np.std(y_vals) > 1e-12:
        slope, intercept = np.polyfit(x_vals, y_vals, 1)
        line_x = np.linspace(float(np.min(x_vals)), float(np.max(x_vals)), 80)
        line_y = slope * line_x + intercept
        fig.add_trace(
            go.Scatter(
                x=line_x,
                y=line_y,
                mode="lines",
                line=dict(color="#0F172A", width=2, dash="dash"),
                hovertemplate="Tren linear<extra></extra>",
                showlegend=False,
            )
        )
    fig.update_traces(textposition="top center", selector=dict(mode="markers+text"))
    return style_publication_figure(
        fig,
        title="Kesesuaian Similarity Politik dan Sosial-Demografis",
        height=460,
        xaxis_title="Similarity Sosial-Demografis",
        yaxis_title="Similarity Politik",
        showlegend=False,
    )


def make_profile_bar(df_plot, x_col, y_col, title, xaxis_title="", yaxis_title=""):
    fig = px.bar(df_plot, x=x_col, y=y_col, color=y_col, color_continuous_scale=CONTINUOUS_SCALE)
    return style_publication_figure(
        fig,
        title=title,
        height=390,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        showlegend=False,
        margin=dict(l=48, r=20, t=68, b=80),
    )


def make_party_share_frame(profile_df, dpr_cols):
    return row_normalize(profile_df.set_index("desa")[dpr_cols])


def make_ppwp_share_frame(profile_df):
    return row_normalize(profile_df.set_index("desa")[PPWP_COLS])


def shorten_party_name(value):
    text = str(value)
    replacements = {
        "Partai Kebangkitan Bangsa": "PKB",
        "Partai Demokrasi Indonesia Perjuangan": "PDIP",
        "Partai Golongan Karya": "Golkar",
        "Partai Gerakan Indonesia Raya": "Gerindra",
        "Partai Nasional Demokrat": "NasDem",
        "Partai Keadilan Sejahtera": "PKS",
        "Partai Amanat Nasional": "PAN",
        "Partai Persatuan Pembangunan": "PPP",
        "Partai ": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def make_party_share_heatmap(profile_df, dpr_cols):
    share = make_party_share_frame(profile_df, dpr_cols)
    plot_df = share.rename(columns={col: shorten_party_name(col) for col in share.columns})
    fig = px.imshow(
        plot_df,
        zmin=0,
        zmax=max(0.25, float(plot_df.max().max()) if not plot_df.empty else 1.0),
        color_continuous_scale=CONTINUOUS_SCALE,
        text_auto=".0%",
        aspect="auto",
    )
    fig.update_traces(textfont=dict(size=9))
    style_publication_figure(fig, title="Heatmap Share Partai per Desa", height=520, margin=dict(l=64, r=24, t=72, b=68))
    fig.update_xaxes(side="top", tickangle=-35)
    fig.update_layout(coloraxis_colorbar=dict(title="Share"))
    return fig


def make_party_bipartite_network(profile_df, dpr_cols, min_share=0.10, top_n_parties=10):
    share = make_party_share_frame(profile_df, dpr_cols)
    top_parties = share.mean(axis=0).sort_values(ascending=False).head(int(top_n_parties)).index.tolist()
    graph_obj = nx.Graph()
    villages = share.index.tolist()
    party_labels = {party: shorten_party_name(party) for party in top_parties}

    for desa in villages:
        graph_obj.add_node(desa, node_type="Desa", label=desa)
    for party in top_parties:
        graph_obj.add_node(party, node_type="Partai", label=party_labels[party])
    for desa in villages:
        for party in top_parties:
            value = float(share.loc[desa, party])
            if value >= float(min_share):
                graph_obj.add_edge(desa, party, weight=value)

    pos = {}
    y_villages = np.linspace(1, -1, max(len(villages), 1))
    y_parties = np.linspace(1, -1, max(len(top_parties), 1))
    for idx, desa in enumerate(villages):
        pos[desa] = (-1.0, y_villages[idx])
    for idx, party in enumerate(top_parties):
        pos[party] = (1.0, y_parties[idx])

    fig = go.Figure()
    weights = [float(data.get("weight", 0.0)) for _, _, data in graph_obj.edges(data=True)]
    low = min(weights) if weights else 0.0
    span = (max(weights) - low) if weights else 1.0
    for source, target, data in graph_obj.edges(data=True):
        weight = float(data.get("weight", 0.0))
        norm = (weight - low) / max(span, 1e-9)
        fig.add_trace(
            go.Scatter(
                x=[pos[source][0], pos[target][0], None],
                y=[pos[source][1], pos[target][1], None],
                mode="lines",
                line=dict(width=0.7 + 3.2 * norm, color=f"rgba(71, 85, 105, {0.22 + 0.42 * norm:.3f})"),
                hovertemplate=f"{source} - {graph_obj.nodes[target]['label']}<br>Share: {weight:.0%}<extra></extra>",
                showlegend=False,
            )
        )

    for node_type, color, size in [("Desa", "#2563EB", 12), ("Partai", "#B91C1C", 15)]:
        nodes = [node for node, attrs in graph_obj.nodes(data=True) if attrs.get("node_type") == node_type]
        fig.add_trace(
            go.Scatter(
                x=[pos[node][0] for node in nodes],
                y=[pos[node][1] for node in nodes],
                mode="markers+text",
                text=[graph_obj.nodes[node]["label"] for node in nodes],
                textposition="middle right" if node_type == "Desa" else "middle left",
                marker=dict(size=size, color=color, line=dict(width=1, color="#0F172A")),
                name=node_type,
                hovertemplate="%{text}<extra></extra>",
            )
        )
    return style_network_figure(fig, title=f"Bipartite Network Desa-Partai (share >= {min_share:.0%})", height=560, showlegend=True)


def make_party_correlation_heatmap(profile_df, dpr_cols, feature_matrices):
    party_share = make_party_share_frame(profile_df, dpr_cols)
    edu = feature_matrices["Pendidikan"].rename(columns=lambda col: strip_prefix(col, "edu_"))
    top_job_cols = profile_df[feature_matrices["Pekerjaan"].columns].sum(axis=0).sort_values(ascending=False).head(8).index
    job = feature_matrices["Pekerjaan"][top_job_cols].rename(columns=lambda col: title_job(strip_prefix(col, "job_")))
    demo = feature_matrices["Demografi"].rename(
        columns={
            "rasio_dpt_perempuan": "Rasio DPT Perempuan",
            "rasio_pengguna_perempuan": "Rasio Pengguna Perempuan",
            "partisipasi_pemilih": "Partisipasi Pemilih",
        }
    )
    features = pd.concat([edu, job, demo], axis=1)
    rows = []
    for party in party_share.columns:
        row = {}
        party_values = party_share[party].to_numpy(dtype=float)
        for feature in features.columns:
            feature_values = features[feature].to_numpy(dtype=float)
            if np.std(party_values) <= 1e-12 or np.std(feature_values) <= 1e-12:
                row[feature] = 0.0
            else:
                row[feature] = float(np.corrcoef(party_values, feature_values)[0, 1])
        rows.append(pd.Series(row, name=shorten_party_name(party)))
    corr_df = pd.DataFrame(rows)
    fig = px.imshow(
        corr_df,
        zmin=-1,
        zmax=1,
        color_continuous_scale="RdBu",
        text_auto=".2f",
        aspect="auto",
    )
    fig.update_traces(textfont=dict(size=8))
    style_publication_figure(fig, title="Korelasi Share Partai dengan Pendidikan, Pekerjaan, dan Pemilih", height=560, margin=dict(l=72, r=24, t=72, b=92))
    fig.update_xaxes(tickangle=-35)
    fig.update_layout(coloraxis_colorbar=dict(title="r"))
    return fig


def make_ppwp_party_scatter(profile_df, dpr_cols, selected_ppwp, selected_party):
    ppwp_share = make_ppwp_share_frame(profile_df)
    party_share = make_party_share_frame(profile_df, dpr_cols)
    plot_df = pd.DataFrame(
        {
            "Desa": ppwp_share.index,
            "Share PPWP": ppwp_share[selected_ppwp].to_numpy(dtype=float),
            "Share Partai": party_share[selected_party].to_numpy(dtype=float),
            "Partai Dominan": profile_df.set_index("desa").loc[ppwp_share.index, "partai_dominan"].map(shorten_party_name),
        }
    )
    fig = px.scatter(
        plot_df,
        x="Share PPWP",
        y="Share Partai",
        text="Desa",
        color="Partai Dominan",
        size="Share Partai",
        color_discrete_sequence=COLOR_SEQUENCE,
        hover_name="Desa",
    )
    fig.update_traces(textposition="top center")
    return style_publication_figure(
        fig,
        title=f"Scatter {selected_ppwp} vs {shorten_party_name(selected_party)}",
        height=430,
        xaxis_title=f"Share {selected_ppwp}",
        yaxis_title=f"Share {shorten_party_name(selected_party)}",
        showlegend=True,
    )


def make_hhi_party_chart(profile_df, dpr_cols):
    share = make_party_share_frame(profile_df, dpr_cols)
    plot_df = pd.DataFrame({"Desa": share.index, "HHI": (share**2).sum(axis=1).to_numpy(dtype=float)})
    plot_df["Kategori"] = pd.cut(
        plot_df["HHI"],
        bins=[-0.01, 0.10, 0.18, 1.01],
        labels=["Kompetitif", "Sedang", "Terkonsentrasi"],
    )
    plot_df = plot_df.sort_values("HHI", ascending=True)
    fig = px.bar(
        plot_df,
        x="HHI",
        y="Desa",
        color="Kategori",
        orientation="h",
        color_discrete_sequence=["#0F766E", "#D97706", "#B91C1C"],
    )
    return style_publication_figure(fig, title="HHI Konsentrasi Partai per Desa", height=430, xaxis_title="HHI", yaxis_title="", showlegend=True)


def make_party_penetration_chart(profile_df, dpr_cols):
    share = make_party_share_frame(profile_df, dpr_cols)
    thresholds = [0.05, 0.10, 0.20]
    rows = []
    for party in share.columns:
        for threshold in thresholds:
            rows.append(
                {
                    "Partai": shorten_party_name(party),
                    "Ambang Share": f">{threshold:.0%}",
                    "Jumlah Desa": int((share[party] >= threshold).sum()),
                }
            )
    plot_df = pd.DataFrame(rows)
    order = share.mean(axis=0).sort_values(ascending=False).index.map(shorten_party_name).tolist()
    fig = px.bar(
        plot_df,
        x="Partai",
        y="Jumlah Desa",
        color="Ambang Share",
        barmode="group",
        category_orders={"Partai": order, "Ambang Share": [">5%", ">10%", ">20%"]},
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_xaxes(tickangle=-35)
    return style_publication_figure(fig, title="Party Penetration Chart", height=430, xaxis_title="", yaxis_title="Jumlah Desa")


def get_profile_series(profile_row, cols, prefix="", normalize=False, top_n=None):
    values = profile_row[cols].astype(float)
    if normalize:
        total = float(values.sum())
        values = values / total if total > 0 else values * 0
    if top_n:
        values = values.sort_values(ascending=False).head(top_n)
    labels = [title_job(strip_prefix(col, prefix)) if prefix == "job_" else strip_prefix(col, prefix) for col in values.index]
    return pd.DataFrame({"Kategori": labels, "Nilai": values.to_numpy(dtype=float)})


def render_metric_row(summary, centrality_df):
    top_node = centrality_df.iloc[0]["Desa"] if not centrality_df.empty else "-"
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Node Desa", f"{summary['Node']:.0f}")
    m2.metric("Edge", f"{summary['Edge']:.0f}")
    m3.metric("Density", f"{summary['Density']:.2f}")
    m4.metric("Rerata Bobot", f"{summary['Rerata Bobot']:.2f}")
    m5.metric("Modularity", f"{summary['Modularity']:.2f}")
    m6.metric("Node Sentral", str(top_node))


def build_network_outputs(profile_df, network_sims, base_sims, edge_mode, top_k, threshold, threshold_sims=None):
    outputs = {}
    for network_name, sim_df in network_sims.items():
        threshold_sim_df = threshold_sims.get(network_name) if threshold_sims else None
        graph_obj = build_graph(
            profile_df,
            sim_df,
            network_name,
            edge_mode,
            top_k=top_k,
            threshold=threshold,
            threshold_sim_df=threshold_sim_df,
        )
        centrality_df = compute_centrality_table(graph_obj)
        edge_df = build_edge_table(graph_obj, base_sims)
        outputs[network_name] = {
            "graph": graph_obj,
            "centrality": centrality_df,
            "edge_table": edge_df,
            "summary": graph_summary(graph_obj),
        }
    return outputs


def normalized_weight_dict(raw_weights):
    total = sum(max(float(value), 0.0) for value in raw_weights.values())
    if total <= 0:
        return {key: 1.0 / len(raw_weights) for key in raw_weights}
    return {key: max(float(value), 0.0) / total for key, value in raw_weights.items()}


# =========================================================
# 5. DASHBOARD
# =========================================================
inject_css()

with st.sidebar:
    logo_col, title_col = st.columns([1, 3], gap="small")
    with logo_col:
        logo_html = (
            f"<img src='{LOGO_DATA_URI}' class='sidebar-logo-img' alt='Logo SNA'/>"
            if LOGO_DATA_URI
            else "<div class='sidebar-logo-fallback'>SNA</div>"
        )
        st.markdown(f"<div class='sidebar-logo-shell'>{logo_html}</div>", unsafe_allow_html=True)
    with title_col:
        st.markdown(
            "<div style='padding-top:7px; font-size:1.02rem; font-weight:800; color:#F8FAFC;'>SNA Desa Batu Putih</div>"
            "<div style='font-size:0.82rem; color:#CBD5E1;'>Network politik dan sosial desa</div>",
            unsafe_allow_html=True,
        )

    st.divider()
    uploaded_file = st.file_uploader("Unggah Excel Batu Putih", type=["xlsx", "xls"])
    selected_network = st.selectbox("Jenis Network", NETWORK_OPTIONS, index=2)
    similarity_method = st.selectbox("Metode Similarity", SIMILARITY_METHOD_OPTIONS, index=0)
    edge_mode = st.radio("Filter Edge", ["Top-k per desa", "Threshold"], index=0)
    if edge_mode == "Top-k per desa":
        top_k = st.slider("Top-k Tetangga", min_value=1, max_value=5, value=3, step=1)
        threshold = 0.90
    else:
        threshold = st.slider("Threshold Similarity", min_value=0.50, max_value=0.99, value=0.90, step=0.01)
        if similarity_method == "Akumulasi nilai ternormalisasi":
            st.caption("Threshold diseleksi memakai skor berbobot; bobot garis tetap memakai akumulasi ternormalisasi.")
        top_k = 3

    color_mode = st.selectbox(
        "Warna Node",
        ["Klaster Louvain", "Pemenang PPWP", "Partai DPR Dominan", "Pekerjaan Dominan"],
        index=0,
    )
    size_mode = st.selectbox(
        "Ukuran Node",
        ["Ukuran Sama", "Weighted Degree", "Total DPT", "Partisipasi Pemilih"],
        index=0,
    )
    centrality_metric = st.selectbox(
        "Metrik Centrality",
        ["Weighted Degree", "Degree", "Betweenness", "Closeness", "Eigenvector"],
        index=0,
    )

    with st.expander("Bobot Similarity", expanded=False):
        if similarity_method == "Akumulasi nilai ternormalisasi":
            st.caption("Pada metode akumulasi, bobot edge memakai rata-rata komponen aktif. Bobot di bawah hanya dipakai untuk seleksi saat filter Threshold.")
        politics_dpr_weight = st.slider("Politik: Bobot DPR", 0.0, 1.0, 0.70, 0.05)
        st.caption(f"Bobot PPWP otomatis: {1 - politics_dpr_weight:.2f}")
        social_raw = {
            "Pendidikan": st.slider("Sosial: Pendidikan", 0.0, 1.0, 0.45, 0.05),
            "Pekerjaan": st.slider("Sosial: Pekerjaan", 0.0, 1.0, 0.45, 0.05),
            "Demografi": st.slider("Sosial: DPT/Partisipasi", 0.0, 1.0, 0.10, 0.05),
        }
        combined_raw = {
            "DPR": st.slider("Gabungan: DPR", 0.0, 1.0, 0.50, 0.05),
            "PPWP": st.slider("Gabungan: PPWP", 0.0, 1.0, 0.25, 0.05),
            "Pendidikan": st.slider("Gabungan: Pendidikan", 0.0, 1.0, 0.15, 0.05),
            "Pekerjaan": st.slider("Gabungan: Pekerjaan", 0.0, 1.0, 0.10, 0.05),
        }

social_weights = normalized_weight_dict(social_raw)
combined_weights = normalized_weight_dict(combined_raw)

render_global_header()
st.markdown("<h1 class='main-header'>Dashboard SNA Desa/Kelurahan Batu Putih</h1>", unsafe_allow_html=True)

uploaded_bytes = uploaded_file.getvalue() if uploaded_file is not None else None
if uploaded_file is None and not DEFAULT_DATA_PATH.exists():
    st.error("File `batu_putih.xlsx` belum ditemukan di folder proyek.")
    st.stop()

try:
    profile_df, meta = load_batu_putih_workbook(uploaded_bytes)
except Exception as exc:
    st.error(f"Gagal membaca data Excel: {exc}")
    st.stop()

base_sims, network_sims, threshold_sims, feature_matrices = build_similarity_bundle(
    profile_df,
    meta,
    politics_dpr_weight=politics_dpr_weight,
    social_weights=social_weights,
    combined_weights=combined_weights,
    similarity_method=similarity_method,
)
network_outputs = build_network_outputs(
    profile_df,
    network_sims,
    base_sims,
    edge_mode,
    top_k,
    threshold,
    threshold_sims=threshold_sims if similarity_method == "Akumulasi nilai ternormalisasi" else None,
)
active = network_outputs[selected_network]
active_graph = active["graph"]
active_centrality = active["centrality"]
active_summary = active["summary"]
active_weights = active_component_weights(
    selected_network,
    politics_dpr_weight,
    social_weights,
    combined_weights,
    similarity_method,
)
active_assortativity = build_assortativity_table(active_graph)
active_edge_drivers = explain_edge_drivers(active["edge_table"], active_weights)
active_driver_summary = summarize_edge_drivers(active["edge_table"], active_weights)

render_metric_row(active_summary, active_centrality)

tabs = st.tabs(
    [
        "Visual Network",
        "Modularity & Assortativity",
        "Perbandingan Network",
        "Centrality",
        "Profil Desa",
        "Analisis Partai",
        "Edge List & Data",
    ]
)

with tabs[0]:
    left, right = st.columns([2.15, 1], gap="large")
    with left:
        st.caption(f"Node pada graf ini hanya desa/kelurahan: {active_graph.number_of_nodes()} node dari kode deskel unik.")
        fig_network = make_network_figure(
            active_graph,
            title=f"Network {selected_network} - {edge_mode}",
            color_mode=color_mode,
            size_mode=size_mode,
            centrality_df=active_centrality,
        )
        st.plotly_chart(fig_network, width="stretch", config=PLOTLY_DRAW_CONFIG)
    with right:
        st.markdown("<div class='section-title'>Ringkasan Network</div>", unsafe_allow_html=True)
        summary_df = pd.DataFrame([active_summary]).T.rename(columns={0: "Nilai"})
        st.dataframe(summary_df, width="stretch")
        if not active["edge_table"].empty:
            strongest = active["edge_table"].iloc[0]
            st.markdown(
                f"""
                <div class="analysis-note">
                    Setiap titik adalah satu desa/kelurahan, sedangkan garis menunjukkan kemiripan profil antar desa.
                    Semakin tebal garis, semakin mirip pasangan desa tersebut.<br><br>
                    Relasi terkuat pada network ini adalah <b>{strongest['source']} - {strongest['target']}</b>
                    dengan bobot similarity <b>{strongest['weight']:.2f}</b>.
                    Desa paling sentral menurut weighted degree adalah <b>{active_centrality.iloc[0]['Desa']}</b>.
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.markdown("<div class='section-title'>Similarity Matrix</div>", unsafe_allow_html=True)
        st.plotly_chart(
            make_similarity_heatmap(network_sims[selected_network], f"Matriks Similarity {selected_network}"),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )

with tabs[1]:
    st.markdown("<div class='section-title'>Modularity Louvain</div>", unsafe_allow_html=True)
    q1, q2 = st.columns([0.85, 1.35], gap="large")
    with q1:
        st.metric("Modularity (Q)", f"{active_summary['Modularity']:.2f}")
        st.caption(interpret_modularity(active_summary["Modularity"]))
    with q2:
        st.markdown(
            """
            <div class="analysis-note">
                Modularity membaca seberapa jelas pembagian klaster Louvain. Nilai makin tinggi berarti edge
                lebih banyak terkumpul di dalam kelompok desa yang sama dibanding antar kelompok.
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div class='section-title'>Assortativity: Desa Terhubung Cenderung Sama Dalam Hal Apa?</div>", unsafe_allow_html=True)
    a1, a2 = st.columns([1.35, 1], gap="large")
    with a1:
        st.dataframe(
            active_assortativity.style.format({"r": "{:.2f}"}),
            width="stretch",
            height=330,
        )
    with a2:
        st.markdown(
            """
            <div class="analysis-note">
                Saran tema paling bagus untuk dibaca:<br>
                <b>Network Politik</b>: cek apakah desa yang mirip politik juga sama pekerjaan atau pendidikannya.<br>
                <b>Network Sosial-Demografis</b>: cek apakah desa yang mirip sosial juga punya pemenang PPWP atau partai dominan yang sama.<br>
                <b>Network Gabungan</b>: cek faktor mana yang paling sering menjadi alasan edge terbentuk.
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div class='section-title'>Faktor Pembentuk Edge Aktif</div>", unsafe_allow_html=True)
    f1, f2 = st.columns([1, 1.45], gap="large")
    with f1:
        if active_driver_summary.empty:
            st.info("Belum ada edge aktif untuk diringkas.")
        else:
            fig_driver = px.bar(
                active_driver_summary,
                x="Komponen",
                y="Kontribusi Rata-rata",
                color="Rata-rata Similarity Edge Aktif",
                color_continuous_scale=CONTINUOUS_SCALE,
                text=active_driver_summary["Kontribusi Rata-rata"].map(lambda value: f"{value:.2f}"),
            )
            st.plotly_chart(
                style_publication_figure(
                    fig_driver,
                    title="Rata-rata Kontribusi Komponen pada Edge",
                    height=360,
                    xaxis_title="",
                    yaxis_title="Kontribusi",
                    showlegend=False,
                ),
                width="stretch",
                config=PLOTLY_DRAW_CONFIG,
            )
    with f2:
        if active_edge_drivers.empty:
            st.info("Belum ada edge aktif untuk dijelaskan.")
        else:
            driver_format = {
                key: value
                for key, value in {
                    "Bobot Final": "{:.2f}",
                    "Kontribusi Faktor Utama": "{:.0%}",
                    "Suara DPR": "{:.2f}",
                    "Suara PPWP": "{:.2f}",
                    "Pendidikan": "{:.2f}",
                    "Pekerjaan": "{:.2f}",
                    "DPT/Partisipasi": "{:.2f}",
                }.items()
                if key in active_edge_drivers.columns
            }
            st.dataframe(
                active_edge_drivers.style.format(driver_format),
                width="stretch",
                height=360,
            )


with tabs[2]:
    summary_rows = []
    for name in NETWORK_OPTIONS:
        row = {"Network": name, **network_outputs[name]["summary"]}
        summary_rows.append(row)
    comparison_df = pd.DataFrame(summary_rows)
    c1, c2 = st.columns([1.05, 1.25], gap="large")
    with c1:
        st.markdown("<div class='section-title'>Perbandingan Struktur Graf</div>", unsafe_allow_html=True)
        st.dataframe(
            comparison_df.style.format(
                {
                    "Density": "{:.2f}",
                    "Rerata Bobot": "{:.2f}",
                    "Bobot Maks": "{:.2f}",
                    "Modularity": "{:.2f}",
                }
            ),
            width="stretch",
        )
        fig_summary = px.bar(
            comparison_df,
            x="Network",
            y=["Edge", "Klaster", "Komponen"],
            barmode="group",
            color_discrete_sequence=COLOR_SEQUENCE,
        )
        st.plotly_chart(
            style_publication_figure(fig_summary, title="Edge, Klaster, dan Komponen", height=420, yaxis_title="Jumlah"),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )
    with c2:
        pol_vals = upper_triangle_values(network_sims["Politik"])["value"]
        soc_vals = upper_triangle_values(network_sims["Sosial-Demografis"])["value"]
        if pol_vals.std() > 1e-12 and soc_vals.std() > 1e-12:
            alignment = float(np.corrcoef(pol_vals, soc_vals)[0, 1])
        else:
            alignment = 0.0
        st.metric("Korelasi Similarity Politik vs Sosial", f"{alignment:.2f}")
        st.plotly_chart(
            make_similarity_scatter(network_sims["Politik"], network_sims["Sosial-Demografis"]),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )

    st.markdown("<div class='section-title'>Pasangan Desa Paling Konsisten</div>", unsafe_allow_html=True)
    pair_table = upper_triangle_values(network_sims["Politik"]).rename(columns={"value": "Politik"})
    pair_table = pair_table.merge(
        upper_triangle_values(network_sims["Sosial-Demografis"]).rename(columns={"value": "Sosial-Demografis"}),
        on=["source", "target"],
    )
    pair_table = pair_table.merge(
        upper_triangle_values(network_sims["Gabungan"]).rename(columns={"value": "Gabungan"}),
        on=["source", "target"],
    )
    pair_table["Selisih Politik-Sosial"] = (pair_table["Politik"] - pair_table["Sosial-Demografis"]).abs()
    pair_table["Pasangan Desa"] = pair_table["source"] + " - " + pair_table["target"]
    st.dataframe(
        pair_table[
            ["Pasangan Desa", "Politik", "Sosial-Demografis", "Gabungan", "Selisih Politik-Sosial"]
        ]
        .sort_values(["Gabungan", "Selisih Politik-Sosial"], ascending=[False, True])
        .head(15)
        .style.format(
            {
                "Politik": "{:.2f}",
                "Sosial-Demografis": "{:.2f}",
                "Gabungan": "{:.2f}",
                "Selisih Politik-Sosial": "{:.2f}",
            }
        ),
        width="stretch",
    )

with tabs[3]:
    st.markdown("<div class='section-title'>Centrality Desa</div>", unsafe_allow_html=True)
    st.plotly_chart(
        make_network_figure(
            active_graph,
            title=f"Graf Centrality {selected_network} - warna: {color_mode}, ukuran: {size_mode}",
            color_mode=color_mode,
            size_mode=size_mode,
            centrality_df=active_centrality,
            show_selected_labels=True,
            height=640,
        ),
        width="stretch",
        config=PLOTLY_DRAW_CONFIG,
    )
    st.markdown(
        """
        <div class="analysis-note">
            Label node mengikuti pilihan visual di sidebar: baris pertama adalah nama desa, baris kedua menunjukkan kategori warna
            dan nilai ukuran node. Layout memakai spring layout berbobot sehingga edge similarity yang lebih kuat menarik node lebih dekat.
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns([1.25, 1], gap="large")
    with c1:
        st.plotly_chart(
            make_centrality_bar(active_centrality, centrality_metric),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )
    with c2:
        st.plotly_chart(
            make_centrality_pie(active_centrality, color_mode),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )

    c3, c4 = st.columns([1.1, 1], gap="large")
    with c3:
        st.plotly_chart(
            make_centrality_grouped_bar(active_centrality),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )
    with c4:
        st.plotly_chart(
            make_centrality_histogram(active_centrality, centrality_metric),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )

    st.plotly_chart(
        make_centrality_scatter(active_centrality, centrality_metric),
        width="stretch",
        config=PLOTLY_DRAW_CONFIG,
    )

    st.markdown("<div class='section-title'>Tabel Centrality</div>", unsafe_allow_html=True)
    st.dataframe(
        active_centrality[
            [
                "Desa",
                "Klaster",
                "Degree",
                "Weighted Degree",
                "Betweenness",
                "Closeness",
                "Eigenvector",
                "Pemenang PPWP",
                "Partai Dominan",
            ]
        ].style.format(
            {
                "Degree": "{:.0f}",
                "Weighted Degree": "{:.2f}",
                "Betweenness": "{:.2f}",
                "Closeness": "{:.2f}",
                "Eigenvector": "{:.2f}",
            }
        ),
        width="stretch",
        height=420,
    )
    with st.expander("Data lengkap centrality", expanded=False):
        st.dataframe(
            active_centrality[
                [
                    "Desa",
                    "Klaster",
                    "Degree",
                    "Weighted Degree",
                    "Betweenness",
                    "Closeness",
                    "Eigenvector",
                    "Pemenang PPWP",
                    "Partai Dominan",
                ]
            ].style.format(
                {
                    "Degree": "{:.0f}",
                    "Weighted Degree": "{:.2f}",
                    "Betweenness": "{:.2f}",
                    "Closeness": "{:.2f}",
                    "Eigenvector": "{:.2f}",
                }
            ),
            width="stretch",
            height=480,
        )
    st.markdown(
        """
        <div class="analysis-note">
            Weighted degree membaca desa yang paling banyak dan paling kuat kemiripannya dengan desa lain.
            Betweenness menunjukkan desa yang cenderung menjadi jembatan antar kelompok similarity.
        </div>
        """,
        unsafe_allow_html=True,
    )

with tabs[4]:
    selected_desa = st.selectbox("Pilih Desa untuk Profil Detail", profile_df["desa"].tolist())
    row = profile_df[profile_df["desa"] == selected_desa].iloc[0]
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("DPT", f"{row['pemilih_dpt_j']:,.0f}")
    p2.metric("Pengguna Hak Pilih", f"{row['pengguna_total']:,.0f}")
    p3.metric("Partisipasi", format_pct(row["partisipasi_pemilih"]))
    p4.metric("Total DPR Sah", f"{row['Total DPR']:,.0f}")

    g1, g2 = st.columns(2, gap="large")
    with g1:
        ppwp_plot = get_profile_series(row, PPWP_COLS, normalize=True)
        st.plotly_chart(
            make_profile_bar(ppwp_plot, "Kategori", "Nilai", f"Proporsi PPWP - {selected_desa}", yaxis_title="Proporsi"),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )
        edu_plot = get_profile_series(row, meta["edu_count_cols"], prefix="edu_", normalize=True)
        st.plotly_chart(
            make_profile_bar(edu_plot, "Kategori", "Nilai", f"Struktur Pendidikan - {selected_desa}", yaxis_title="Proporsi"),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )
    with g2:
        dpr_plot = get_profile_series(row, meta["dpr_cols"], normalize=False, top_n=8)
        st.plotly_chart(
            make_profile_bar(dpr_plot, "Kategori", "Nilai", f"Top Partai DPR - {selected_desa}", yaxis_title="Suara"),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )
        job_plot = get_profile_series(row, meta["job_count_cols"], prefix="job_", normalize=False, top_n=8)
        st.plotly_chart(
            make_profile_bar(job_plot, "Kategori", "Nilai", f"Top Pekerjaan - {selected_desa}", yaxis_title="Jumlah"),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )

    neighbors = []
    for name in NETWORK_OPTIONS:
        sim_series = network_sims[name].loc[selected_desa].drop(index=selected_desa).sort_values(ascending=False).head(5)
        for target, value in sim_series.items():
            neighbors.append({"Network": name, "Desa Pembanding": target, "Similarity": float(value)})
    st.markdown("<div class='section-title'>Tetangga Similarity Terdekat</div>", unsafe_allow_html=True)
    st.dataframe(
        pd.DataFrame(neighbors).style.format({"Similarity": "{:.2f}"}),
        width="stretch",
    )

with tabs[5]:
    st.markdown("<div class='section-title'>Analisis Pola Partai DPR</div>", unsafe_allow_html=True)
    st.markdown(
        """
        <div class="analysis-note">
            Bagian ini membaca kekuatan partai sebagai share suara di tiap desa. Fokusnya bukan jumlah suara mentah,
            melainkan seberapa besar proporsi partai dalam desa, sehingga desa besar dan kecil tetap bisa dibandingkan.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.plotly_chart(
        make_party_share_heatmap(profile_df, meta["dpr_cols"]),
        width="stretch",
        config=PLOTLY_DRAW_CONFIG,
    )
    st.markdown(
        """
        <div class="analysis-note">
            Heatmap membantu melihat pola dominasi partai lintas desa. Warna yang makin kuat berarti share suara partai
            tersebut makin besar di desa terkait.
        </div>
        """,
        unsafe_allow_html=True,
    )

    b1, b2 = st.columns([1, 1], gap="large")
    with b1:
        bipartite_threshold = st.slider("Ambang edge desa-partai", 0.01, 0.30, 0.10, 0.01)
    with b2:
        bipartite_top_n = st.slider("Jumlah partai pada graf bipartite", 5, min(18, len(meta["dpr_cols"])), 10, 1)
    st.plotly_chart(
        make_party_bipartite_network(profile_df, meta["dpr_cols"], min_share=bipartite_threshold, top_n_parties=bipartite_top_n),
        width="stretch",
        config=PLOTLY_DRAW_CONFIG,
    )
    st.markdown(
        """
        <div class="analysis-note">
            Bipartite network memperlihatkan hubungan desa dan partai. Edge muncul ketika share partai melewati ambang,
            sehingga partai yang tersebar luas akan punya koneksi ke banyak desa.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.plotly_chart(
        make_party_correlation_heatmap(profile_df, meta["dpr_cols"], feature_matrices),
        width="stretch",
        config=PLOTLY_DRAW_CONFIG,
    )
    st.markdown(
        """
        <div class="analysis-note">
            Korelasi positif berarti share partai cenderung naik ketika variabel sosial tersebut tinggi. Korelasi negatif
            berarti share partai cenderung turun pada desa dengan karakteristik tersebut.
        </div>
        """,
        unsafe_allow_html=True,
    )

    s1, s2 = st.columns([1, 1], gap="large")
    with s1:
        selected_ppwp_scatter = st.selectbox("Sumbu X PPWP", PPWP_COLS, index=0)
    with s2:
        selected_party_scatter = st.selectbox(
            "Sumbu Y Partai",
            meta["dpr_cols"],
            index=0,
            format_func=shorten_party_name,
        )
    st.plotly_chart(
        make_ppwp_party_scatter(profile_df, meta["dpr_cols"], selected_ppwp_scatter, selected_party_scatter),
        width="stretch",
        config=PLOTLY_DRAW_CONFIG,
    )
    st.markdown(
        """
        <div class="analysis-note">
            Scatter PPWP vs partai dipakai untuk melihat apakah desa dengan dukungan kuat pada pasangan PPWP tertentu
            juga cenderung memberi share tinggi pada partai tertentu.
        </div>
        """,
        unsafe_allow_html=True,
    )

    h1, h2 = st.columns([1, 1], gap="large")
    with h1:
        st.plotly_chart(
            make_hhi_party_chart(profile_df, meta["dpr_cols"]),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )
    with h2:
        st.plotly_chart(
            make_party_penetration_chart(profile_df, meta["dpr_cols"]),
            width="stretch",
            config=PLOTLY_DRAW_CONFIG,
        )
    st.markdown(
        """
        <div class="analysis-note">
            HHI membaca konsentrasi kompetisi partai: nilai tinggi menunjukkan desa lebih didominasi sedikit partai,
            sedangkan nilai rendah menunjukkan suara lebih tersebar. Party penetration menunjukkan berapa banyak desa
            yang berhasil ditembus partai pada ambang share 5%, 10%, dan 20%.
        </div>
        """,
        unsafe_allow_html=True,
    )


with tabs[6]:
    e1, e2 = st.columns([1.35, 1], gap="large")
    with e1:
        st.markdown("<div class='section-title'>Edge List Aktif</div>", unsafe_allow_html=True)
        edge_table = active["edge_table"]
        st.dataframe(
            edge_table.style.format(
                {
                    "weight": "{:.2f}",
                    "sim_ppwp": "{:.2f}",
                    "sim_dpr": "{:.2f}",
                    "sim_pendidikan": "{:.2f}",
                    "sim_pekerjaan": "{:.2f}",
                    "sim_demografi": "{:.2f}",
                }
            ),
            width="stretch",
            height=520,
        )
        st.download_button(
            "Unduh Edge List CSV",
            data=edge_table.to_csv(index=False).encode("utf-8"),
            file_name=f"edge_list_{selected_network.lower().replace('-', '_')}.csv",
            mime="text/csv",
        )
    with e2:
        st.markdown("<div class='section-title'>Node Attribute</div>", unsafe_allow_html=True)
        node_cols = [
            "kode",
            "desa",
            "pemilih_dpt_j",
            "pengguna_total",
            "partisipasi_pemilih",
            "ppwp_dominan",
            "partai_dominan",
            "pendidikan_dominan",
            "pekerjaan_dominan",
        ]
        node_table = profile_df[node_cols].rename(
            columns={
                "kode": "Kode",
                "desa": "Desa",
                "pemilih_dpt_j": "DPT",
                "pengguna_total": "Pengguna",
                "partisipasi_pemilih": "Partisipasi",
                "ppwp_dominan": "Pemenang PPWP",
                "partai_dominan": "Partai Dominan",
                "pendidikan_dominan": "Pendidikan Dominan",
                "pekerjaan_dominan": "Pekerjaan Dominan",
            }
        )
        st.dataframe(
            node_table.style.format({"Partisipasi": "{:.2%}", "DPT": "{:.0f}", "Pengguna": "{:.0f}"}),
            width="stretch",
            height=520,
        )
