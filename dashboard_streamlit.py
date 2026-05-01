from __future__ import annotations

import hashlib
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import id as iid_pipeline


BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / ".streamlit_runtime"
UPLOAD_DIR = RUNTIME_DIR / "uploads"
OUTPUT_CACHE_DIR = RUNTIME_DIR / "outputs"
DEFAULT_INPUT_CANDIDATES = (
    BASE_DIR / "data_asli.csv",
    BASE_DIR / "data_asli.xlsx",
    BASE_DIR / "data_asli.xls",
)

for directory in (RUNTIME_DIR, UPLOAD_DIR, OUTPUT_CACHE_DIR):
    directory.mkdir(parents=True, exist_ok=True)


TABLE_SPECS: dict[str, dict[str, Any]] = {
    "data_keluarga": {
        "filename": "data_keluarga.csv",
        "label": "Data keluarga",
        "description": "Hasil akhir tingkat anggota dan rumah tangga. Skor indeks biasanya terisi pada baris kepala keluarga.",
        "required": True,
    },
    "indeks_desa": {
        "filename": "indeks_desa.csv",
        "label": "Indeks desa",
        "description": "Ringkasan skor indikator, dimensi, IID desa, IKD desa, dan ketimpangan per desa/kelurahan.",
        "required": True,
    },
    "penjelasan_variabel": {
        "filename": "penjelasan_variabel.csv",
        "label": "Penjelasan variabel",
        "description": "Kamus variabel, sumber nilai, aturan skoring, dan keterangan tiap indikator atau dimensi.",
        "required": True,
    },
    "rumah_tangga_dikeluarkan": {
        "filename": "rumah_tangga_dikeluarkan.csv",
        "label": "RT dikeluarkan",
        "description": "Rumah tangga yang tidak masuk perhitungan indeks beserta alasan pengeluarannya.",
        "required": False,
    },
    "sebaran_iid_rt_desa": {
        "filename": "sebaran_iid_rt_desa.csv",
        "label": "Sebaran IID-RT per desa",
        "description": "Distribusi kategori IID-RT di setiap desa/kelurahan.",
        "required": False,
    },
    "sebaran_warga_iid_rt": {
        "filename": "sebaran_warga_iid_rt.csv",
        "label": "Sebaran warga menurut IID-RT",
        "description": "Distribusi jumlah dan persentase warga menurut kategori IID-RT.",
        "required": False,
    },
    "ringkasan_pengolahan": {
        "filename": "ringkasan_pengolahan.csv",
        "label": "Ringkasan pengolahan",
        "description": "Ringkasan proses olah data dari pipeline, termasuk jumlah RT valid dan aturan usia sekolah.",
        "required": False,
    },
    "ringkasan_ketimpangan": {
        "filename": "ringkasan_ketimpangan.csv",
        "label": "Ringkasan ketimpangan",
        "description": "Ringkasan Gini keseluruhan dan per desa, termasuk kategori dan rumah tangga kontributor utama.",
        "required": False,
    },
    "kontributor_ketimpangan": {
        "filename": "kontributor_ketimpangan.csv",
        "label": "Kontributor ketimpangan",
        "description": "Daftar rumah tangga yang berkontribusi pada ketimpangan, baik untuk keseluruhan wilayah maupun per desa.",
        "required": False,
    },
    "sebaran_gini_desa": {
        "filename": "sebaran_gini_desa.csv",
        "label": "Sebaran Gini desa",
        "description": "Klasifikasi relatif berbasis tertil untuk Gini IID rumah tangga antar desa dalam sampel penelitian.",
        "required": False,
    },
    "batas_kategori_iid_rt": {
        "filename": "batas_kategori_iid_rt.csv",
        "label": "Batas kategori IID-RT",
        "description": "Batas bawah dan batas atas kategori IID-RT pada skema rekomendasi.",
        "required": False,
    },
    "perbandingan_skema": {
        "filename": "perbandingan_skema.csv",
        "label": "Perbandingan skema",
        "description": "Perbandingan statistik dan distribusi kategori antara skema baseline dan rekomendasi.",
        "required": False,
    },
    "skema_rekomendasi": {
        "filename": "skema_rekomendasi.csv",
        "label": "Skema rekomendasi",
        "description": "Spesifikasi komponen, bobot, dan aturan pada skema rekomendasi.",
        "required": False,
    },
    "analisis_determinasi_dimensi": {
        "filename": "analisis_determinasi_dimensi.csv",
        "label": "Determinasi dimensi",
        "description": "Koefisien determinasi tiap dimensi terhadap IID desa pada skala log natural.",
        "required": False,
    },
    "analisis_determinasi_variabel": {
        "filename": "analisis_determinasi_variabel.csv",
        "label": "Determinasi variabel",
        "description": "Koefisien determinasi tiap indikator terhadap dimensinya dan IID desa.",
        "required": False,
    },
    "analisis_sensitivitas_oat": {
        "filename": "analisis_sensitivitas_oat.csv",
        "label": "Sensitivitas OAT",
        "description": "Simulasi One-At-a-Time dengan kenaikan satu poin dimensi pada skala 0-100 untuk melihat persentase kenaikan IID dan persentase penurunan IKD.",
        "required": False,
    },
    "analisis_shapley_variabel": {
        "filename": "analisis_shapley_variabel.csv",
        "label": "Kontribusi Shapley variabel",
        "description": "Kontribusi Shapley R2 tiap indikator dalam menjelaskan IID desa.",
        "required": False,
    },
}

CORE_TABLE_KEYS = ("data_keluarga", "indeks_desa", "penjelasan_variabel")

CATEGORY_ORDER = [*iid_pipeline.IID_RT_CATEGORY_ORDER, iid_pipeline.UNSCORED_IID_CATEGORY_LABEL]
CATEGORY_COLORS = {
    "sangat rendah": "#9f1239",
    "rendah": "#ea580c",
    "sedang": "#eab308",
    "tinggi": "#14b8a6",
    "sangat tinggi": "#2563eb",
    iid_pipeline.UNSCORED_IID_CATEGORY_LABEL: "#64748b",
}
GINI_COLORS = {
    "Rendah": "#16a34a",
    "Sedang": "#eab308",
    "Tinggi": "#dc2626",
}
INEQUALITY_DIRECTION_COLORS = {
    "di bawah rata-rata": "#b91c1c",
    "di atas rata-rata": "#0f766e",
    "sama dengan rata-rata": "#64748b",
}
IKD_QUARTILE_ORDER = ["Q1", "Q2", "Q3", "Q4"]
IKD_QUARTILE_LABELS = {
    "Q1": "Kuartil 1 - IKD terendah",
    "Q2": "Kuartil 2",
    "Q3": "Kuartil 3",
    "Q4": "Kuartil 4 - IKD tertinggi",
}
IKD_QUARTILE_COLORS = {
    "Q1": "#0f766e",
    "Q2": "#14b8a6",
    "Q3": "#f59e0b",
    "Q4": "#b91c1c",
}
DIMENSION_LABELS = {
    "dimensi_A": "Akses perangkat",
    "dimensi_B": "Konektivitas internet",
    "dimensi_C": "Kapasitas manusia",
    "dimensi_D": "Penggunaan digital",
    "dimensi_E": "Lingkungan sosial",
}


st.set_page_config(
    page_title="Dashboard Inklusi Digital",
    page_icon="assets/logo-banner2.png" if (BASE_DIR / "assets" / "logo-banner2.png").exists() else None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg-start: #f6f7f1;
            --bg-end: #eef6f8;
            --card: rgba(255, 255, 255, 0.88);
            --card-strong: rgba(255, 255, 255, 0.96);
            --border: rgba(15, 23, 42, 0.08);
            --shadow: 0 24px 50px rgba(15, 23, 42, 0.08);
            --text-main: #163249;
            --text-soft: #5b7083;
            --accent: #0f766e;
            --accent-soft: #d8f3eb;
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(15, 118, 110, 0.10), transparent 32%),
                radial-gradient(circle at top right, rgba(37, 99, 235, 0.10), transparent 30%),
                linear-gradient(180deg, var(--bg-start) 0%, var(--bg-end) 100%);
        }
        .main .block-container {
            max-width: 1400px;
            padding-top: 1.2rem;
            padding-bottom: 2rem;
        }
        section[data-testid="stSidebar"] {
            background:
                linear-gradient(180deg, rgba(22, 50, 73, 0.98) 0%, rgba(11, 31, 49, 0.98) 100%);
            border-right: 1px solid rgba(255, 255, 255, 0.08);
        }
        section[data-testid="stSidebar"] * {
            color: #f8fafc !important;
        }
        section[data-testid="stSidebar"] [data-baseweb="select"] > div,
        section[data-testid="stSidebar"] [data-baseweb="input"] > div,
        section[data-testid="stSidebar"] .stSlider {
            background: rgba(255, 255, 255, 0.08) !important;
            border-radius: 12px !important;
            border: 1px solid rgba(255, 255, 255, 0.10) !important;
        }
        .hero-shell {
            padding: 1.6rem 1.7rem;
            border-radius: 26px;
            background:
                linear-gradient(135deg, rgba(15, 118, 110, 0.94) 0%, rgba(21, 128, 61, 0.86) 38%, rgba(22, 50, 73, 0.92) 100%);
            color: white;
            box-shadow: 0 28px 55px rgba(15, 23, 42, 0.16);
            border: 1px solid rgba(255, 255, 255, 0.18);
            overflow: hidden;
            position: relative;
        }
        .hero-shell::after {
            content: "";
            position: absolute;
            inset: auto -10% -35% auto;
            width: 280px;
            height: 280px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.10);
            filter: blur(10px);
        }
        .hero-kicker {
            letter-spacing: 0.16em;
            font-size: 0.75rem;
            text-transform: uppercase;
            opacity: 0.78;
            margin-bottom: 0.35rem;
            font-weight: 700;
        }
        .hero-title {
            font-size: 2.15rem;
            line-height: 1.05;
            font-weight: 800;
            margin: 0;
        }
        .hero-subtitle {
            margin-top: 0.65rem;
            max-width: 920px;
            font-size: 1.02rem;
            line-height: 1.55;
            color: rgba(248, 250, 252, 0.92);
        }
        .hero-badges {
            display: flex;
            gap: 0.55rem;
            flex-wrap: wrap;
            margin-top: 1rem;
        }
        .hero-badge {
            background: rgba(255, 255, 255, 0.14);
            border: 1px solid rgba(255, 255, 255, 0.18);
            padding: 0.42rem 0.75rem;
            border-radius: 999px;
            font-size: 0.85rem;
        }
        div[data-testid="stMetric"] {
            background: var(--card);
            border: 1px solid var(--border);
            padding: 0.9rem 1rem;
            border-radius: 18px;
            box-shadow: var(--shadow);
            backdrop-filter: blur(8px);
        }
        div[data-testid="stMetricLabel"] {
            color: var(--text-soft);
            font-weight: 600;
        }
        div[data-testid="stMetricValue"] {
            color: var(--text-main);
            font-weight: 800;
        }
        .section-card {
            background: var(--card-strong);
            border: 1px solid var(--border);
            border-radius: 22px;
            box-shadow: var(--shadow);
            padding: 0.35rem 0.8rem 0.9rem 0.8rem;
        }
        .section-note {
            color: var(--text-soft);
            font-size: 0.95rem;
            line-height: 1.55;
            margin-top: 0.1rem;
            margin-bottom: 0.9rem;
        }
        .pill-note {
            display: inline-block;
            padding: 0.28rem 0.65rem;
            background: var(--accent-soft);
            color: var(--accent);
            border-radius: 999px;
            font-weight: 700;
            font-size: 0.82rem;
            margin-bottom: 0.8rem;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.45rem;
        }
        .stTabs [data-baseweb="tab"] {
            background: rgba(255, 255, 255, 0.82);
            border-radius: 999px;
            padding: 0.55rem 0.95rem;
            border: 1px solid rgba(15, 23, 42, 0.08);
        }
        .stTabs [aria-selected="true"] {
            background: #163249 !important;
            color: white !important;
        }
        .small-muted {
            color: var(--text-soft);
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def detect_default_output_dir() -> Path:
    candidates = (
        BASE_DIR / "hasil_indeks_digital_uji2",
        BASE_DIR / "hasil_indeks_digital_skema_rekomendasi_codex",
        BASE_DIR / "hasil_indeks_digital",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return BASE_DIR / "hasil_indeks_digital_uji2"


def detect_default_input_path() -> Path | None:
    for candidate in DEFAULT_INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def format_number(value: Any, digits: int = 3) -> str:
    if value is None or pd.isna(value):
        return "-"
    if isinstance(value, int):
        return f"{value:,}".replace(",", ".")
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def format_percent(value: Any, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


def format_currency(value: Any, digits: int = 0) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"Rp {float(value):,.{digits}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def build_file_signature(path: Path) -> str:
    stats = path.stat()
    raw = f"{path.resolve()}|{stats.st_size}|{stats.st_mtime_ns}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def build_folder_signature(output_dir: Path) -> str:
    parts: list[str] = []
    for spec in TABLE_SPECS.values():
        file_path = output_dir / spec["filename"]
        if file_path.exists():
            stats = file_path.stat()
            parts.append(f"{file_path.name}|{stats.st_size}|{stats.st_mtime_ns}")
    workbook_path = output_dir / "hasil_olahdata.xlsx"
    if workbook_path.exists():
        stats = workbook_path.stat()
        parts.append(f"{workbook_path.name}|{stats.st_size}|{stats.st_mtime_ns}")
    if not parts:
        return "empty"
    raw = "|".join(sorted(parts))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def save_uploaded_file(uploaded_file: Any) -> Path:
    content = uploaded_file.getvalue()
    digest = hashlib.md5(content).hexdigest()[:12]
    safe_name = uploaded_file.name.replace(" ", "_")
    target_path = UPLOAD_DIR / f"{digest}_{safe_name}"
    if not target_path.exists():
        target_path.write_bytes(content)
    return target_path


def derive_processing_summary(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    keluarga_df = tables.get("data_keluarga", pd.DataFrame())
    desa_df = tables.get("indeks_desa", pd.DataFrame())
    excluded_df = tables.get("rumah_tangga_dikeluarkan", pd.DataFrame())
    rows: list[dict[str, Any]] = []

    if not keluarga_df.empty:
        rows.append({"metrik": "jumlah_baris_data_keluarga", "nilai": int(len(keluarga_df))})
        if "family_id" in keluarga_df.columns:
            rows.append(
                {
                    "metrik": "jumlah_rumah_tangga_tercatat",
                    "nilai": int(keluarga_df["family_id"].astype("string").nunique(dropna=True)),
                }
            )
        if "iid_rumah_tangga" in keluarga_df.columns and "family_id" in keluarga_df.columns:
            valid_households = keluarga_df.loc[keluarga_df["iid_rumah_tangga"].notna(), "family_id"]
            rows.append({"metrik": "jumlah_rumah_tangga_valid", "nilai": int(valid_households.nunique(dropna=True))})

    if not excluded_df.empty and "family_id" in excluded_df.columns:
        rows.append(
            {
                "metrik": "jumlah_rumah_tangga_dikeluarkan",
                "nilai": int(excluded_df["family_id"].astype("string").nunique(dropna=True)),
            }
        )

    if not desa_df.empty:
        rows.append({"metrik": "jumlah_desa", "nilai": int(len(desa_df))})
        if "jumlah_kk" in desa_df.columns:
            rows.append({"metrik": "jumlah_kk_agregat", "nilai": int(pd.to_numeric(desa_df["jumlah_kk"], errors="coerce").sum())})
        if "iid_desa" in desa_df.columns:
            rows.append({"metrik": "rata_rata_iid_desa", "nilai": float(pd.to_numeric(desa_df["iid_desa"], errors="coerce").mean())})

    if not rows:
        return pd.DataFrame(columns=["metrik", "nilai"])
    return pd.DataFrame(rows)


def normalize_desa_gini_table(desa_df: pd.DataFrame) -> pd.DataFrame:
    if desa_df.empty or "gini_iid_rumah_tangga" not in desa_df.columns:
        return desa_df.copy()
    normalized_df, _ = iid_pipeline.apply_relative_gini_classification(desa_df.copy())
    return normalized_df


def normalize_gini_distribution_table(
    distribution_df: pd.DataFrame,
    desa_df: pd.DataFrame,
) -> pd.DataFrame:
    if not distribution_df.empty and {"interpretasi_gini", "rentang_gini", "jumlah_desa"}.issubset(distribution_df.columns):
        normalized_df = distribution_df.copy()
        for column in ("jumlah_desa", "persentase_desa", "total_desa", "batas_bawah", "batas_atas"):
            if column in normalized_df.columns:
                normalized_df[column] = pd.to_numeric(normalized_df[column], errors="coerce")
        return normalized_df
    _, derived_df = iid_pipeline.apply_relative_gini_classification(desa_df.copy())
    return derived_df


def normalize_variable_explanation_table(variable_df: pd.DataFrame) -> pd.DataFrame:
    if variable_df.empty or "nama_variabel" not in variable_df.columns or "aturan_skoring" not in variable_df.columns:
        return variable_df.copy()
    normalized_df = variable_df.copy()
    mask = normalized_df["nama_variabel"].astype("string").eq("interpretasi_gini")
    normalized_df.loc[mask, "aturan_skoring"] = iid_pipeline.GINI_INTERPRETATION_RULE_TEXT
    return normalized_df


def ensure_advanced_analysis_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    analysis_keys = (
        "analisis_determinasi_dimensi",
        "analisis_determinasi_variabel",
        "analisis_sensitivitas_oat",
        "analisis_shapley_variabel",
    )
    desa_df = tables.get("indeks_desa", pd.DataFrame())
    variable_df = tables.get("penjelasan_variabel", pd.DataFrame())
    derived_tables = iid_pipeline.build_advanced_analysis_tables(desa_df, variable_df)
    enriched_tables = tables.copy()
    for key in analysis_keys:
        enriched_tables[key] = derived_tables.get(key, pd.DataFrame())
    return enriched_tables


@st.cache_data(show_spinner=False)
def load_output_bundle_cached(output_dir_str: str, folder_signature: str) -> dict[str, Any]:
    del folder_signature
    output_dir = Path(output_dir_str)
    if not output_dir.exists():
        raise FileNotFoundError(f"Folder output tidak ditemukan: {output_dir}")

    tables: dict[str, pd.DataFrame] = {}
    missing_required: list[str] = []

    for key, spec in TABLE_SPECS.items():
        csv_path = output_dir / spec["filename"]
        if csv_path.exists():
            tables[key] = pd.read_csv(csv_path, low_memory=False)
        elif spec["required"]:
            missing_required.append(spec["filename"])

    if missing_required:
        joined = ", ".join(missing_required)
        raise FileNotFoundError(f"File inti tidak lengkap di folder output: {joined}")

    if "ringkasan_pengolahan" not in tables:
        tables["ringkasan_pengolahan"] = derive_processing_summary(tables)
    if "indeks_desa" in tables:
        tables["indeks_desa"] = normalize_desa_gini_table(tables["indeks_desa"])
        tables["sebaran_gini_desa"] = normalize_gini_distribution_table(
            tables.get("sebaran_gini_desa", pd.DataFrame()),
            tables["indeks_desa"],
        )
    if "penjelasan_variabel" in tables:
        tables["penjelasan_variabel"] = normalize_variable_explanation_table(tables["penjelasan_variabel"])
    tables = ensure_advanced_analysis_tables(tables)

    workbook_path = output_dir / "hasil_olahdata.xlsx"
    meta = {
        "output_dir": str(output_dir.resolve()),
        "workbook_path": str(workbook_path.resolve()) if workbook_path.exists() else None,
        "available_tables": [key for key in TABLE_SPECS if key in tables],
    }
    return {"tables": tables, "meta": meta}


def load_output_bundle(output_dir: Path) -> dict[str, Any]:
    signature = build_folder_signature(output_dir)
    bundle = load_output_bundle_cached(str(output_dir), signature)
    bundle["meta"]["source_mode"] = "folder_hasil"
    bundle["meta"]["source_label"] = "Folder hasil siap pakai"
    return bundle


@st.cache_data(show_spinner=False)
def process_input_bundle_cached(
    input_path_str: str,
    input_signature: str,
    scheme: str,
    school_age_min: int,
    school_age_max: int,
    missing_threshold: float,
) -> dict[str, Any]:
    del input_signature
    input_path = Path(input_path_str)
    if not input_path.exists():
        raise FileNotFoundError(f"File input tidak ditemukan: {input_path}")

    output_hash = hashlib.md5(
        f"{input_path.resolve()}|{build_file_signature(input_path)}|{scheme}|{school_age_min}|{school_age_max}|{missing_threshold}".encode(
            "utf-8"
        )
    ).hexdigest()[:12]
    output_dir = OUTPUT_CACHE_DIR / f"{input_path.stem}_{scheme}_{output_hash}"

    expected_paths = [output_dir / TABLE_SPECS[key]["filename"] for key in CORE_TABLE_KEYS]
    if not all(path.exists() for path in expected_paths):
        if scheme == "rekomendasi":
            iid_pipeline.run_pipeline_recommended(
                input_path=input_path,
                output_dir=output_dir,
                school_age_min=school_age_min,
                school_age_max=school_age_max,
                missing_threshold=missing_threshold,
            )
        else:
            iid_pipeline.run_pipeline(
                input_path=input_path,
                output_dir=output_dir,
                school_age_min=school_age_min,
                school_age_max=school_age_max,
                missing_threshold=missing_threshold,
            )

    bundle = load_output_bundle_cached(str(output_dir), build_folder_signature(output_dir))
    bundle["meta"]["source_mode"] = "olah_ulang"
    bundle["meta"]["source_label"] = "Olah dari file mentah"
    bundle["meta"]["scheme"] = scheme
    bundle["meta"]["input_path"] = str(input_path.resolve())
    bundle["meta"]["school_age_min"] = school_age_min
    bundle["meta"]["school_age_max"] = school_age_max
    bundle["meta"]["missing_threshold"] = missing_threshold
    return bundle


def process_input_bundle(
    input_path: Path,
    scheme: str,
    school_age_min: int,
    school_age_max: int,
    missing_threshold: float,
) -> dict[str, Any]:
    signature = build_file_signature(input_path)
    return process_input_bundle_cached(
        str(input_path),
        signature,
        scheme,
        school_age_min,
        school_age_max,
        missing_threshold,
    )


def ensure_request_state() -> None:
    if "dashboard_request" not in st.session_state:
        st.session_state.dashboard_request = {
            "mode": "folder_hasil",
            "output_dir": str(detect_default_output_dir()),
        }


@st.cache_data(show_spinner=False)
def load_household_detail_cached(
    input_path_str: str,
    input_signature: str,
    school_age_min: int,
    school_age_max: int,
    missing_threshold: float,
) -> pd.DataFrame:
    del input_signature
    input_path = Path(input_path_str)
    person_df = iid_pipeline.load_source_data(input_path)
    valid_df, _, _ = iid_pipeline.build_household_index(
        person_df,
        school_age_min=school_age_min,
        school_age_max=school_age_max,
        missing_threshold=missing_threshold,
    )
    keep_columns = [
        "family_id",
        "deskel",
        "dusun",
        "rw",
        "lat",
        "lng",
        "jml_keluarga",
        "jumlah_anggota_rumah_tangga",
        "hp_jumlah_num",
        "hp_jumlah_terstandar",
        "rp_komunikasi_tertinggi",
        "iid_rt",
        "ikd_rt",
    ]
    existing_columns = [column for column in keep_columns if column in valid_df.columns]
    detail_df = valid_df[existing_columns].copy()
    for column in (
        "jml_keluarga",
        "jumlah_anggota_rumah_tangga",
        "hp_jumlah_num",
        "hp_jumlah_terstandar",
        "rp_komunikasi_tertinggi",
        "iid_rt",
        "ikd_rt",
        "lat",
        "lng",
    ):
        if column in detail_df.columns:
            detail_df[column] = pd.to_numeric(detail_df[column], errors="coerce")
    return detail_df


def resolve_household_detail_df(meta: dict[str, Any], tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    input_path_value = meta.get("input_path")
    input_path = Path(input_path_value) if input_path_value else detect_default_input_path()
    if input_path is None or not input_path.exists():
        return pd.DataFrame()

    school_age_min = int(meta.get("school_age_min", iid_pipeline.SCHOOL_AGE_MIN))
    school_age_max = int(meta.get("school_age_max", iid_pipeline.SCHOOL_AGE_MAX))
    missing_threshold = float(meta.get("missing_threshold", iid_pipeline.MISSING_THRESHOLD))

    detail_df = load_household_detail_cached(
        str(input_path),
        build_file_signature(input_path),
        school_age_min,
        school_age_max,
        missing_threshold,
    )
    if detail_df.empty:
        return detail_df

    household_df = get_household_rows(tables.get("data_keluarga", pd.DataFrame()))
    if not household_df.empty and {"family_id", "kategori_iid_rt"}.issubset(household_df.columns):
        category_df = household_df[["family_id", "kategori_iid_rt"]].drop_duplicates(subset=["family_id"])
        detail_df = detail_df.merge(category_df, on="family_id", how="left")
    else:
        detail_df["kategori_iid_rt"] = detail_df["iid_rt"].apply(iid_pipeline.classify_iid_rt)

    return detail_df


def get_household_rows(keluarga_df: pd.DataFrame) -> pd.DataFrame:
    if keluarga_df.empty:
        return keluarga_df.copy()
    household_df = keluarga_df.copy()
    if "iid_rumah_tangga" in household_df.columns:
        household_df["iid_rumah_tangga"] = pd.to_numeric(household_df["iid_rumah_tangga"], errors="coerce")
        household_df = household_df.loc[household_df["iid_rumah_tangga"].notna()].copy()
    if "family_id" in household_df.columns:
        household_df = household_df.drop_duplicates(subset=["family_id"], keep="first")
    return household_df


def resolve_inequality_tables(tables: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_df = tables.get("ringkasan_ketimpangan", pd.DataFrame()).copy()
    contributor_df = tables.get("kontributor_ketimpangan", pd.DataFrame()).copy()
    valid_gini_labels = set(iid_pipeline.GINI_CATEGORY_ORDER)
    summary_labels = set(summary_df.get("interpretasi_gini", pd.Series(dtype="string")).dropna().astype("string").tolist())
    contributor_labels = set(
        contributor_df.get("interpretasi_gini_cakupan", pd.Series(dtype="string")).dropna().astype("string").tolist()
    )
    uses_relative_labels = (
        (not summary_labels or summary_labels.issubset(valid_gini_labels))
        and (not contributor_labels or contributor_labels.issubset(valid_gini_labels))
    )

    if summary_df.empty or contributor_df.empty or not uses_relative_labels:
        household_df = get_household_rows(tables.get("data_keluarga", pd.DataFrame()))
        if not household_df.empty:
            summary_df, contributor_df = iid_pipeline.build_gini_assessment_tables(household_df)

    for column in (
        "jumlah_kk",
        "rata_rata_iid_rumah_tangga",
        "gini_iid_rumah_tangga",
        "jumlah_kontributor_non_nol",
        "iid_kontributor_utama",
        "porsi_kontributor_utama",
    ):
        if column in summary_df.columns:
            summary_df[column] = pd.to_numeric(summary_df[column], errors="coerce")

    for column in (
        "jumlah_kk_cakupan",
        "gini_iid_rumah_tangga_cakupan",
        "rata_rata_iid_cakupan",
        "iid_rumah_tangga",
        "deviasi_iid_cakupan",
        "jumlah_selisih_pasangan",
        "kontribusi_gini",
        "porsi_kontribusi_gini",
        "peringkat_kontribusi",
    ):
        if column in contributor_df.columns:
            contributor_df[column] = pd.to_numeric(contributor_df[column], errors="coerce")

    return summary_df, contributor_df


def build_top_inequality_contributors_figure(
    contributor_df: pd.DataFrame,
    title: str,
    top_n: int = 15,
) -> go.Figure:
    plot_df = contributor_df.copy()
    plot_df = plot_df.sort_values(
        ["porsi_kontribusi_gini", "kontribusi_gini", "iid_rumah_tangga"],
        ascending=[False, False, False],
        kind="mergesort",
    ).head(top_n)
    if plot_df["deskel"].astype("string").nunique(dropna=True) > 1:
        plot_df["label_rt"] = plot_df["family_id"].astype("string") + " | " + plot_df["deskel"].astype("string")
    else:
        plot_df["label_rt"] = plot_df["family_id"].astype("string")
    plot_df = plot_df.sort_values("porsi_kontribusi_gini", ascending=True, kind="mergesort")

    fig = px.bar(
        plot_df,
        x="porsi_kontribusi_gini",
        y="label_rt",
        orientation="h",
        color="arah_deviasi",
        color_discrete_map=INEQUALITY_DIRECTION_COLORS,
        text=plot_df["porsi_kontribusi_gini"].map(lambda value: f"{float(value) * 100:.2f}%"),
        hover_data={
            "family_id": True,
            "deskel": True,
            "iid_rumah_tangga": ":.3f",
            "rata_rata_iid_cakupan": ":.3f",
            "kontribusi_gini": ":.4f",
            "porsi_kontribusi_gini": ":.2%",
            "label_rt": False,
        },
    )
    fig.update_layout(
        title=title,
        xaxis_title="Porsi kontribusi terhadap Gini",
        yaxis_title="Rumah tangga",
        legend_title_text="Posisi skor",
        margin=dict(l=20, r=20, t=55, b=20),
    )
    fig.update_xaxes(tickformat=".0%")
    return fig


def get_coordinate_columns(df: pd.DataFrame) -> tuple[str | None, str | None]:
    lat_col = "lat" if "lat" in df.columns else None
    lon_col = None
    for candidate in ("long", "lng", "lon", "longitude"):
        if candidate in df.columns:
            lon_col = candidate
            break
    return lat_col, lon_col


def render_hero(meta: dict[str, Any]) -> None:
    badges = [
        f"Sumber: {meta.get('source_label', '-')}",
        f"Folder output: {Path(meta.get('output_dir', '-')).name}",
    ]
    if meta.get("scheme"):
        badges.append(f"Skema: {meta['scheme']}")
    if meta.get("input_path"):
        badges.append(f"Input: {Path(meta['input_path']).name}")

    badge_html = "".join(f"<span class='hero-badge'>{item}</span>" for item in badges)
    st.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-kicker">Dashboard Streamlit</div>
            <h1 class="hero-title">Visualisasi Inklusi Digital Rumah Tangga dan Desa</h1>
            <div class="hero-subtitle">
                Dashboard ini menampilkan ringkasan hasil olah data dari pipeline <code>id.py</code>,
                lengkap dengan grafik, profil skor, penjelasan variabel, dan deskripsi tabel
                agar data lebih mudah dibaca langsung dari browser.
            </div>
            <div class="hero-badges">{badge_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_top_summary_metrics(tables: dict[str, pd.DataFrame]) -> None:
    keluarga_df = tables.get("data_keluarga", pd.DataFrame())
    desa_df = tables.get("indeks_desa", pd.DataFrame())
    excluded_df = tables.get("rumah_tangga_dikeluarkan", pd.DataFrame())
    warga_df = tables.get("sebaran_warga_iid_rt", pd.DataFrame())
    inequality_summary_df, _ = resolve_inequality_tables(tables)

    household_df = get_household_rows(keluarga_df)
    total_warga = int(len(keluarga_df))
    if not warga_df.empty and "total_warga" in warga_df.columns:
        total_warga = int(pd.to_numeric(warga_df["total_warga"], errors="coerce").max())

    total_valid = int(len(household_df))
    total_excluded = 0
    if not excluded_df.empty and "family_id" in excluded_df.columns:
        total_excluded = int(excluded_df["family_id"].astype("string").nunique(dropna=True))
    total_desa = int(len(desa_df))
    avg_iid = pd.to_numeric(desa_df.get("iid_desa"), errors="coerce").mean() if not desa_df.empty else None
    overall_row = inequality_summary_df.loc[
        inequality_summary_df["cakupan_analisis"].astype("string").eq("keseluruhan")
    ].head(1)
    overall_gini = overall_row["gini_iid_rumah_tangga"].iloc[0] if not overall_row.empty else None
    overall_category = overall_row["interpretasi_gini"].iloc[0] if not overall_row.empty else "-"

    metric_cols = st.columns(5)
    metric_cols[0].metric("RT valid", format_number(total_valid, 0))
    metric_cols[1].metric("RT dikeluarkan", format_number(total_excluded, 0))
    metric_cols[2].metric("Jumlah desa", format_number(total_desa, 0))
    metric_cols[3].metric("Rata-rata IID desa", format_number(avg_iid))
    metric_cols[4].metric("Gini keseluruhan", format_number(overall_gini))

    extra_cols = st.columns(3)
    extra_cols[0].metric("Jumlah warga", format_number(total_warga, 0))
    if not household_df.empty and "kategori_iid_rt" in household_df.columns:
        top_category = household_df["kategori_iid_rt"].astype("string").value_counts(dropna=True)
        extra_cols[1].metric("Kategori RT dominan", top_category.index[0] if not top_category.empty else "-")
    else:
        extra_cols[1].metric("Kategori RT dominan", "-")
    extra_cols[2].metric("Kategori relatif Gini", str(overall_category) if pd.notna(overall_category) else "-")


def build_household_resource_summary(detail_df: pd.DataFrame) -> dict[str, float]:
    hp_series = pd.to_numeric(detail_df.get("hp_jumlah_num"), errors="coerce")
    member_series = pd.to_numeric(detail_df.get("jml_keluarga"), errors="coerce")
    comm_series = pd.to_numeric(detail_df.get("rp_komunikasi_tertinggi"), errors="coerce")
    return {
        "avg_hp": float(hp_series.mean()) if hp_series.notna().any() else float("nan"),
        "avg_members": float(member_series.mean()) if member_series.notna().any() else float("nan"),
        "avg_comm": float(comm_series.mean()) if comm_series.notna().any() else float("nan"),
        "median_comm": float(comm_series.median()) if comm_series.notna().any() else float("nan"),
    }


def build_category_count_figure(household_df: pd.DataFrame) -> go.Figure:
    category_counts = (
        household_df["kategori_iid_rt"]
        .fillna(iid_pipeline.UNSCORED_IID_CATEGORY_LABEL)
        .astype("string")
        .value_counts()
        .reindex(CATEGORY_ORDER, fill_value=0)
        .reset_index()
    )
    category_counts.columns = ["kategori_iid_rt", "jumlah_rt"]
    fig = px.bar(
        category_counts,
        x="kategori_iid_rt",
        y="jumlah_rt",
        color="kategori_iid_rt",
        color_discrete_map=CATEGORY_COLORS,
        text_auto=True,
    )
    fig.update_layout(
        title="Distribusi rumah tangga valid menurut kategori IID-RT",
        xaxis_title="Kategori IID-RT",
        yaxis_title="Jumlah rumah tangga",
        showlegend=False,
        margin=dict(l=20, r=20, t=55, b=20),
    )
    return fig


def build_household_histogram_figure(household_df: pd.DataFrame) -> go.Figure:
    fig = px.histogram(
        household_df,
        x="iid_rumah_tangga",
        nbins=30,
        color_discrete_sequence=["#0f766e"],
    )
    fig.update_layout(
        title="Sebaran skor IID rumah tangga",
        xaxis_title="Skor IID rumah tangga",
        yaxis_title="Jumlah rumah tangga",
        margin=dict(l=20, r=20, t=55, b=20),
    )
    return fig


def build_household_average_figure(detail_df: pd.DataFrame) -> go.Figure:
    summary = build_household_resource_summary(detail_df)
    plot_df = pd.DataFrame(
        [
            {"metrik": "Rata-rata jumlah HP", "nilai": summary["avg_hp"], "warna": "#0f766e"},
            {"metrik": "Rata-rata anggota keluarga", "nilai": summary["avg_members"], "warna": "#163249"},
        ]
    )
    fig = px.bar(
        plot_df,
        x="metrik",
        y="nilai",
        color="metrik",
        color_discrete_sequence=plot_df["warna"].tolist(),
        text_auto=".2f",
    )
    fig.update_layout(
        title="Perbandingan rata-rata HP dan anggota keluarga",
        xaxis_title="Metrik",
        yaxis_title="Rata-rata",
        showlegend=False,
        margin=dict(l=20, r=20, t=55, b=20),
    )
    return fig


def build_comm_cost_distribution_figure(detail_df: pd.DataFrame) -> go.Figure:
    plot_df = detail_df.copy()
    plot_df["rp_komunikasi_tertinggi"] = pd.to_numeric(plot_df["rp_komunikasi_tertinggi"], errors="coerce")
    plot_df = plot_df.dropna(subset=["rp_komunikasi_tertinggi"])
    fig = px.histogram(
        plot_df,
        x="rp_komunikasi_tertinggi",
        nbins=40,
        color_discrete_sequence=["#ea580c"],
    )
    fig.update_layout(
        title="Sebaran biaya komunikasi rumah tangga",
        xaxis_title="Biaya komunikasi tertinggi per rumah tangga (Rp)",
        yaxis_title="Jumlah rumah tangga",
        margin=dict(l=20, r=20, t=55, b=20),
    )
    fig.update_xaxes(tickformat=",.0f")
    return fig


def build_hp_member_scatter_figure(detail_df: pd.DataFrame) -> go.Figure:
    plot_df = detail_df.copy()
    for column in ("jml_keluarga", "hp_jumlah_num", "rp_komunikasi_tertinggi"):
        plot_df[column] = pd.to_numeric(plot_df[column], errors="coerce")
    plot_df = plot_df.dropna(subset=["jml_keluarga", "hp_jumlah_num"])
    if len(plot_df) > 5000:
        plot_df = plot_df.sample(5000, random_state=42)
    fig = px.scatter(
        plot_df,
        x="jml_keluarga",
        y="hp_jumlah_num",
        color="rp_komunikasi_tertinggi",
        color_continuous_scale=["#dbeafe", "#f59e0b", "#b91c1c"],
        hover_name="deskel" if "deskel" in plot_df.columns else None,
        opacity=0.65,
    )
    fig.update_layout(
        title="Hubungan jumlah anggota, jumlah HP, dan biaya komunikasi",
        xaxis_title="Jumlah anggota keluarga",
        yaxis_title="Jumlah HP",
        margin=dict(l=20, r=20, t=55, b=20),
        coloraxis_colorbar_title="Biaya (Rp)",
    )
    return fig


def build_household_resource_by_desa_figure(detail_df: pd.DataFrame, metric: str, top_n: int = 12) -> go.Figure:
    label_map = {
        "hp_jumlah_num": ("Rata-rata jumlah HP per desa", "Rata-rata jumlah HP", "#0f766e"),
        "jml_keluarga": ("Rata-rata anggota keluarga per desa", "Rata-rata anggota keluarga", "#163249"),
        "rp_komunikasi_tertinggi": ("Rata-rata biaya komunikasi per desa", "Rata-rata biaya komunikasi (Rp)", "#ea580c"),
    }
    title, xaxis_title, color = label_map[metric]
    plot_df = detail_df[["deskel", metric]].copy()
    plot_df[metric] = pd.to_numeric(plot_df[metric], errors="coerce")
    plot_df = plot_df.dropna(subset=[metric])
    plot_df = plot_df.groupby("deskel", dropna=False)[metric].mean().reset_index()
    plot_df = plot_df.nlargest(top_n, metric).sort_values(metric)
    fig = px.bar(
        plot_df,
        x=metric,
        y="deskel",
        orientation="h",
        text_auto=".2f" if metric != "rp_komunikasi_tertinggi" else ".0f",
        color_discrete_sequence=[color],
    )
    fig.update_layout(
        title=title,
        xaxis_title=xaxis_title,
        yaxis_title="Desa/kelurahan",
        showlegend=False,
        margin=dict(l=20, r=20, t=55, b=20),
    )
    if metric == "rp_komunikasi_tertinggi":
        fig.update_xaxes(tickformat=",.0f")
    return fig


def build_person_distribution_figure(warga_df: pd.DataFrame) -> go.Figure:
    distribution = warga_df.copy()
    distribution["kategori_iid_rt"] = distribution["kategori_iid_rt"].astype("string")
    distribution["jumlah_warga"] = pd.to_numeric(distribution["jumlah_warga"], errors="coerce")
    distribution = distribution.set_index("kategori_iid_rt").reindex(CATEGORY_ORDER).reset_index()
    fig = px.pie(
        distribution,
        values="jumlah_warga",
        names="kategori_iid_rt",
        color="kategori_iid_rt",
        color_discrete_map=CATEGORY_COLORS,
        hole=0.45,
    )
    fig.update_layout(title="Komposisi warga menurut kategori IID-RT", margin=dict(l=10, r=10, t=55, b=10))
    return fig


def build_top_bottom_desa_figure(desa_df: pd.DataFrame, mode: str) -> go.Figure:
    if mode == "top":
        chart_df = desa_df.nlargest(10, "iid_desa").sort_values("iid_desa")
        title = "10 desa dengan IID tertinggi"
        color = "#0f766e"
    else:
        chart_df = desa_df.nsmallest(10, "iid_desa").sort_values("iid_desa")
        title = "10 desa dengan IID terendah"
        color = "#b91c1c"

    fig = px.bar(
        chart_df,
        x="iid_desa",
        y="deskel",
        orientation="h",
        text_auto=".3f",
        color_discrete_sequence=[color],
    )
    fig.update_layout(
        title=title,
        xaxis_title="Skor IID desa",
        yaxis_title="Desa/kelurahan",
        margin=dict(l=20, r=20, t=55, b=20),
        showlegend=False,
    )
    return fig


def build_dimension_profile_figure(desa_df: pd.DataFrame) -> go.Figure:
    rows: list[dict[str, Any]] = []
    for column, label in DIMENSION_LABELS.items():
        if column in desa_df.columns:
            rows.append({"dimensi": label, "skor": pd.to_numeric(desa_df[column], errors="coerce").mean()})
    profile_df = pd.DataFrame(rows)
    fig = px.bar(
        profile_df,
        x="dimensi",
        y="skor",
        color="skor",
        color_continuous_scale=["#d8f3eb", "#0f766e", "#163249"],
        text_auto=".3f",
    )
    fig.update_layout(
        title="Profil rata-rata dimensi pada tingkat desa",
        xaxis_title="Dimensi",
        yaxis_title="Skor rata-rata",
        coloraxis_showscale=False,
        margin=dict(l=20, r=20, t=55, b=20),
    )
    fig.update_yaxes(range=[0, 1])
    return fig


def build_gini_scatter_figure(desa_df: pd.DataFrame) -> go.Figure:
    fig = px.scatter(
        desa_df,
        x="iid_desa",
        y="gini_iid_rumah_tangga",
        size="jumlah_kk",
        hover_name="deskel",
        color="interpretasi_gini",
        color_discrete_map=GINI_COLORS,
    )
    fig.update_layout(
        title="Relasi IID desa dan Gini rumah tangga dengan kategori relatif tertil",
        xaxis_title="IID desa",
        yaxis_title="Gini IID rumah tangga",
        legend_title_text="Kategori relatif",
        margin=dict(l=20, r=20, t=55, b=20),
    )
    return fig


def add_ikd_quartile_columns(desa_df: pd.DataFrame) -> pd.DataFrame:
    enriched_df = desa_df.copy()
    enriched_df["ikd_desa"] = pd.to_numeric(enriched_df["ikd_desa"], errors="coerce")
    valid_count = int(enriched_df["ikd_desa"].notna().sum())
    if valid_count < 4:
        enriched_df["ikd_kuartil"] = pd.NA
        enriched_df["kategori_kuartil"] = pd.NA
        return enriched_df

    ranked_values = enriched_df["ikd_desa"].rank(method="first")
    quartile_series = pd.qcut(ranked_values, q=4, labels=IKD_QUARTILE_ORDER)
    enriched_df["ikd_kuartil"] = quartile_series.astype("string")
    enriched_df["kategori_kuartil"] = enriched_df["ikd_kuartil"].map(IKD_QUARTILE_LABELS)
    return enriched_df


def build_ikd_quartile_distribution_figure(desa_df: pd.DataFrame) -> go.Figure:
    plot_df = (
        desa_df["ikd_kuartil"]
        .astype("string")
        .value_counts(dropna=False)
        .reindex(IKD_QUARTILE_ORDER, fill_value=0)
        .rename_axis("ikd_kuartil")
        .reset_index(name="jumlah_desa")
    )
    plot_df["kategori_kuartil"] = plot_df["ikd_kuartil"].map(IKD_QUARTILE_LABELS)
    plot_df["persentase_desa"] = plot_df["jumlah_desa"] / max(int(plot_df["jumlah_desa"].sum()), 1)
    fig = px.bar(
        plot_df,
        x="ikd_kuartil",
        y="jumlah_desa",
        color="ikd_kuartil",
        color_discrete_map=IKD_QUARTILE_COLORS,
        text_auto=True,
        hover_data={"kategori_kuartil": True, "persentase_desa": ":.2%"},
    )
    fig.update_layout(
        title="Sebaran desa berdasarkan IKD kuartil",
        xaxis_title="IKD kuartil",
        yaxis_title="Jumlah desa",
        showlegend=False,
        margin=dict(l=20, r=20, t=55, b=20),
    )
    return fig


def build_ikd_quartile_scatter_figure(desa_df: pd.DataFrame) -> go.Figure:
    plot_df = desa_df.sort_values("ikd_desa").reset_index(drop=True).copy()
    plot_df["urutan_desa"] = plot_df.index + 1
    fig = px.scatter(
        plot_df,
        x="urutan_desa",
        y="ikd_desa",
        color="ikd_kuartil",
        color_discrete_map=IKD_QUARTILE_COLORS,
        hover_name="deskel",
        hover_data={"kategori_kuartil": True, "jumlah_kk": True, "urutan_desa": False},
    )
    fig.update_traces(marker=dict(size=9, opacity=0.82))
    fig.update_layout(
        title="Sebaran nilai IKD desa menurut kuartil",
        xaxis_title="Urutan desa setelah diurutkan dari IKD terendah",
        yaxis_title="Skor IKD desa",
        legend_title_text="Kuartil",
        margin=dict(l=20, r=20, t=55, b=20),
    )
    return fig


def build_desa_distribution_figure(distribution_df: pd.DataFrame, top_n: int) -> go.Figure:
    plot_df = distribution_df.copy()
    plot_df["jumlah_kk"] = pd.to_numeric(plot_df["jumlah_kk"], errors="coerce")
    plot_df["persentase_kk"] = pd.to_numeric(plot_df["persentase_kk"], errors="coerce")

    top_desa = (
        plot_df[["deskel", "total_kk_desa"]]
        .drop_duplicates()
        .assign(total_kk_desa=lambda df: pd.to_numeric(df["total_kk_desa"], errors="coerce"))
        .nlargest(top_n, "total_kk_desa")
    )
    filtered = plot_df[plot_df["deskel"].isin(top_desa["deskel"])]
    pivot_df = (
        filtered.pivot_table(index="deskel", columns="kategori_iid_rt", values="persentase_kk", aggfunc="sum")
        .fillna(0)
        .reindex(columns=[category for category in CATEGORY_ORDER if category in filtered["kategori_iid_rt"].unique()])
    )
    fig = go.Figure()
    for category in pivot_df.columns:
        fig.add_bar(
            x=pivot_df.index,
            y=pivot_df[category] * 100,
            name=category,
            marker_color=CATEGORY_COLORS.get(category, "#64748b"),
        )
    fig.update_layout(
        title=f"Komposisi kategori IID-RT pada {top_n} desa dengan RT terbanyak",
        xaxis_title="Desa/kelurahan",
        yaxis_title="Persentase rumah tangga",
        barmode="stack",
        margin=dict(l=20, r=20, t=55, b=20),
        legend_title_text="Kategori",
    )
    return fig


def build_map_figure(household_df: pd.DataFrame) -> go.Figure | None:
    lat_col, lon_col = get_coordinate_columns(household_df)
    if not lat_col or not lon_col:
        return None

    map_df = household_df.copy()
    map_df[lat_col] = pd.to_numeric(map_df[lat_col], errors="coerce")
    map_df[lon_col] = pd.to_numeric(map_df[lon_col], errors="coerce")
    map_df = map_df.dropna(subset=[lat_col, lon_col, "iid_rumah_tangga"])
    if map_df.empty:
        return None

    sample_size = min(2500, len(map_df))
    map_df = map_df.sample(sample_size, random_state=42) if len(map_df) > sample_size else map_df
    fig = px.scatter_mapbox(
        map_df,
        lat=lat_col,
        lon=lon_col,
        color="kategori_iid_rt" if "kategori_iid_rt" in map_df.columns else None,
        color_discrete_map=CATEGORY_COLORS,
        hover_name="deskel" if "deskel" in map_df.columns else None,
        hover_data={"iid_rumah_tangga": ":.3f"},
        zoom=8,
        height=520,
    )
    fig.update_layout(
        mapbox_style="open-street-map",
        title="Sebaran lokasi rumah tangga valid",
        margin=dict(l=10, r=10, t=55, b=10),
        legend_title_text="Kategori",
    )
    return fig


def build_table_overview(df: pd.DataFrame) -> pd.DataFrame:
    total_cells = int(df.shape[0] * df.shape[1])
    missing_cells = int(df.isna().sum().sum())
    numeric_count = int(len(df.select_dtypes(include="number").columns))
    text_count = int(len(df.columns) - numeric_count)
    overview_rows = [
        {"metrik": "Jumlah baris", "nilai": int(df.shape[0])},
        {"metrik": "Jumlah kolom", "nilai": int(df.shape[1])},
        {"metrik": "Kolom numerik", "nilai": numeric_count},
        {"metrik": "Kolom non numerik", "nilai": text_count},
        {"metrik": "Sel kosong", "nilai": missing_cells},
        {"metrik": "Persentase sel kosong", "nilai": format_percent(missing_cells / total_cells if total_cells else 0)},
    ]
    return pd.DataFrame(overview_rows)


def build_column_profile(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    total_rows = max(len(df), 1)
    for column in df.columns:
        series = df[column]
        preview_values = [str(value) for value in series.dropna().astype(str).head(3).tolist()]
        rows.append(
            {
                "kolom": column,
                "tipe_data": str(series.dtype),
                "terisi": int(series.notna().sum()),
                "kosong": int(series.isna().sum()),
                "persen_kosong": format_percent(series.isna().sum() / total_rows),
                "unik": int(series.nunique(dropna=True)),
                "contoh_nilai": ", ".join(preview_values) if preview_values else "-",
            }
        )
    return pd.DataFrame(rows)


def render_column_detail(df: pd.DataFrame, column_name: str) -> None:
    series = df[column_name]
    detail_cols = st.columns(4)
    detail_cols[0].metric("Tipe data", str(series.dtype))
    detail_cols[1].metric("Nilai terisi", format_number(int(series.notna().sum()), 0))
    detail_cols[2].metric("Nilai unik", format_number(int(series.nunique(dropna=True)), 0))
    detail_cols[3].metric("Nilai kosong", format_number(int(series.isna().sum()), 0))

    if pd.api.types.is_numeric_dtype(series):
        stats_df = series.describe(percentiles=[0.25, 0.5, 0.75]).rename("nilai").reset_index()
        stats_df.columns = ["statistik", "nilai"]
        st.dataframe(stats_df, use_container_width=True, hide_index=True)
    else:
        top_values = series.fillna("NA").astype(str).value_counts().head(10).reset_index()
        top_values.columns = ["nilai", "frekuensi"]
        st.dataframe(top_values, use_container_width=True, hide_index=True)


def csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def excel_bytes_from_sheets(sheets: dict[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    prepared_sheets = {
        sheet_name[:31]: iid_pipeline.round_numeric_dataframe(df.copy())
        for sheet_name, df in sheets.items()
    }
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, df in prepared_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        iid_pipeline.apply_excel_number_formats(writer.book, prepared_sheets)
    buffer.seek(0)
    return buffer.getvalue()


def collect_advanced_analysis_tables_for_download(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    ordered_keys = (
        "analisis_determinasi_dimensi",
        "analisis_determinasi_variabel",
        "analisis_sensitivitas_oat",
        "analisis_shapley_variabel",
    )
    collected: dict[str, pd.DataFrame] = {}
    for key in ordered_keys:
        df = tables.get(key, pd.DataFrame()).copy()
        if not df.empty:
            collected[key] = df
    return collected


def render_sidebar() -> None:
    ensure_request_state()
    default_output_dir = detect_default_output_dir()

    st.sidebar.markdown("## Pengaturan dashboard")
    with st.sidebar.form("dashboard_loader_form"):
        source_mode = st.radio(
            "Sumber data",
            options=("Folder hasil siap pakai", "Olah dari file mentah"),
            index=0 if st.session_state.dashboard_request.get("mode") == "folder_hasil" else 1,
        )

        if source_mode == "Folder hasil siap pakai":
            output_dir = st.text_input("Folder output", value=st.session_state.dashboard_request.get("output_dir", str(default_output_dir)))
            submit = st.form_submit_button("Tampilkan dashboard")
            if submit:
                st.session_state.dashboard_request = {
                    "mode": "folder_hasil",
                    "output_dir": output_dir,
                }
        else:
            uploaded_file = st.file_uploader("Upload file CSV/XLSX", type=["csv", "xlsx", "xls"])
            input_path = st.text_input("Atau path file lokal", value=str(BASE_DIR / "data_asli.csv"))
            scheme = st.selectbox("Skema perhitungan", options=["baseline", "rekomendasi"], index=1)
            school_age_min = st.number_input("Batas usia sekolah minimum", min_value=0, max_value=100, value=7, step=1)
            school_age_max = st.number_input("Batas usia sekolah maksimum", min_value=0, max_value=100, value=25, step=1)
            missing_threshold = st.slider("Ambang indikator inti hilang", min_value=0.0, max_value=1.0, value=0.20, step=0.01)
            submit = st.form_submit_button("Proses dan tampilkan")
            if submit:
                source_path = save_uploaded_file(uploaded_file) if uploaded_file is not None else Path(input_path)
                st.session_state.dashboard_request = {
                    "mode": "olah_ulang",
                    "input_path": str(source_path),
                    "scheme": scheme,
                    "school_age_min": int(school_age_min),
                    "school_age_max": int(school_age_max),
                    "missing_threshold": float(missing_threshold),
                }

    st.sidebar.markdown(
        """
        <div class="small-muted">
            Dashboard bisa memuat hasil CSV yang sudah ada, atau menjalankan ulang pipeline dari
            file mentah lalu menampilkan hasilnya langsung di browser.
        </div>
        """,
        unsafe_allow_html=True,
    )


def resolve_bundle_from_request() -> dict[str, Any]:
    request = st.session_state.dashboard_request
    if request["mode"] == "folder_hasil":
        output_dir = Path(request["output_dir"])
        return load_output_bundle(output_dir)

    input_path = Path(request["input_path"])
    return process_input_bundle(
        input_path=input_path,
        scheme=request["scheme"],
        school_age_min=request["school_age_min"],
        school_age_max=request["school_age_max"],
        missing_threshold=request["missing_threshold"],
    )


def render_household_resource_section(detail_df: pd.DataFrame, section_key: str) -> None:
    if detail_df.empty:
        st.info("Statistik jumlah HP, anggota keluarga, dan biaya komunikasi belum bisa dihitung karena file sumber mentah tidak tersedia.")
        return

    summary = build_household_resource_summary(detail_df)
    metric_cols = st.columns(4)
    metric_cols[0].metric("Rata-rata jumlah HP", format_number(summary["avg_hp"], 2))
    metric_cols[1].metric("Rata-rata anggota keluarga", format_number(summary["avg_members"], 2))
    metric_cols[2].metric("Rata-rata biaya komunikasi", format_currency(summary["avg_comm"], 0))
    metric_cols[3].metric("Median biaya komunikasi", format_currency(summary["median_comm"], 0))

    chart_cols = st.columns(2)
    chart_cols[0].plotly_chart(
        build_household_average_figure(detail_df),
        use_container_width=True,
        key=f"{section_key}_avg_hp_members",
    )
    chart_cols[1].plotly_chart(
        build_comm_cost_distribution_figure(detail_df),
        use_container_width=True,
        key=f"{section_key}_comm_distribution",
    )

    st.plotly_chart(
        build_hp_member_scatter_figure(detail_df),
        use_container_width=True,
        key=f"{section_key}_hp_member_scatter",
    )

    bottom_cols = st.columns(2)
    bottom_cols[0].plotly_chart(
        build_household_resource_by_desa_figure(detail_df, "hp_jumlah_num"),
        use_container_width=True,
        key=f"{section_key}_hp_by_desa",
    )
    bottom_cols[1].plotly_chart(
        build_household_resource_by_desa_figure(detail_df, "jml_keluarga"),
        use_container_width=True,
        key=f"{section_key}_members_by_desa",
    )

    if "rp_komunikasi_tertinggi" in detail_df.columns and detail_df["rp_komunikasi_tertinggi"].notna().any():
        st.plotly_chart(
            build_household_resource_by_desa_figure(detail_df, "rp_komunikasi_tertinggi"),
            use_container_width=True,
            key=f"{section_key}_comm_by_desa",
        )


def render_overall_inequality_section(tables: dict[str, pd.DataFrame]) -> None:
    summary_df, contributor_df = resolve_inequality_tables(tables)
    if summary_df.empty or contributor_df.empty:
        return

    overall_summary = summary_df.loc[summary_df["cakupan_analisis"].astype("string").eq("keseluruhan")].head(1)
    overall_contributors = contributor_df.loc[
        contributor_df["cakupan_analisis"].astype("string").eq("keseluruhan")
    ].copy()
    if overall_summary.empty or overall_contributors.empty:
        return

    top_row = overall_contributors.sort_values("peringkat_kontribusi", ascending=True, kind="mergesort").head(1)
    top_label = "-"
    if not top_row.empty:
        top_label = f"{top_row['family_id'].iloc[0]} ({format_percent(top_row['porsi_kontribusi_gini'].iloc[0])})"

    st.markdown("### Ketimpangan keseluruhan")
    st.caption(
        "Kontribusi dihitung dari total selisih skor IID-RT terhadap rumah tangga lain. Nilai kontribusi yang besar berarti rumah tangga itu berada cukup jauh dari pola umum, sehingga lebih kuat membentuk ketimpangan. Label kategori Gini mengikuti tertil relatif antar desa dalam sampel penelitian."
    )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Gini keseluruhan", format_number(overall_summary["gini_iid_rumah_tangga"].iloc[0]))
    metric_cols[1].metric("Kategori relatif", str(overall_summary["interpretasi_gini"].iloc[0]))
    metric_cols[2].metric("RT terlibat", format_number(overall_summary["jumlah_kk"].iloc[0], 0))
    metric_cols[3].metric("Kontributor utama", top_label)

    chart_cols = st.columns([1.15, 0.85])
    chart_cols[0].plotly_chart(
        build_top_inequality_contributors_figure(
            overall_contributors,
            title="Rumah tangga dengan kontribusi ketimpangan terbesar secara keseluruhan",
        ),
        use_container_width=True,
        key="overall_inequality_contributors",
    )
    with chart_cols[1]:
        preview_columns = [
            column
            for column in (
                "family_id",
                "deskel",
                "iid_rumah_tangga",
                "arah_deviasi",
                "porsi_kontribusi_gini",
            )
            if column in overall_contributors.columns
        ]
        preview_df = overall_contributors[preview_columns].head(15).copy()
        if "porsi_kontribusi_gini" in preview_df.columns:
            preview_df["porsi_kontribusi_gini"] = preview_df["porsi_kontribusi_gini"].map(
                lambda value: format_percent(value)
            )
        st.dataframe(preview_df, use_container_width=True, hide_index=True)


def render_summary_tab(tables: dict[str, pd.DataFrame], detail_df: pd.DataFrame) -> None:
    st.markdown("<span class='pill-note'>Ringkasan utama</span>", unsafe_allow_html=True)
    st.markdown(
        "<div class='section-note'>Lihat gambaran umum skor indeks, distribusi rumah tangga, dan catatan hasil pengolahan.</div>",
        unsafe_allow_html=True,
    )
    render_top_summary_metrics(tables)

    keluarga_df = tables.get("data_keluarga", pd.DataFrame())
    desa_df = tables.get("indeks_desa", pd.DataFrame())
    warga_df = tables.get("sebaran_warga_iid_rt", pd.DataFrame())
    household_df = get_household_rows(keluarga_df)

    if not household_df.empty:
        chart_cols = st.columns(2)
        chart_cols[0].plotly_chart(
            build_category_count_figure(household_df),
            use_container_width=True,
            key="summary_category_count",
        )
        chart_cols[1].plotly_chart(
            build_household_histogram_figure(household_df),
            use_container_width=True,
            key="summary_household_histogram",
        )

    if not warga_df.empty:
        warga_col, ringkas_col = st.columns([1.15, 0.85])
        warga_col.plotly_chart(
            build_person_distribution_figure(warga_df),
            use_container_width=True,
            key="summary_person_distribution",
        )
        with ringkas_col:
            st.markdown("### Ringkasan pengolahan")
            summary_df = tables.get("ringkasan_pengolahan", pd.DataFrame())
            if summary_df.empty:
                st.info("Ringkasan pengolahan belum tersedia untuk sumber data ini.")
            else:
                st.dataframe(summary_df, use_container_width=True, hide_index=True)
    elif not desa_df.empty:
        st.markdown("### Ringkasan pengolahan")
        summary_df = tables.get("ringkasan_pengolahan", pd.DataFrame())
        if summary_df.empty:
            st.info("Ringkasan pengolahan belum tersedia untuk sumber data ini.")
        else:
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

    st.markdown("### Profil HP, anggota keluarga, dan biaya komunikasi")
    st.caption("Statistik ini dihitung pada tingkat rumah tangga valid.")
    render_household_resource_section(detail_df, section_key="summary_resource")
    render_overall_inequality_section(tables)


def render_household_tab(tables: dict[str, pd.DataFrame], detail_df: pd.DataFrame) -> None:
    keluarga_df = tables.get("data_keluarga", pd.DataFrame())
    household_df = get_household_rows(keluarga_df)

    if household_df.empty:
        st.warning("Tidak ada data rumah tangga valid yang bisa divisualisasikan.")
        return

    filter_cols = st.columns(2)
    desa_options = ["Semua desa"] + sorted(household_df["deskel"].dropna().astype(str).unique().tolist()) if "deskel" in household_df.columns else ["Semua desa"]
    selected_desa = filter_cols[0].selectbox("Filter desa", options=desa_options)
    kategori_options = ["Semua kategori"] + [category for category in CATEGORY_ORDER if category in household_df["kategori_iid_rt"].astype("string").unique().tolist()]
    selected_category = filter_cols[1].selectbox("Filter kategori IID-RT", options=kategori_options)

    filtered_df = household_df.copy()
    if selected_desa != "Semua desa" and "deskel" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["deskel"].astype(str) == selected_desa]
    if selected_category != "Semua kategori":
        filtered_df = filtered_df[filtered_df["kategori_iid_rt"].astype(str) == selected_category]

    st.caption(f"Menampilkan {len(filtered_df):,} rumah tangga valid.".replace(",", "."))

    filtered_detail_df = pd.DataFrame()
    if not detail_df.empty:
        filtered_detail_df = detail_df.copy()
        if selected_desa != "Semua desa" and "deskel" in filtered_detail_df.columns:
            filtered_detail_df = filtered_detail_df[filtered_detail_df["deskel"].astype(str) == selected_desa]
        if selected_category != "Semua kategori" and "kategori_iid_rt" in filtered_detail_df.columns:
            filtered_detail_df = filtered_detail_df[filtered_detail_df["kategori_iid_rt"].astype(str) == selected_category]

    chart_cols = st.columns(2)
    chart_cols[0].plotly_chart(
        build_category_count_figure(filtered_df),
        use_container_width=True,
        key="household_category_count",
    )
    chart_cols[1].plotly_chart(
        build_household_histogram_figure(filtered_df),
        use_container_width=True,
        key="household_histogram",
    )

    st.markdown("### Statistik struktur rumah tangga")
    render_household_resource_section(filtered_detail_df, section_key="household_resource")

    map_figure = build_map_figure(filtered_df)
    if map_figure is not None:
        st.plotly_chart(map_figure, use_container_width=True, key="household_map")

    preview_columns = [column for column in ("family_id", "deskel", "iid_rumah_tangga", "kategori_iid_rt", "dimensi_A", "dimensi_B", "dimensi_C", "dimensi_D", "dimensi_E") if column in filtered_df.columns]
    st.markdown("### Preview data rumah tangga valid")
    st.dataframe(filtered_df[preview_columns].head(200), use_container_width=True, hide_index=True)


def render_desa_tab(tables: dict[str, pd.DataFrame]) -> None:
    desa_df = normalize_desa_gini_table(tables.get("indeks_desa", pd.DataFrame()))
    distribution_df = tables.get("sebaran_iid_rt_desa", pd.DataFrame()).copy()
    gini_distribution_df = normalize_gini_distribution_table(tables.get("sebaran_gini_desa", pd.DataFrame()), desa_df)
    inequality_summary_df, inequality_contributor_df = resolve_inequality_tables(tables)
    if desa_df.empty:
        st.warning("Tabel indeks desa belum tersedia.")
        return

    numeric_columns = ["iid_desa", "gini_iid_rumah_tangga", "jumlah_kk"]
    for column in numeric_columns:
        if column in desa_df.columns:
            desa_df[column] = pd.to_numeric(desa_df[column], errors="coerce")
    if "ikd_desa" in desa_df.columns:
        desa_df["ikd_desa"] = pd.to_numeric(desa_df["ikd_desa"], errors="coerce")
        desa_df = add_ikd_quartile_columns(desa_df)

    top_cols = st.columns(2)
    top_cols[0].plotly_chart(
        build_top_bottom_desa_figure(desa_df, "top"),
        use_container_width=True,
        key="desa_top_iid",
    )
    top_cols[1].plotly_chart(
        build_top_bottom_desa_figure(desa_df, "bottom"),
        use_container_width=True,
        key="desa_bottom_iid",
    )

    mid_cols = st.columns(2)
    mid_cols[0].plotly_chart(
        build_dimension_profile_figure(desa_df),
        use_container_width=True,
        key="desa_dimension_profile",
    )
    mid_cols[1].plotly_chart(
        build_gini_scatter_figure(desa_df),
        use_container_width=True,
        key="desa_gini_scatter",
    )

    if not gini_distribution_df.empty:
        st.markdown("### Kategori relatif Gini antar desa")
        st.caption(
            "Karena seluruh nilai Gini desa berada pada rentang rendah secara absolut, pembeda posisi ketimpangan antar desa dibuat dengan tertil relatif: sepertiga terendah, sepertiga tengah, dan sepertiga tertinggi dalam sampel."
        )
        preview_columns = [
            column
            for column in ("interpretasi_gini", "rentang_gini", "jumlah_desa", "persentase_desa")
            if column in gini_distribution_df.columns
        ]
        gini_preview_df = gini_distribution_df[preview_columns].copy()
        if "persentase_desa" in gini_preview_df.columns:
            gini_preview_df["persentase_desa"] = gini_preview_df["persentase_desa"].map(format_percent)
        st.dataframe(gini_preview_df, use_container_width=True, hide_index=True)

    if not distribution_df.empty:
        top_n = st.slider("Jumlah desa pada grafik komposisi kategori", min_value=5, max_value=20, value=10, step=1)
        st.plotly_chart(
            build_desa_distribution_figure(distribution_df, top_n),
            use_container_width=True,
            key=f"desa_distribution_{top_n}",
        )

    desa_inequality_df = inequality_summary_df.loc[
        inequality_summary_df["cakupan_analisis"].astype("string").eq("desa")
    ].copy()
    desa_inequality_df = desa_inequality_df.dropna(subset=["deskel"], how="all")
    if not desa_inequality_df.empty:
        st.markdown("### Evaluasi ketimpangan per desa")
        st.caption(
            "Bagian ini menunjukkan kategori relatif ketimpangan setiap desa dan rumah tangga mana yang paling kuat mendorong ketimpangan di desa tersebut."
        )

        selector_df = desa_inequality_df[["kode_deskel", "deskel"]].drop_duplicates().copy()
        selector_df["label_desa"] = selector_df.apply(
            lambda row: (
                f"{row['deskel']} ({row['kode_deskel']})"
                if pd.notna(row.get("kode_deskel")) and str(row.get("kode_deskel")).strip() not in {"", "nan"}
                else str(row.get("deskel"))
            ),
            axis=1,
        )
        selected_label = st.selectbox(
            "Pilih desa untuk membaca kontributor ketimpangan",
            options=selector_df["label_desa"].tolist(),
            key="desa_inequality_selector",
        )
        selected_info = selector_df.loc[selector_df["label_desa"] == selected_label].iloc[0]
        selected_summary = desa_inequality_df.loc[
            desa_inequality_df["deskel"].astype("string").eq(str(selected_info["deskel"]))
        ].copy()
        if pd.notna(selected_info["kode_deskel"]):
            selected_summary = selected_summary.loc[
                selected_summary["kode_deskel"].astype("string").eq(str(selected_info["kode_deskel"]))
            ]
        selected_summary = selected_summary.head(1)

        selected_contributors = inequality_contributor_df.loc[
            (
                inequality_contributor_df["cakupan_analisis"].astype("string").eq("desa")
            )
            & (
                inequality_contributor_df["deskel_cakupan"].astype("string").eq(str(selected_info["deskel"]))
            )
        ].copy()
        if pd.notna(selected_info["kode_deskel"]):
            selected_contributors = selected_contributors.loc[
                selected_contributors["kode_deskel_cakupan"].astype("string").eq(str(selected_info["kode_deskel"]))
            ]

        if not selected_summary.empty and not selected_contributors.empty:
            top_contributor = selected_contributors.sort_values(
                "peringkat_kontribusi",
                ascending=True,
                kind="mergesort",
            ).head(1)
            top_contributor_label = "-"
            if not top_contributor.empty:
                top_contributor_label = (
                    f"{top_contributor['family_id'].iloc[0]} "
                    f"({format_percent(top_contributor['porsi_kontribusi_gini'].iloc[0])})"
                )

            metric_cols = st.columns(4)
            metric_cols[0].metric("Gini desa", format_number(selected_summary["gini_iid_rumah_tangga"].iloc[0]))
            metric_cols[1].metric("Kategori relatif", str(selected_summary["interpretasi_gini"].iloc[0]))
            metric_cols[2].metric("Jumlah KK", format_number(selected_summary["jumlah_kk"].iloc[0], 0))
            metric_cols[3].metric("Kontributor utama", top_contributor_label)

            chart_cols = st.columns([1.1, 0.9])
            chart_cols[0].plotly_chart(
                build_top_inequality_contributors_figure(
                    selected_contributors,
                    title=f"Kontributor ketimpangan terbesar di {selected_info['deskel']}",
                ),
                use_container_width=True,
                key="desa_selected_inequality_contributors",
            )
            with chart_cols[1]:
                preview_columns = [
                    column
                    for column in (
                        "family_id",
                        "iid_rumah_tangga",
                        "arah_deviasi",
                        "porsi_kontribusi_gini",
                    )
                    if column in selected_contributors.columns
                ]
                preview_df = selected_contributors[preview_columns].head(15).copy()
                if "porsi_kontribusi_gini" in preview_df.columns:
                    preview_df["porsi_kontribusi_gini"] = preview_df["porsi_kontribusi_gini"].map(format_percent)
                st.dataframe(preview_df, use_container_width=True, hide_index=True)

        ranking_columns = [
            column
            for column in (
                "kode_deskel",
                "deskel",
                "jumlah_kk",
                "gini_iid_rumah_tangga",
                "interpretasi_gini",
                "family_id_kontributor_utama",
                "porsi_kontributor_utama",
            )
            if column in desa_inequality_df.columns
        ]
        ranking_df = desa_inequality_df[ranking_columns].sort_values(
            ["gini_iid_rumah_tangga", "jumlah_kk"],
            ascending=[False, False],
            kind="mergesort",
        ).copy()
        if "porsi_kontributor_utama" in ranking_df.columns:
            ranking_df["porsi_kontributor_utama"] = ranking_df["porsi_kontributor_utama"].map(format_percent)
        st.dataframe(ranking_df, use_container_width=True, hide_index=True)

    if {"ikd_desa", "ikd_kuartil", "kategori_kuartil"}.issubset(desa_df.columns):
        st.markdown("### Sebaran desa berdasarkan IKD kuartil")
        st.caption("Kolom `ikd_kuartil` dan `kategori_kuartil` dihitung otomatis dari pembagian kuartil nilai `ikd_desa` pada seluruh desa yang tersedia.")
        quartile_cols = st.columns(2)
        quartile_cols[0].plotly_chart(
            build_ikd_quartile_distribution_figure(desa_df),
            use_container_width=True,
            key="desa_ikd_quartile_distribution",
        )
        quartile_cols[1].plotly_chart(
            build_ikd_quartile_scatter_figure(desa_df),
            use_container_width=True,
            key="desa_ikd_quartile_scatter",
        )

        quartile_preview_columns = [
            column
            for column in ("kode_deskel", "deskel", "jumlah_kk", "ikd_desa", "ikd_kuartil", "kategori_kuartil")
            if column in desa_df.columns
        ]
        st.dataframe(
            desa_df[quartile_preview_columns].sort_values("ikd_desa", ascending=True),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("### Preview indeks desa")
    st.dataframe(desa_df.head(100), use_container_width=True, hide_index=True)


def build_dimension_determinant_figure(determinant_df: pd.DataFrame) -> go.Figure:
    plot_df = determinant_df.dropna(subset=["R2 IID Desa"]).copy()
    fig = px.bar(
        plot_df,
        x="Dimensi",
        y="R2 IID Desa",
        color_discrete_sequence=["#0f766e"],
        text_auto=".3f",
    )
    fig.update_layout(
        title="Koefisien determinasi dimensi terhadap IID desa",
        yaxis_title="Nilai R2",
        xaxis_title="Dimensi",
        margin=dict(l=10, r=10, t=60, b=10),
        showlegend=False,
    )
    return fig


def build_oat_sensitivity_figure(oat_df: pd.DataFrame) -> go.Figure:
    plot_df = oat_df.melt(
        id_vars="Dimensi",
        value_vars=[
            column
            for column in ("Rata-rata Kenaikan IID (%)", "Rata-rata Penurunan IKD (%)")
            if column in oat_df.columns
        ],
        var_name="Metrik",
        value_name="Delta",
    ).dropna(subset=["Delta"])
    fig = px.bar(
        plot_df,
        x="Dimensi",
        y="Delta",
        color="Metrik",
        barmode="group",
        color_discrete_map={
            "Rata-rata Kenaikan IID (%)": "#0f766e",
            "Rata-rata Penurunan IKD (%)": "#b91c1c",
        },
        text_auto=".2f",
    )
    fig.update_layout(
        title="Perubahan rata-rata outcome pada simulasi OAT",
        yaxis_title="Perubahan rata-rata (%)",
        xaxis_title="Dimensi",
        margin=dict(l=10, r=10, t=60, b=10),
        legend_title_text="Outcome",
    )
    return fig


def build_shapley_figure(shapley_df: pd.DataFrame, value_column: str) -> go.Figure:
    plot_df = shapley_df.dropna(subset=[value_column]).copy()
    fig = px.bar(
        plot_df,
        x="Variabel",
        y=value_column,
        color="Dimensi",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(
        title=f"Kontribusi Shapley indikator menurut {value_column.lower()}",
        yaxis_title=value_column,
        xaxis_title="Variabel",
        margin=dict(l=10, r=10, t=60, b=10),
        showlegend=False,
    )
    return fig


def render_advanced_analysis_tab(tables: dict[str, pd.DataFrame]) -> None:
    dimension_df = tables.get("analisis_determinasi_dimensi", pd.DataFrame()).copy()
    variable_df = tables.get("analisis_determinasi_variabel", pd.DataFrame()).copy()
    oat_df = tables.get("analisis_sensitivitas_oat", pd.DataFrame()).copy()
    shapley_df = tables.get("analisis_shapley_variabel", pd.DataFrame()).copy()
    download_tables = collect_advanced_analysis_tables_for_download(tables)

    if dimension_df.empty and variable_df.empty and oat_df.empty and shapley_df.empty:
        st.info("Tabel analisis lanjutan belum tersedia.")
        return

    st.markdown("<span class='pill-note'>Analisis lanjutan</span>", unsafe_allow_html=True)
    st.markdown(
        "<div class='section-note'>Bagian ini menempatkan IID desa sebagai outcome utama. IKD desa dibaca sebagai komplemen `1 - IID`, sehingga ketika IID desa naik maka IKD desa akan turun.</div>",
        unsafe_allow_html=True,
    )

    if download_tables:
        st.markdown("### Unduh data analisis lanjutan")
        excel_sheet_map = {TABLE_SPECS[key]["label"]: df for key, df in download_tables.items()}
        st.download_button(
            label="Unduh semua tabel analisis lanjutan (Excel)",
            data=excel_bytes_from_sheets(excel_sheet_map),
            file_name="analisis_lanjutan_iid_desa.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        csv_download_cols = st.columns(len(download_tables))
        for column_container, (key, df) in zip(csv_download_cols, download_tables.items(), strict=False):
            with column_container:
                st.download_button(
                    label=f"Unduh {TABLE_SPECS[key]['label']}",
                    data=csv_bytes(df),
                    file_name=TABLE_SPECS[key]["filename"],
                    mime="text/csv",
                    use_container_width=True,
                    key=f"download_{key}",
                )

    subtab_dimensi, subtab_variabel, subtab_oat, subtab_shapley = st.tabs(
        ["Determinasi Dimensi", "Determinasi Variabel", "Sensitivitas OAT", "Shapley"]
    )

    with subtab_dimensi:
        if dimension_df.empty:
            st.info("Tabel determinasi dimensi belum tersedia.")
        else:
            st.plotly_chart(
                build_dimension_determinant_figure(dimension_df),
                use_container_width=True,
                key="advanced_dimension_determinant",
            )
            st.dataframe(dimension_df, use_container_width=True, hide_index=True)

    with subtab_variabel:
        if variable_df.empty:
            st.info("Tabel determinasi variabel belum tersedia.")
        else:
            dimension_options = ["Semua dimensi"] + variable_df["Dimensi"].dropna().astype(str).unique().tolist()
            selected_dimension = st.selectbox(
                "Pilih dimensi",
                options=dimension_options,
                key="advanced_variable_dimension_filter",
            )
            filtered_variable_df = variable_df.copy()
            if selected_dimension != "Semua dimensi":
                filtered_variable_df = filtered_variable_df[filtered_variable_df["Dimensi"].astype(str) == selected_dimension]

            metric_column = st.selectbox(
                "Metrik yang ditonjolkan",
                options=[column for column in ("R2 Dimensi", "R2 IID Desa") if column in filtered_variable_df.columns],
                key="advanced_variable_metric",
            )
            chart_df = filtered_variable_df.dropna(subset=[metric_column]).sort_values(metric_column, ascending=False)
            fig = px.bar(
                chart_df,
                x="Variabel",
                y=metric_column,
                color="Dimensi",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(
                title=f"Koefisien determinasi indikator menurut {metric_column.lower()}",
                yaxis_title=metric_column,
                xaxis_title="Variabel",
                margin=dict(l=10, r=10, t=60, b=10),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="advanced_variable_determinant")
            st.dataframe(filtered_variable_df, use_container_width=True, hide_index=True)

    with subtab_oat:
        if oat_df.empty:
            st.info("Tabel sensitivitas OAT belum tersedia.")
        else:
            st.caption(
                "Simulasi OAT menaikkan satu poin pada satu dimensi pada skala 0-100, lalu IID dihitung ulang. Nilai yang ditampilkan adalah rata-rata persentase kenaikan IID desa dan rata-rata persentase penurunan IKD desa terhadap kondisi awal."
            )
            st.plotly_chart(
                build_oat_sensitivity_figure(oat_df),
                use_container_width=True,
                key="advanced_oat_sensitivity",
            )
            st.dataframe(oat_df, use_container_width=True, hide_index=True)

    with subtab_shapley:
        if shapley_df.empty:
            st.info("Tabel kontribusi Shapley belum tersedia.")
        else:
            shapley_dimension_options = ["Semua dimensi"] + shapley_df["Dimensi"].dropna().astype(str).unique().tolist()
            selected_shapley_dimension = st.selectbox(
                "Pilih dimensi untuk Shapley",
                options=shapley_dimension_options,
                key="advanced_shapley_dimension_filter",
            )
            filtered_shapley_df = shapley_df.copy()
            if selected_shapley_dimension != "Semua dimensi":
                filtered_shapley_df = filtered_shapley_df[filtered_shapley_df["Dimensi"].astype(str) == selected_shapley_dimension]

            shapley_metric = st.selectbox(
                "Tampilan nilai Shapley",
                options=[
                    column
                    for column in (
                        "Shapley R2 IID Desa",
                        "Proporsi Shapley IID",
                    )
                    if column in filtered_shapley_df.columns
                ],
                key="advanced_shapley_metric",
            )
            st.plotly_chart(
                build_shapley_figure(filtered_shapley_df.sort_values(shapley_metric, ascending=False), shapley_metric),
                use_container_width=True,
                key="advanced_shapley_chart",
            )
            preview_df = filtered_shapley_df.copy()
            for column in ("Proporsi Shapley IID",):
                if column in preview_df.columns:
                    preview_df[column] = preview_df[column].map(
                    lambda value: format_percent(value) if pd.notna(value) else "-"
                    )
            st.dataframe(preview_df, use_container_width=True, hide_index=True)


def render_variable_tab(tables: dict[str, pd.DataFrame]) -> None:
    variable_df = tables.get("penjelasan_variabel", pd.DataFrame()).copy()
    if variable_df.empty:
        st.warning("Tabel penjelasan variabel belum tersedia.")
        return

    filter_cols = st.columns(2)
    dimensi_options = ["Semua dimensi"] + sorted(variable_df["dimensi"].dropna().astype(str).unique().tolist()) if "dimensi" in variable_df.columns else ["Semua dimensi"]
    selected_dimension = filter_cols[0].selectbox("Filter dimensi", options=dimensi_options)
    keyword = filter_cols[1].text_input("Cari variabel atau konsep", value="")

    filtered_df = variable_df.copy()
    if selected_dimension != "Semua dimensi" and "dimensi" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["dimensi"].astype(str) == selected_dimension]
    if keyword.strip():
        keyword_mask = filtered_df.apply(
            lambda row: keyword.lower() in " ".join(str(value).lower() for value in row.values),
            axis=1,
        )
        filtered_df = filtered_df[keyword_mask]

    st.caption(f"Menampilkan {len(filtered_df):,} baris penjelasan variabel.".replace(",", "."))
    st.dataframe(filtered_df, use_container_width=True, hide_index=True)

    if not filtered_df.empty and "nama_variabel" in filtered_df.columns:
        chosen_variable = st.selectbox("Pilih variabel untuk melihat detail", options=filtered_df["nama_variabel"].astype(str).tolist())
        selected_row = filtered_df.loc[filtered_df["nama_variabel"].astype(str) == chosen_variable].head(1).T.reset_index()
        selected_row.columns = ["atribut", "nilai"]
        st.markdown("### Detail variabel")
        st.dataframe(selected_row, use_container_width=True, hide_index=True)


def render_table_explorer_tab(tables: dict[str, pd.DataFrame]) -> None:
    available_keys = [key for key in TABLE_SPECS if key in tables]
    option_labels = {TABLE_SPECS[key]["label"]: key for key in available_keys}
    selected_label = st.selectbox("Pilih tabel", options=list(option_labels.keys()))
    selected_key = option_labels[selected_label]
    df = tables[selected_key]
    spec = TABLE_SPECS[selected_key]

    st.markdown(f"### {spec['label']}")
    st.caption(spec["description"])

    overview_cols = st.columns([0.9, 1.1])
    with overview_cols[0]:
        st.markdown("#### Deskripsi tabel")
        st.dataframe(build_table_overview(df), use_container_width=True, hide_index=True)
    with overview_cols[1]:
        st.markdown("#### Profil kolom")
        st.dataframe(build_column_profile(df), use_container_width=True, hide_index=True)

    if len(df.columns) > 0:
        inspected_column = st.selectbox("Kolom yang ingin diperiksa", options=df.columns.tolist())
        render_column_detail(df, inspected_column)

    preview_limit = st.slider("Jumlah baris preview", min_value=20, max_value=300, value=100, step=20)
    st.markdown("#### Preview data")
    st.dataframe(df.head(preview_limit), use_container_width=True, hide_index=True)

    st.download_button(
        label=f"Unduh {spec['filename']}",
        data=csv_bytes(df),
        file_name=spec["filename"],
        mime="text/csv",
    )


def render_scheme_tables(tables: dict[str, pd.DataFrame]) -> None:
    optional_keys = [key for key in ("batas_kategori_iid_rt", "perbandingan_skema", "skema_rekomendasi") if key in tables]
    if not optional_keys:
        return

    st.markdown("### Tabel tambahan skema")
    for key in optional_keys:
        st.markdown(f"#### {TABLE_SPECS[key]['label']}")
        st.caption(TABLE_SPECS[key]["description"])
        st.dataframe(tables[key], use_container_width=True, hide_index=True)


def main() -> None:
    inject_styles()
    render_sidebar()

    try:
        with st.spinner("Memuat dashboard dan tabel hasil olah data..."):
            bundle = resolve_bundle_from_request()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    meta = bundle["meta"]
    tables = bundle["tables"]

    with st.spinner("Menghitung statistik HP, anggota keluarga, dan biaya komunikasi..."):
        household_detail_df = resolve_household_detail_df(meta, tables)

    render_hero(meta)

    if meta.get("workbook_path"):
        st.markdown(
            f"<div class='small-muted'>Workbook Excel tersedia di <code>{meta['workbook_path']}</code></div>",
            unsafe_allow_html=True,
        )

    tab_ringkasan, tab_rt, tab_desa, tab_analisis, tab_variabel, tab_tabel = st.tabs(
        ["Ringkasan", "Rumah Tangga", "Desa", "Analisis Lanjutan", "Penjelasan Variabel", "Eksplorasi Tabel"]
    )

    with tab_ringkasan:
        render_summary_tab(tables, household_detail_df)
        render_scheme_tables(tables)

    with tab_rt:
        render_household_tab(tables, household_detail_df)

    with tab_desa:
        render_desa_tab(tables)

    with tab_analisis:
        render_advanced_analysis_tab(tables)

    with tab_variabel:
        render_variable_tab(tables)

    with tab_tabel:
        render_table_explorer_tab(tables)


if __name__ == "__main__":
    main()
