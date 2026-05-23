import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from scipy import stats
from plotly.subplots import make_subplots


DATA_PATH = "data_arsyad.xlsx"
SHEET_NAME = "UJIUJI"

COLOR_MAP = {
    "Semua": "#2563eb",
    "Digital": "#0f766e",
    "Konvensional": "#b91c1c",
}

VARIABLE_LABELS = {
    "Kesiapan Teknologi": "X1 Teknologi",
    "Kesiapan Pemuda": "X2 Pemuda",
    "Partisipasi Pemuda": "Y Partisipasi",
}

VARIABLE_SETS = {
    "Total": {
        "x1": 33,
        "y": 52,
        "x2": 131,
        "x1_label": "Kesiapan Teknologi (Total)",
        "x2_label": "Kesiapan Pemuda (Total)",
        "y_label": "Partisipasi Pemuda (Total)",
    },
    "Rata-rata": {
        "x1": 34,
        "y": 53,
        "x2": 132,
        "x1_label": "Kesiapan Teknologi (Rata-rata)",
        "x2_label": "Kesiapan Pemuda (Rata-rata)",
        "y_label": "Partisipasi Pemuda (Rata-rata)",
    },
}


st.set_page_config(
    page_title="Uji Regresi Ganda",
    page_icon="RG",
    layout="wide",
)


st.markdown(
    """
    <style>
    :root {
        --ink: #0f172a;
        --muted: #475569;
        --line: rgba(15, 23, 42, 0.12);
        --panel: rgba(255, 255, 255, 0.88);
        --panel-strong: rgba(255, 255, 255, 0.96);
    }
    .stApp {
        background:
            radial-gradient(900px 360px at 5% -8%, rgba(37, 99, 235, 0.18), rgba(37, 99, 235, 0) 64%),
            radial-gradient(680px 340px at 95% 0%, rgba(15, 118, 110, 0.13), rgba(15, 118, 110, 0) 62%),
            linear-gradient(180deg, #f8fafc 0%, #eef2f7 58%, #ffffff 100%);
        color: var(--ink);
    }
    .main .block-container {
        max-width: 1380px;
        padding-top: 1rem;
        padding-bottom: 2rem;
    }
    .dashboard-hero {
        border: 1px solid var(--line);
        background: linear-gradient(135deg, rgba(255,255,255,0.96), rgba(241,245,249,0.86));
        border-radius: 8px;
        padding: 20px 22px 18px 22px;
        box-shadow: 0 22px 55px rgba(15, 23, 42, 0.10);
        margin-bottom: 16px;
    }
    .dashboard-hero h1 {
        font-size: clamp(1.6rem, 2.4vw, 2.35rem);
        line-height: 1.12;
        margin: 0 0 8px 0;
        letter-spacing: 0;
        color: var(--ink);
    }
    .dashboard-hero p {
        margin: 0;
        max-width: 980px;
        color: var(--muted);
        font-size: 1rem;
    }
    .analysis-chip-wrap {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 14px;
    }
    .analysis-chip {
        border: 1px solid rgba(15, 23, 42, 0.12);
        background: rgba(255, 255, 255, 0.82);
        border-radius: 8px;
        padding: 6px 10px;
        font-size: 0.86rem;
        color: #334155;
    }
    div[data-testid="stMetric"] {
        background: var(--panel-strong);
        border: 1px solid rgba(15, 23, 42, 0.10);
        border-radius: 8px;
        padding: 14px 16px;
        box-shadow: 0 14px 34px rgba(15, 23, 42, 0.07);
    }
    .section-title {
        font-size: 1.15rem;
        font-weight: 750;
        margin: 0.4rem 0 0.6rem 0;
    }
    div[data-testid="stDataFrame"] {
        border: 1px solid rgba(15, 23, 42, 0.08);
        border-radius: 8px;
        overflow: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

px.defaults.template = "plotly_white"


def p_value_label(p_value):
    if pd.isna(p_value):
        return "-"
    if p_value < 0.001:
        return "< 0.001"
    return f"{p_value:.3f}"


def significance_label(p_value, alpha=0.05):
    if pd.isna(p_value):
        return "Tidak tersedia"
    return "Signifikan" if p_value < alpha else "Tidak signifikan"


@st.cache_data(show_spinner=False)
def load_regression_data(path):
    raw = pd.read_excel(path, sheet_name=SHEET_NAME, header=None)
    rows = raw.iloc[3:].copy()

    base = pd.DataFrame(
        {
            "Nama": rows.iloc[:, 1],
            "Jenis Kelamin": rows.iloc[:, 2],
            "Desa": rows.iloc[:, 3],
            "Umur": pd.to_numeric(rows.iloc[:, 4], errors="coerce"),
            "Pendidikan": rows.iloc[:, 5],
            "Nomor Responden": rows.iloc[:, 6].astype(str).str.strip(),
        }
    )

    base = base[base["Nomor Responden"].notna()]
    base = base[base["Nomor Responden"].str.lower() != "nan"]
    base["Skema"] = np.select(
        [
            base["Nomor Responden"].str.contains("digital", case=False, na=False),
            base["Nomor Responden"].str.contains("konvensional", case=False, na=False),
        ],
        ["Digital", "Konvensional"],
        default="Lainnya",
    )

    datasets = {}
    for score_name, cfg in VARIABLE_SETS.items():
        df = base.copy()
        df["Kesiapan Teknologi"] = pd.to_numeric(rows.iloc[:, cfg["x1"]], errors="coerce")
        df["Kesiapan Pemuda"] = pd.to_numeric(rows.iloc[:, cfg["x2"]], errors="coerce")
        df["Partisipasi Pemuda"] = pd.to_numeric(rows.iloc[:, cfg["y"]], errors="coerce")
        df = df.dropna(subset=["Kesiapan Teknologi", "Kesiapan Pemuda", "Partisipasi Pemuda"])
        datasets[score_name] = df.reset_index(drop=True)

    return datasets


def fit_ols(df, score_name, group_name):
    x_cols = ["Kesiapan Teknologi", "Kesiapan Pemuda"]
    y_col = "Partisipasi Pemuda"
    model_df = df[x_cols + [y_col]].dropna().copy()
    n = len(model_df)
    k = len(x_cols)
    p = k + 1

    empty_result = {
        "score_name": score_name,
        "group_name": group_name,
        "n": n,
        "is_valid": False,
        "reason": "Jumlah data tidak cukup untuk regresi ganda.",
    }
    if n <= p:
        return empty_result

    y = model_df[y_col].to_numpy(dtype=float)
    x = model_df[x_cols].to_numpy(dtype=float)
    x_design = np.column_stack([np.ones(n), x])
    beta = np.linalg.pinv(x_design) @ y
    y_hat = x_design @ beta
    residual = y - y_hat

    sse = float(np.sum(residual**2))
    sst = float(np.sum((y - np.mean(y)) ** 2))
    ssr = max(sst - sse, 0.0)
    df_model = k
    df_resid = n - p
    mse = sse / df_resid if df_resid > 0 else np.nan
    rmse = float(np.sqrt(mse)) if not pd.isna(mse) else np.nan
    r2 = 1 - (sse / sst) if sst > 0 else np.nan
    adj_r2 = 1 - ((1 - r2) * (n - 1) / df_resid) if sst > 0 and df_resid > 0 else np.nan
    f_stat = (ssr / df_model) / mse if mse and mse > 0 else np.nan
    f_pvalue = stats.f.sf(f_stat, df_model, df_resid) if not pd.isna(f_stat) else np.nan

    xtx_inv = np.linalg.pinv(x_design.T @ x_design)
    se = np.sqrt(np.maximum(np.diag(mse * xtx_inv), 0)) if not pd.isna(mse) else np.full(p, np.nan)
    t_values = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se != 0)
    p_values = 2 * stats.t.sf(np.abs(t_values), df_resid)
    ci_low = beta - stats.t.ppf(0.975, df_resid) * se
    ci_high = beta + stats.t.ppf(0.975, df_resid) * se

    x_std = (x - x.mean(axis=0)) / x.std(axis=0, ddof=0)
    y_std = (y - y.mean()) / y.std(ddof=0)
    std_beta = np.linalg.pinv(x_std) @ y_std if np.all(np.isfinite(x_std)) and y.std(ddof=0) > 0 else [np.nan] * k

    coefficient_table = pd.DataFrame(
        {
            "Variabel": ["Intercept", *x_cols],
            "Koefisien B": beta,
            "Std. Error": se,
            "t hitung": t_values,
            "p-value": p_values,
            "Sig.": [significance_label(pv) for pv in p_values],
            "CI 95% bawah": ci_low,
            "CI 95% atas": ci_high,
            "Beta Standar": [np.nan, *std_beta],
        }
    )

    predictions = df.loc[model_df.index].copy()
    predictions["Prediksi Y"] = y_hat
    predictions["Residual"] = residual

    return {
        "score_name": score_name,
        "group_name": group_name,
        "n": n,
        "is_valid": True,
        "x_cols": x_cols,
        "y_col": y_col,
        "beta": beta,
        "r2": r2,
        "adj_r2": adj_r2,
        "f_stat": f_stat,
        "f_pvalue": f_pvalue,
        "rmse": rmse,
        "sse": sse,
        "df_model": df_model,
        "df_resid": df_resid,
        "coefficient_table": coefficient_table,
        "predictions": predictions,
    }


def model_equation(result):
    b0, b1, b2 = result["beta"]
    return (
        "Y = "
        f"{b0:.4f} + ({b1:.4f} x Kesiapan Teknologi) "
        f"+ ({b2:.4f} x Kesiapan Pemuda)"
    )


def style_table(df, decimals=4):
    return df.style.format(
        {
            col: f"{{:.{decimals}f}}"
            for col in df.select_dtypes(include=[np.number]).columns
        },
        na_rep="-",
    )


def filter_group(df, group_name):
    if group_name == "Semua":
        return df[df["Skema"].isin(["Digital", "Konvensional"])].copy()
    return df[df["Skema"] == group_name].copy()


def make_model_summary(results):
    rows = []
    for result in results:
        if not result["is_valid"]:
            rows.append(
                {
                    "Jenis Skor": result["score_name"],
                    "Kelompok": result["group_name"],
                    "N": result["n"],
                    "Multiple R": np.nan,
                    "R Square": np.nan,
                    "Adjusted R Square": np.nan,
                    "F hitung": np.nan,
                    "p-value F": np.nan,
                    "Keputusan Model": result["reason"],
                    "RMSE": np.nan,
                }
            )
            continue

        rows.append(
                {
                    "Jenis Skor": result["score_name"],
                    "Kelompok": result["group_name"],
                    "N": result["n"],
                    "Multiple R": np.sqrt(result["r2"]) if pd.notna(result["r2"]) and result["r2"] >= 0 else np.nan,
                    "R Square": result["r2"],
                    "Adjusted R Square": result["adj_r2"],
                    "F hitung": result["f_stat"],
                "p-value F": result["f_pvalue"],
                "Keputusan Model": significance_label(result["f_pvalue"]),
                "RMSE": result["rmse"],
            }
        )
    return pd.DataFrame(rows)


def make_coefficient_summary(results):
    tables = []
    for result in results:
        if result["is_valid"]:
            table = result["coefficient_table"].copy()
            table.insert(0, "Kelompok", result["group_name"])
            table.insert(0, "Jenis Skor", result["score_name"])
            tables.append(table)
    return pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()


def plot_scatter_matrix(df, title):
    plot_df = df.rename(columns=VARIABLE_LABELS)
    fig = px.scatter_matrix(
        plot_df,
        dimensions=["X1 Teknologi", "X2 Pemuda", "Y Partisipasi"],
        color="Skema",
        color_discrete_map=COLOR_MAP,
        title=title,
        opacity=0.78,
    )
    fig.update_traces(diagonal_visible=False, marker=dict(size=8, line=dict(width=0.5, color="white")))
    fig.update_layout(height=560, margin=dict(l=20, r=20, t=55, b=20))
    return fig


def plot_scheme_correlation_heatmap(df, title):
    variables = ["Kesiapan Teknologi", "Kesiapan Pemuda", "Partisipasi Pemuda"]
    labels = [VARIABLE_LABELS[var] for var in variables]
    groups = ["Semua", "Digital", "Konvensional"]

    fig = make_subplots(
        rows=1,
        cols=3,
        subplot_titles=groups,
        horizontal_spacing=0.08,
    )
    for idx, group_name in enumerate(groups, start=1):
        group_df = filter_group(df, group_name)
        corr = group_df[variables].corr().reindex(index=variables, columns=variables)
        fig.add_trace(
            go.Heatmap(
                z=corr.to_numpy(),
                x=labels,
                y=labels,
                zmin=-1,
                zmax=1,
                colorscale=[
                    [0, "#b91c1c"],
                    [0.5, "#f8fafc"],
                    [1, "#0f766e"],
                ],
                text=np.round(corr.to_numpy(), 3),
                texttemplate="%{text}",
                hovertemplate="%{y} vs %{x}<br>Korelasi: %{z:.3f}<extra></extra>",
                colorbar=dict(title="r", len=0.72) if idx == 3 else None,
                showscale=idx == 3,
            ),
            row=1,
            col=idx,
        )

    fig.update_layout(
        title=title,
        height=420,
        margin=dict(l=20, r=20, t=70, b=20),
    )
    fig.update_xaxes(tickangle=0)
    return fig


def plot_scheme_difference_heatmap(df, title):
    variables = ["Kesiapan Teknologi", "Kesiapan Pemuda", "Partisipasi Pemuda"]
    labels = [VARIABLE_LABELS[var] for var in variables]
    digital_corr = filter_group(df, "Digital")[variables].corr()
    conventional_corr = filter_group(df, "Konvensional")[variables].corr()
    diff = digital_corr - conventional_corr

    fig = go.Figure(
        data=go.Heatmap(
            z=diff.reindex(index=variables, columns=variables).to_numpy(),
            x=labels,
            y=labels,
            zmin=-1,
            zmax=1,
            colorscale=[
                [0, "#b91c1c"],
                [0.5, "#f8fafc"],
                [1, "#2563eb"],
            ],
            text=np.round(diff.reindex(index=variables, columns=variables).to_numpy(), 3),
            texttemplate="%{text}",
            colorbar=dict(title="Selisih r"),
            hovertemplate="%{y} vs %{x}<br>Digital - Konvensional: %{z:.3f}<extra></extra>",
        )
    )
    fig.update_layout(
        title=title,
        height=420,
        margin=dict(l=20, r=20, t=60, b=20),
    )
    return fig


def plot_model_metric_heatmap(summary, score_choice):
    score_summary = summary[summary["Jenis Skor"] == score_choice].copy()
    metric_cols = ["Multiple R", "R Square", "Adjusted R Square"]
    score_summary = score_summary.set_index("Kelompok").reindex(["Semua", "Digital", "Konvensional"])
    z = score_summary[metric_cols].to_numpy(dtype=float)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=metric_cols,
            y=score_summary.index.tolist(),
            zmin=0,
            zmax=1,
            colorscale=[
                [0, "#eff6ff"],
                [0.45, "#93c5fd"],
                [1, "#0f766e"],
            ],
            text=np.round(z, 3),
            texttemplate="%{text}",
            hovertemplate="%{y}<br>%{x}: %{z:.3f}<extra></extra>",
            colorbar=dict(title="Nilai"),
        )
    )
    fig.update_layout(
        title=f"Heatmap Kekuatan Model - {score_choice}",
        height=330,
        margin=dict(l=20, r=20, t=60, b=20),
    )
    return fig


def plot_scatter_with_trend(df, x_col, y_col, title):
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        color="Skema",
        color_discrete_map=COLOR_MAP,
        title=title,
        hover_data=["Nomor Responden", "Nama", "Desa"],
    )
    for group_name, group_df in df.groupby("Skema"):
        clean = group_df[[x_col, y_col]].dropna()
        if len(clean) < 2 or clean[x_col].nunique() < 2:
            continue
        slope, intercept = np.polyfit(clean[x_col].to_numpy(dtype=float), clean[y_col].to_numpy(dtype=float), 1)
        x_line = np.linspace(clean[x_col].min(), clean[x_col].max(), 50)
        y_line = intercept + slope * x_line
        fig.add_trace(
            go.Scatter(
                x=x_line,
                y=y_line,
                mode="lines",
                line=dict(color=COLOR_MAP.get(group_name, "#64748b"), width=2),
                name=f"Tren {group_name}",
                showlegend=True,
            )
        )
    fig.update_traces(marker=dict(size=9, line=dict(width=0.6, color="white")))
    fig.update_layout(
        height=440,
        margin=dict(l=20, r=20, t=55, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    return fig


def plot_actual_vs_predicted(result):
    pred = result["predictions"]
    fig = px.scatter(
        pred,
        x="Prediksi Y",
        y="Partisipasi Pemuda",
        color="Skema",
        color_discrete_map=COLOR_MAP,
        title=f"Aktual vs Prediksi - {result['score_name']} / {result['group_name']}",
        hover_data=["Nomor Responden", "Nama", "Residual"],
    )
    min_val = min(pred["Prediksi Y"].min(), pred["Partisipasi Pemuda"].min())
    max_val = max(pred["Prediksi Y"].max(), pred["Partisipasi Pemuda"].max())
    fig.add_trace(
        go.Scatter(
            x=[min_val, max_val],
            y=[min_val, max_val],
            mode="lines",
            line=dict(color="#0f172a", dash="dash"),
            name="Garis ideal",
        )
    )
    fig.update_layout(height=430, margin=dict(l=20, r=20, t=55, b=20))
    return fig


def plot_coefficients(result):
    coef = result["coefficient_table"].query("Variabel != 'Intercept'").copy()
    coef["Arah"] = np.where(coef["Koefisien B"] >= 0, "Positif", "Negatif")
    fig = px.bar(
        coef,
        x="Variabel",
        y="Koefisien B",
        color="Arah",
        color_discrete_map={"Positif": "#0f766e", "Negatif": "#b91c1c"},
        title=f"Koefisien Regresi - {result['score_name']} / {result['group_name']}",
        text=coef["Koefisien B"].map(lambda value: f"{value:.3f}"),
    )
    fig.add_hline(y=0, line_color="#334155", line_width=1)
    fig.update_layout(height=360, margin=dict(l=20, r=20, t=55, b=20), showlegend=False)
    return fig


def plot_residuals(result):
    pred = result["predictions"]
    fig = px.histogram(
        pred,
        x="Residual",
        nbins=18,
        marginal="box",
        title=f"Distribusi Residual - {result['score_name']} / {result['group_name']}",
        color_discrete_sequence=["#2563eb"],
    )
    fig.add_vline(x=0, line_dash="dash", line_color="#0f172a")
    fig.update_layout(height=360, margin=dict(l=20, r=20, t=55, b=20))
    return fig


def plot_3d_surface(result):
    pred = result["predictions"]
    b0, b1, b2 = result["beta"]
    x1 = pred["Kesiapan Teknologi"]
    x2 = pred["Kesiapan Pemuda"]
    x_grid = np.linspace(x1.min(), x1.max(), 24)
    y_grid = np.linspace(x2.min(), x2.max(), 24)
    xx, yy = np.meshgrid(x_grid, y_grid)
    zz = b0 + b1 * xx + b2 * yy

    fig = go.Figure()
    fig.add_trace(
        go.Surface(
            x=xx,
            y=yy,
            z=zz,
            colorscale="Blues",
            opacity=0.45,
            showscale=False,
            name="Bidang regresi",
        )
    )
    for group_name, group_df in pred.groupby("Skema"):
        fig.add_trace(
            go.Scatter3d(
                x=group_df["Kesiapan Teknologi"],
                y=group_df["Kesiapan Pemuda"],
                z=group_df["Partisipasi Pemuda"],
                mode="markers",
                marker=dict(size=5, color=COLOR_MAP.get(group_name, "#64748b"), line=dict(width=0.5, color="white")),
                name=group_name,
                text=group_df["Nomor Responden"],
            )
        )
    fig.update_layout(
        title=f"Bidang Regresi 3D - {result['score_name']} / {result['group_name']}",
        height=620,
        margin=dict(l=0, r=0, t=55, b=0),
        scene=dict(
            xaxis_title="Kesiapan Teknologi",
            yaxis_title="Kesiapan Pemuda",
            zaxis_title="Partisipasi Pemuda",
        ),
    )
    return fig


def render_result_detail(result, group_df):
    if not result["is_valid"]:
        st.warning(result["reason"])
        return

    multiple_r = np.sqrt(result["r2"]) if pd.notna(result["r2"]) and result["r2"] >= 0 else np.nan
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Jumlah Data", f"{result['n']}")
    c2.metric("Multiple R", f"{multiple_r:.3f}" if pd.notna(multiple_r) else "-")
    c3.metric("R Square", f"{result['r2']:.3f}")
    c4.metric("Adjusted R Square", f"{result['adj_r2']:.3f}")
    c5.metric("p-value F", p_value_label(result["f_pvalue"]))

    st.markdown(f"**Persamaan regresi:** `{model_equation(result)}`")

    st.markdown('<div class="section-title">Tabel Koefisien</div>', unsafe_allow_html=True)
    coef = result["coefficient_table"].copy()
    coef["p-value"] = coef["p-value"].map(p_value_label)
    st.dataframe(style_table(coef), use_container_width=True, hide_index=True)

    left, right = st.columns([1, 1])
    with left:
        st.plotly_chart(plot_coefficients(result), use_container_width=True)
    with right:
        st.plotly_chart(plot_actual_vs_predicted(result), use_container_width=True)

    st.plotly_chart(plot_3d_surface(result), use_container_width=True)

    st.plotly_chart(plot_residuals(result), use_container_width=True)

    with st.expander("Lihat Data, Prediksi, dan Residual"):
        shown_cols = [
            "Nomor Responden",
            "Skema",
            "Nama",
            "Kesiapan Teknologi",
            "Kesiapan Pemuda",
            "Partisipasi Pemuda",
            "Prediksi Y",
            "Residual",
        ]
        st.dataframe(style_table(result["predictions"][shown_cols]), use_container_width=True, hide_index=True)


def main():
    st.markdown(
        """
        <div class="dashboard-hero">
            <h1>Dashboard Uji Regresi Ganda</h1>
            <p>
                Analisis hubungan Kesiapan Teknologi dan Kesiapan Pemuda terhadap Partisipasi Pemuda,
                dengan pembandingan responden Digital dan Konvensional.
            </p>
            <div class="analysis-chip-wrap">
                <span class="analysis-chip">Y: Partisipasi Pemuda</span>
                <span class="analysis-chip">X1: Kesiapan Teknologi</span>
                <span class="analysis-chip">X2: Kesiapan Pemuda</span>
                <span class="analysis-chip">Skor: Total dan Rata-rata</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not os.path.exists(DATA_PATH):
        st.error(f"File `{DATA_PATH}` tidak ditemukan di folder aplikasi.")
        st.stop()

    datasets = load_regression_data(DATA_PATH)
    all_results = []
    for score_name, score_df in datasets.items():
        for group_name in ["Semua", "Digital", "Konvensional"]:
            group_df = filter_group(score_df, group_name)
            all_results.append(fit_ols(group_df, score_name, group_name))

    summary = make_model_summary(all_results)
    coef_summary = make_coefficient_summary(all_results)

    score_choice = st.segmented_control(
        "Jenis skor",
        options=["Total", "Rata-rata"],
        default="Total",
    )

    score_df = datasets[score_choice]
    respondent_counts = (
        score_df[score_df["Skema"].isin(["Digital", "Konvensional"])]
        .groupby("Skema")
        .size()
        .reindex(["Digital", "Konvensional"])
        .fillna(0)
        .astype(int)
    )
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Responden", f"{int(respondent_counts.sum())}")
    m2.metric("Digital", f"{respondent_counts.get('Digital', 0)}")
    m3.metric("Konvensional", f"{respondent_counts.get('Konvensional', 0)}")

    st.markdown('<div class="section-title">Ringkasan Model</div>', unsafe_allow_html=True)
    left, right = st.columns([1.15, 0.85])
    with left:
        st.dataframe(style_table(summary), use_container_width=True, hide_index=True)
    with right:
        st.plotly_chart(plot_model_metric_heatmap(summary, score_choice), use_container_width=True)

    st.markdown('<div class="section-title">Heatmap Perbandingan Skema</div>', unsafe_allow_html=True)
    st.plotly_chart(
        plot_scheme_correlation_heatmap(
            score_df[score_df["Skema"].isin(["Digital", "Konvensional"])],
            f"Heatmap Korelasi Antarvariabel - {score_choice}",
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        plot_scheme_difference_heatmap(
            score_df[score_df["Skema"].isin(["Digital", "Konvensional"])],
            f"Selisih Korelasi Digital vs Konvensional - {score_choice}",
        ),
        use_container_width=True,
    )

    st.markdown('<div class="section-title">Visualisasi Hubungan Variabel</div>', unsafe_allow_html=True)
    left, right = st.columns([1.05, 0.95])
    with left:
        st.plotly_chart(
            plot_scatter_matrix(
                score_df[score_df["Skema"].isin(["Digital", "Konvensional"])],
                f"Hubungan Antarvariabel - {score_choice}",
            ),
            use_container_width=True,
        )
    with right:
        st.plotly_chart(
            plot_scatter_with_trend(
                score_df[score_df["Skema"].isin(["Digital", "Konvensional"])],
                "Kesiapan Teknologi",
                "Partisipasi Pemuda",
                f"X1 terhadap Y - {score_choice}",
            ),
            use_container_width=True,
        )
        st.plotly_chart(
            plot_scatter_with_trend(
                score_df[score_df["Skema"].isin(["Digital", "Konvensional"])],
                "Kesiapan Pemuda",
                "Partisipasi Pemuda",
                f"X2 terhadap Y - {score_choice}",
            ),
            use_container_width=True,
        )

    st.markdown('<div class="section-title">Detail Regresi per Kelompok</div>', unsafe_allow_html=True)
    tabs = st.tabs(["Semua", "Digital", "Konvensional"])
    for tab, group_name in zip(tabs, ["Semua", "Digital", "Konvensional"]):
        with tab:
            group_df = filter_group(score_df, group_name)
            result = fit_ols(group_df, score_choice, group_name)
            render_result_detail(result, group_df)

    with st.expander("Tabel Gabungan Koefisien Semua Analisis"):
        if coef_summary.empty:
            st.info("Belum ada koefisien yang dapat ditampilkan.")
        else:
            coef_shown = coef_summary.copy()
            coef_shown["p-value"] = coef_shown["p-value"].map(p_value_label)
            st.dataframe(style_table(coef_shown), use_container_width=True, hide_index=True)

    csv_summary = summary.to_csv(index=False).encode("utf-8")
    csv_coef = coef_summary.to_csv(index=False).encode("utf-8") if not coef_summary.empty else b""
    dl1, dl2 = st.columns(2)
    dl1.download_button("Unduh Ringkasan Model (CSV)", csv_summary, "ringkasan_regresi_ganda.csv", "text/csv")
    dl2.download_button("Unduh Koefisien (CSV)", csv_coef, "koefisien_regresi_ganda.csv", "text/csv")


if __name__ == "__main__":
    main()
