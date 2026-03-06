"""
PCH Global — Queneto Dashboard
Explorador de exportaciones peruanas: filtros rápidos, tablas y gráficos.
"""
import sqlite3, os, io
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Configuración ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="PCH Global — Reporte Queneto",
    page_icon="🥑",
    layout="wide",
    initial_sidebar_state="expanded",
)

FOLDER  = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(FOLDER, "queneto_app.db")

# Modo de conexión: Azure SQL si están definidas las vars de entorno, SQLite si no
SQL_SERVER = os.environ.get("SQL_SERVER_HOST", "")
SQL_DB     = os.environ.get("SQL_DATABASE", "")
SQL_USER   = os.environ.get("SQL_USER", "")
SQL_PASS   = os.environ.get("SQL_PASSWORD", "")
USE_AZURE  = bool(SQL_SERVER and SQL_USER and SQL_PASS)

VERDE   = "#1B4332"
VERDE_C = "#52B788"
NARANJA = "#F4A261"

# ── CSS mínimo ─────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    .metric-box {
        background: #f0faf4;
        border-left: 4px solid #52B788;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 6px;
    }
    .metric-label { font-size: 12px; color: #666; font-weight: 600; text-transform: uppercase; }
    .metric-value { font-size: 24px; color: #1B4332; font-weight: 700; margin-top: 2px; }
    .metric-sub   { font-size: 11px; color: #999; }
    h1, h2, h3 { color: #1B4332 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background: #f0faf4; border-radius: 6px 6px 0 0;
        font-weight: 600; color: #2D6A4F;
    }
    .stTabs [aria-selected="true"] {
        background: #1B4332 !important; color: white !important;
    }
</style>
""", unsafe_allow_html=True)

# ── Carga de datos ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Cargando datos…")
def load_data():
    query = """
        SELECT anio_src, semana_src, mes, fecha_zarpe,
               producto, variedad, continente, pais_destino, ciudad_destino,
               puerto, puerto_destino, naviera, embarcador, consignatorio,
               fcl, peso_neto, fob_total, fob_kg, sector
        FROM reporte_pch
    """
    if USE_AZURE:
        import pymssql
        conn = pymssql.connect(server=SQL_SERVER, user=SQL_USER,
                               password=SQL_PASS, database=SQL_DB, tds_version='7.4')
    else:
        conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    df["anio_src"]   = df["anio_src"].astype("Int64")
    df["semana_src"] = df["semana_src"].astype("Int64")
    df["fob_total"]  = pd.to_numeric(df["fob_total"], errors="coerce")
    df["fob_kg"]     = pd.to_numeric(df["fob_kg"],    errors="coerce")
    df["peso_neto"]  = pd.to_numeric(df["peso_neto"], errors="coerce")
    return df

df_full = load_data()

# ── Sidebar — filtros ──────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(f"<h2 style='color:{VERDE}; margin-top:0'>PCH Global</h2>", unsafe_allow_html=True)
    st.caption("Reporte Queneto — Exportaciones Peruanas")
    st.divider()

    productos_disp = sorted(df_full["producto"].dropna().unique())
    sel_producto = st.multiselect("🍎 Producto", productos_disp,
                                  default=["PALTA FRESCO"])

    años_disp = sorted(df_full["anio_src"].dropna().unique())
    sel_año = st.multiselect("📅 Año", años_disp, default=list(años_disp))

    continentes_disp = sorted(df_full["continente"].dropna().unique())
    sel_continente = st.multiselect("🌍 Continente", continentes_disp, default=list(continentes_disp))

    st.divider()

    # Filtros secundarios (colapsables)
    with st.expander("Más filtros"):
        semanas_disp = sorted(df_full["semana_src"].dropna().unique())
        sem_min, sem_max = int(min(semanas_disp)), int(max(semanas_disp))
        sel_semana = st.slider("Semana", sem_min, sem_max, (sem_min, sem_max))

        meses_disp = sorted(df_full["mes"].dropna().unique())
        sel_mes = st.multiselect("Mes", meses_disp, default=list(meses_disp))

        paises_disp = sorted(df_full["pais_destino"].dropna().unique())
        sel_pais = st.multiselect("País destino", paises_disp, default=list(paises_disp))

        embs_disp = sorted(df_full["embarcador"].dropna().unique())
        sel_emb = st.multiselect("Embarcador", embs_disp, default=list(embs_disp))

    st.divider()
    if st.button("🔄 Limpiar caché", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Base: {len(df_full):,} registros totales")

# ── Aplicar filtros ────────────────────────────────────────────────────────────

mask = (
    (df_full["producto"].isin(sel_producto) if sel_producto else True) &
    (df_full["anio_src"].isin(sel_año) if sel_año else True) &
    (df_full["continente"].isin(sel_continente) if sel_continente else True) &
    (df_full["semana_src"].between(*sel_semana)) &
    (df_full["mes"].isin(sel_mes) if sel_mes else True) &
    (df_full["pais_destino"].isin(sel_pais) if sel_pais else True) &
    (df_full["embarcador"].isin(sel_emb) if sel_emb else True)
)
df = df_full[mask].copy()

# ── Header ─────────────────────────────────────────────────────────────────────

st.markdown(f"<h1 style='margin-bottom:0'>PCH Global — Reporte Queneto</h1>", unsafe_allow_html=True)
filtro_desc = " · ".join(sel_producto) if sel_producto else "Todos los productos"
años_desc   = " · ".join(str(a) for a in sorted(sel_año)) if sel_año else "Todos los años"
st.caption(f"{filtro_desc} | {años_desc} | {len(df):,} registros filtrados")
st.divider()

# ── KPIs ───────────────────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns(4)

total_cont    = len(df)
total_peso    = df["peso_neto"].sum() / 1_000_000  # millones de kg → ton / 1000
total_fob     = df["fob_total"].sum() / 1_000_000  # millones USD
avg_fob_kg    = df["fob_kg"].mean()

def kpi(col, label, value, sub=""):
    with col:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{sub}</div>
        </div>""", unsafe_allow_html=True)

kpi(col1, "Contenedores (FCL)", f"{total_cont:,}", "registros filtrados")
kpi(col2, "Peso Neto", f"{total_peso:,.1f} M kg", f"{total_peso*1000:,.0f} ton")
kpi(col3, "FOB Total", f"US$ {total_fob:.1f} M", "millones USD")
kpi(col4, "FOB / kg promedio", f"US$ {avg_fob_kg:.3f}" if pd.notna(avg_fob_kg) else "—", "USD por kg")

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["📊 Gráficos", "📋 Tabla de Datos", "📈 Análisis por Semana"])

# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — Gráficos
# ════════════════════════════════════════════════════════════════════════════════

with tab1:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        g1, g2 = st.columns(2)

        # Contenedores por semana y año
        with g1:
            st.subheader("Contenedores por Semana")
            df_sem = (df.groupby(["anio_src", "semana_src"])
                        .size().reset_index(name="contenedores"))
            df_sem["anio_src"] = df_sem["anio_src"].astype(str)
            fig = px.line(df_sem, x="semana_src", y="contenedores",
                         color="anio_src", markers=True,
                         labels={"semana_src": "Semana", "contenedores": "Contenedores",
                                 "anio_src": "Año"},
                         color_discrete_sequence=["#4E9AF1","#F4A261","#1ABC9C","#1B4332","#9B5DE5"])
            fig.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                             legend_title="Año", height=350)
            st.plotly_chart(fig, use_container_width=True)

        # FOB total por año
        with g2:
            st.subheader("FOB Total por Año (US$ M)")
            df_fob = (df.groupby("anio_src")["fob_total"]
                        .sum().reset_index())
            df_fob["fob_M"] = df_fob["fob_total"] / 1_000_000
            df_fob["anio_src"] = df_fob["anio_src"].astype(str)
            fig2 = px.bar(df_fob, x="anio_src", y="fob_M",
                         labels={"anio_src": "Año", "fob_M": "FOB (US$ M)"},
                         color="anio_src",
                         color_discrete_sequence=["#4E9AF1","#F4A261","#1ABC9C","#1B4332","#9B5DE5"],
                         text_auto=".1f")
            fig2.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                              showlegend=False, height=350)
            st.plotly_chart(fig2, use_container_width=True)

        g3, g4 = st.columns(2)

        # Top 10 países destino
        with g3:
            st.subheader("Top Países Destino")
            df_pais = (df.groupby("pais_destino").size()
                         .reset_index(name="contenedores")
                         .sort_values("contenedores", ascending=True)
                         .tail(15))
            fig3 = px.bar(df_pais, x="contenedores", y="pais_destino",
                         orientation="h",
                         labels={"pais_destino": "", "contenedores": "Contenedores"},
                         color_discrete_sequence=[VERDE_C])
            fig3.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=380)
            st.plotly_chart(fig3, use_container_width=True)

        # Top embarcadores
        with g4:
            st.subheader("Top Embarcadores")
            df_emb = (df.groupby("embarcador").size()
                        .reset_index(name="contenedores")
                        .sort_values("contenedores", ascending=True)
                        .tail(15))
            fig4 = px.bar(df_emb, x="contenedores", y="embarcador",
                         orientation="h",
                         labels={"embarcador": "", "contenedores": "Contenedores"},
                         color_discrete_sequence=[NARANJA])
            fig4.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=380)
            st.plotly_chart(fig4, use_container_width=True)

        # FOB/kg por semana
        st.subheader("FOB / kg promedio por Semana y Año")
        df_fob_sem = (df.groupby(["anio_src", "semana_src"])["fob_kg"]
                        .mean().reset_index())
        df_fob_sem["anio_src"] = df_fob_sem["anio_src"].astype(str)
        fig5 = px.line(df_fob_sem, x="semana_src", y="fob_kg",
                      color="anio_src", markers=True,
                      labels={"semana_src": "Semana", "fob_kg": "FOB/kg (USD)",
                              "anio_src": "Año"},
                      color_discrete_sequence=["#4E9AF1","#F4A261","#1ABC9C","#1B4332","#9B5DE5"])
        fig5.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                          legend_title="Año", height=320)
        st.plotly_chart(fig5, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — Tabla de Datos
# ════════════════════════════════════════════════════════════════════════════════

with tab2:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        # Opciones de agrupación
        col_a, col_b, col_c = st.columns([2, 2, 1])
        with col_a:
            agrupacion = st.selectbox("Agrupar por", [
                "Sin agrupar (filas individuales)",
                "Semana y Año",
                "Mes y Año",
                "Producto y Año",
                "País Destino",
                "Embarcador",
                "Naviera",
            ])
        with col_b:
            max_filas = st.selectbox("Filas a mostrar", [100, 500, 1000, 5000, "Todas"], index=0)

        if agrupacion == "Sin agrupar (filas individuales)":
            cols_show = ["anio_src","semana_src","mes","fecha_zarpe","producto","variedad",
                        "embarcador","pais_destino","naviera","fcl","peso_neto","fob_total","fob_kg"]
            df_show = df[cols_show].copy()
            df_show.columns = ["Año","Semana","Mes","Fecha Zarpe","Producto","Variedad",
                               "Embarcador","País Destino","Naviera","FCL",
                               "Peso Neto (kg)","FOB Total (USD)","FOB/kg"]
        else:
            group_map = {
                "Semana y Año":      ["anio_src","semana_src"],
                "Mes y Año":         ["anio_src","mes"],
                "Producto y Año":    ["anio_src","producto"],
                "País Destino":      ["pais_destino"],
                "Embarcador":        ["embarcador"],
                "Naviera":           ["naviera"],
            }
            gcols = group_map[agrupacion]
            df_show = (df.groupby(gcols, dropna=False)
                         .agg(
                             Contenedores=("fcl","count"),
                             Peso_Neto_kg=("peso_neto","sum"),
                             FOB_Total_USD=("fob_total","sum"),
                             FOB_kg_prom=("fob_kg","mean"),
                         )
                         .reset_index()
                         .sort_values("Contenedores", ascending=False))
            df_show.columns = [c.replace("_"," ") for c in df_show.columns]

        n_show = len(df_show) if max_filas == "Todas" else min(int(max_filas), len(df_show))
        st.caption(f"Mostrando {n_show:,} de {len(df_show):,} filas")
        st.dataframe(df_show.head(n_show), use_container_width=True, height=480)

        # Descarga Excel
        with col_c:
            st.write("")
            st.write("")
            buf_xl = io.BytesIO()
            df_show.head(n_show).to_excel(buf_xl, index=False, engine="openpyxl")
            buf_xl.seek(0)
            st.download_button(
                "⬇️ Excel",
                data=buf_xl,
                file_name="PCH_Queneto_Export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 — Análisis por Semana (tabla numérica W01-W52)
# ════════════════════════════════════════════════════════════════════════════════

with tab3:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        st.subheader("Contenedores por Semana y Año")
        st.caption("Tabla pivote W01-W52. Útil para comparar el mismo período entre años.")

        df_pivot = (df.groupby(["anio_src","semana_src"])
                      .size()
                      .reset_index(name="cont"))
        pivot = (df_pivot.pivot(index="semana_src", columns="anio_src", values="cont")
                         .reindex(range(1, 53))
                         .fillna(0)
                         .astype(int))
        pivot.index.name = "Semana"
        # Renombrar columnas a string para evitar error con background_gradient
        pivot.columns = [str(c) for c in pivot.columns]
        pivot["TOTAL"] = pivot.sum(axis=1)

        año_cols = [c for c in pivot.columns if c != "TOTAL"]
        styled = (pivot.style
                  .background_gradient(subset=año_cols, cmap="Greens", axis=None)
                  .background_gradient(subset=["TOTAL"], cmap="Blues", axis=None))
        st.dataframe(styled, use_container_width=True, height=600)

        # Descarga Excel de la pivote
        buf_piv = io.BytesIO()
        pivot.to_excel(buf_piv, engine="openpyxl")
        buf_piv.seek(0)
        st.download_button(
            "⬇️ Descargar tabla pivote Excel",
            data=buf_piv,
            file_name="PCH_Queneto_Semanas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.divider()
        st.subheader("FOB/kg promedio por Semana y Año")
        df_fob_p = (df.groupby(["anio_src","semana_src"])["fob_kg"]
                      .mean().reset_index())
        piv_fob = (df_fob_p.pivot(index="semana_src", columns="anio_src", values="fob_kg")
                            .reindex(range(1, 53))
                            .round(3))
        piv_fob.index.name = "Semana"
        # Renombrar columnas a string para evitar IndexingError
        piv_fob.columns = [str(c) for c in piv_fob.columns]
        styled_fob = (piv_fob.style
                      .background_gradient(cmap="RdYlGn", axis=None)
                      .format("{:.3f}", na_rep="—"))
        st.dataframe(styled_fob, use_container_width=True, height=600)
