"""
PCH Global — Queneto Dashboard
Explorador de exportaciones peruanas. Filtros aplicados en SQL (server-side).
"""
import os, io
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="PCH Global — Reporte Queneto",
    page_icon="🥑",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Credenciales (Streamlit Cloud secrets → env vars) ───────────────────────────

def _secret(key, default=""):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

SQL_SERVER = _secret("SQL_SERVER_HOST")
SQL_DB     = _secret("SQL_DATABASE")
SQL_USER   = _secret("SQL_USER")
SQL_PASS   = _secret("SQL_PASSWORD")

if not all([SQL_SERVER, SQL_USER, SQL_PASS]):
    st.error("⚠️ Credenciales Azure SQL no configuradas. Agrega SQL_SERVER_HOST, SQL_USER y SQL_PASSWORD en Secrets.")
    st.stop()

PH = "%s"  # pymssql usa %s

def _conn():
    import pymssql, time
    waits = [15, 30, 45, 60, 90]
    for attempt, wait in enumerate(waits, 1):
        try:
            return pymssql.connect(server=SQL_SERVER, user=SQL_USER,
                                   password=SQL_PASS, database=SQL_DB,
                                   tds_version="7.4", timeout=0, login_timeout=30)
        except Exception as e:
            if attempt == len(waits):
                raise
            code = getattr(e, "args", [None])[0]
            if code not in (40613, 40615, 42119):
                raise
            time.sleep(wait)
    raise RuntimeError("No se pudo conectar a Azure SQL")

# ── Estilos ───────────────────────────────────────────────────────────────────

VERDE   = "#1B4332"
VERDE_C = "#52B788"
NARANJA = "#F4A261"
COLORES = ["#4E9AF1", "#F4A261", "#1ABC9C", "#1B4332", "#9B5DE5", "#E63946"]

st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    .metric-box {
        background: #f0faf4; border-left: 4px solid #52B788;
        border-radius: 6px; padding: 12px 16px; margin-bottom: 6px;
    }
    .metric-label { font-size: 12px; color: #666; font-weight: 600; text-transform: uppercase; }
    .metric-value { font-size: 24px; color: #1B4332; font-weight: 700; margin-top: 2px; }
    .metric-sub   { font-size: 11px; color: #999; }
    h1, h2, h3 { color: #1B4332 !important; }

    @media print {
        section[data-testid="stSidebar"],
        header[data-testid="stHeader"],
        div[data-testid="stToolbar"],
        div[data-testid="stDecoration"],
        div[data-testid="stStatusWidget"],
        footer,
        .stButton,
        .stDownloadButton,
        button { display: none !important; }

        .block-container { padding: 0 !important; margin: 0 !important; max-width: 100% !important; }
        .main .block-container { padding-top: 0.5rem !important; }
        section.main { margin-left: 0 !important; }

        .metric-box { break-inside: avoid; }
        .stTabs [data-baseweb="tab-panel"] { break-inside: avoid; }

        @page { margin: 1.5cm; size: A4 landscape; }
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background: #f0faf4; border-radius: 6px 6px 0 0;
        font-weight: 600; color: #2D6A4F;
    }
    .stTabs [aria-selected="true"] { background: #1B4332 !important; color: white !important; }
</style>
""", unsafe_allow_html=True)

# ── Opciones para filtros (query ligera, valores distintos) ───────────────────

@st.cache_data(ttl=600, show_spinner=False)
def load_options():
    q = """
        SELECT DISTINCT anio_src, semana_src, mes, producto, variedad,
               continente, pais_destino, ciudad_destino, embarcador,
               naviera, transporte, sector, puerto, puerto_destino, consignatorio
        FROM reporte_pch
        WHERE anio_src IS NOT NULL
    """
    conn = _conn()
    df = pd.read_sql(q, conn)
    conn.close()
    return df

# ── Carga de datos filtrada en SQL ────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Cargando datos…")
def load_data(
    productos, años, continentes, sem_min, sem_max,
    meses, paises, embarcadores, navieras, variedades, transportes, sectores,
    puertos, puertos_dst, consignatarios
):
    conds, params = [], []

    def _add(col, vals):
        if vals:
            conds.append(f"{col} IN ({','.join([PH]*len(vals))})")
            params.extend(vals)

    _add("producto",       productos)
    _add("anio_src",       años)
    _add("continente",     continentes)
    _add("mes",            meses)
    _add("pais_destino",   paises)
    _add("embarcador",     embarcadores)
    _add("naviera",        navieras)
    _add("variedad",       variedades)
    _add("transporte",     transportes)
    _add("sector",         sectores)
    _add("puerto",         puertos)
    _add("puerto_destino", puertos_dst)
    _add("consignatorio",  consignatarios)

    conds.append(f"semana_src >= {PH}"); params.append(sem_min)
    conds.append(f"semana_src <= {PH}"); params.append(sem_max)

    where = " AND ".join(conds) if conds else "1=1"
    q = f"""
        SELECT anio_src, semana_src, mes, fecha_zarpe,
               producto, variedad, continente, pais_destino, ciudad_destino,
               puerto, puerto_destino, naviera, embarcador, consignatorio,
               transporte, sector, fcl, peso_neto, fob_total, fob_kg
        FROM reporte_pch
        WHERE {where}
        ORDER BY anio_src, semana_src
    """
    conn = _conn()
    df = pd.read_sql(q, conn, params=params if params else None)
    conn.close()
    for col in ("anio_src", "semana_src"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("fob_total", "fob_kg", "peso_neto"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ── Cargar opciones ───────────────────────────────────────────────────────────

opts = load_options()

def _opts(col):
    return sorted(opts[col].dropna().unique())

# ── Sidebar — filtros ─────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(f"<h2 style='color:{VERDE}; margin-top:0'>PCH Global</h2>", unsafe_allow_html=True)
    st.caption("Reporte Queneto — Exportaciones Peruanas")
    st.caption("🟢 Azure SQL")
    st.divider()

    # Filtros principales
    sel_producto  = st.multiselect("🍎 Producto",    _opts("producto"),   default=["PALTA FRESCO"])
    sel_año       = st.multiselect("📅 Año",         _opts("anio_src"),   default=list(_opts("anio_src")))
    sel_continente= st.multiselect("🌍 Continente",  _opts("continente"), default=list(_opts("continente")))

    st.divider()

    # Filtros secundarios
    with st.expander("📦 Producto / Variedad"):
        sel_variedad  = st.multiselect("Variedad",    _opts("variedad"),   default=[])
        sel_transporte= st.multiselect("Transporte",  _opts("transporte"), default=[])
        sel_sector    = st.multiselect("Sector",      _opts("sector"),     default=[])

    with st.expander("🗺️ Destino"):
        sel_pais      = st.multiselect("País destino",    _opts("pais_destino"),   default=[])
        sel_ciudad    = st.multiselect("Ciudad destino",  _opts("ciudad_destino"), default=[])
        sel_puerto_dst= st.multiselect("Puerto destino",  _opts("puerto_destino"), default=[])

    with st.expander("🚢 Transporte / Origen"):
        sel_naviera   = st.multiselect("Naviera",         _opts("naviera"),        default=[])
        sel_puerto    = st.multiselect("Puerto origen",   _opts("puerto"),         default=[])
        sel_consig    = st.multiselect("Consignatario",   _opts("consignatorio"),  default=[])
        sel_emb       = st.multiselect("Embarcador",      _opts("embarcador"),     default=[])

    with st.expander("📆 Período"):
        sems = sorted(opts["semana_src"].dropna().unique().astype(int))
        sel_semana = st.slider("Semana", int(min(sems)), int(max(sems)), (int(min(sems)), int(max(sems))))
        sel_mes    = st.multiselect("Mes", _opts("mes"), default=[])

    st.divider()
    if st.button("🔄 Limpiar caché", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Cargar datos con filtros ──────────────────────────────────────────────────

df = load_data(
    productos    = tuple(sel_producto),
    años         = tuple(int(a) for a in sel_año),
    continentes  = tuple(sel_continente),
    sem_min      = sel_semana[0],
    sem_max      = sel_semana[1],
    meses        = tuple(sel_mes),
    paises       = tuple(sel_pais),
    embarcadores = tuple(sel_emb),
    navieras     = tuple(sel_naviera),
    variedades   = tuple(sel_variedad),
    transportes  = tuple(sel_transporte),
    sectores     = tuple(sel_sector),
    puertos      = tuple(sel_puerto),
    puertos_dst  = tuple(sel_puerto_dst),
    consignatarios = tuple(sel_consig),
)

# ── Generador PDF ────────────────────────────────────────────────────────────

def generar_pdf(df, prod_desc, años_desc):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    from matplotlib.backends.backend_pdf import PdfPages

    buf = io.BytesIO()
    VERDE = "#1B4332"

    with PdfPages(buf) as pdf:

        # ── Página 1: KPIs + Contenedores por semana + FOB por año ──────────
        fig = plt.figure(figsize=(16, 10))
        fig.patch.set_facecolor("white")
        gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.5, wspace=0.35)

        # Header
        ax_h = fig.add_subplot(gs[0, :])
        ax_h.axis("off")
        ax_h.text(0, 0.85, "PCH Global — Reporte Queneto", fontsize=18, fontweight="bold", color=VERDE)
        ax_h.text(0, 0.5, f"Producto: {prod_desc}  |  Años: {años_desc}", fontsize=11, color="#444")
        ax_h.text(0, 0.15,
            f"Contenedores: {len(df):,}   |   "
            f"Peso Neto: {df['peso_neto'].sum()/1e6:,.1f} M kg   |   "
            f"FOB Total: US$ {df['fob_total'].sum()/1e6:.1f} M   |   "
            f"FOB/kg prom: US$ {df['fob_kg'].mean():.3f}   |   "
            f"Embarcadores: {df['embarcador'].nunique():,}",
            fontsize=9, color="#555")

        # Contenedores por semana
        ax1 = fig.add_subplot(gs[1, 0])
        df_sem = df.groupby(["anio_src", "semana_src"]).size().reset_index(name="cont")
        colores = ["#4E9AF1", "#F4A261", "#1ABC9C", "#1B4332", "#9B5DE5", "#E63946"]
        for i, (año, grp) in enumerate(df_sem.groupby("anio_src")):
            ax1.plot(grp["semana_src"], grp["cont"], marker="o", markersize=2,
                     label=str(año), color=colores[i % len(colores)], linewidth=1.5)
        ax1.set_title("Contenedores por Semana y Año", fontsize=10, fontweight="bold", color=VERDE)
        ax1.set_xlabel("Semana", fontsize=8); ax1.set_ylabel("Contenedores", fontsize=8)
        ax1.legend(fontsize=7, ncol=3); ax1.grid(True, alpha=0.3)
        ax1.tick_params(labelsize=7)

        # FOB por año
        ax2 = fig.add_subplot(gs[1, 1])
        df_fob = df.groupby("anio_src")["fob_total"].sum().reset_index()
        df_fob["fob_M"] = df_fob["fob_total"] / 1e6
        bars = ax2.bar([str(a) for a in df_fob["anio_src"]], df_fob["fob_M"],
                       color=colores[:len(df_fob)])
        for bar, val in zip(bars, df_fob["fob_M"]):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                     f"{val:.1f}", ha="center", va="bottom", fontsize=8)
        ax2.set_title("FOB Total por Año (US$ M)", fontsize=10, fontweight="bold", color=VERDE)
        ax2.set_ylabel("FOB (US$ M)", fontsize=8); ax2.tick_params(labelsize=8)
        ax2.grid(True, alpha=0.3, axis="y")

        # FOB/kg por semana
        ax3 = fig.add_subplot(gs[2, 0])
        df_fkg = df.groupby(["anio_src", "semana_src"])["fob_kg"].mean().reset_index()
        for i, (año, grp) in enumerate(df_fkg.groupby("anio_src")):
            ax3.plot(grp["semana_src"], grp["fob_kg"], marker="o", markersize=2,
                     label=str(año), color=colores[i % len(colores)], linewidth=1.5)
        ax3.set_title("FOB/kg promedio por Semana y Año", fontsize=10, fontweight="bold", color=VERDE)
        ax3.set_xlabel("Semana", fontsize=8); ax3.set_ylabel("USD/kg", fontsize=8)
        ax3.legend(fontsize=7, ncol=3); ax3.grid(True, alpha=0.3)
        ax3.tick_params(labelsize=7)

        # Top países
        ax4 = fig.add_subplot(gs[2, 1])
        df_p = df.groupby("pais_destino").size().reset_index(name="cont").sort_values("cont").tail(10)
        ax4.barh(df_p["pais_destino"], df_p["cont"], color="#52B788")
        ax4.set_title("Top Países Destino", fontsize=10, fontweight="bold", color=VERDE)
        ax4.tick_params(labelsize=7); ax4.grid(True, alpha=0.3, axis="x")

        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # ── Página 2: Top embarcadores + tabla semana pivote ─────────────────
        fig2 = plt.figure(figsize=(16, 11))
        fig2.patch.set_facecolor("white")
        gs2 = gridspec.GridSpec(2, 2, figure=fig2, hspace=0.5, wspace=0.35)

        # Top embarcadores por contenedores
        ax5 = fig2.add_subplot(gs2[0, 0])
        df_emb = df.groupby("embarcador").size().reset_index(name="cont").sort_values("cont").tail(15)
        ax5.barh(df_emb["embarcador"], df_emb["cont"], color="#F4A261")
        ax5.set_title("Top Embarcadores", fontsize=10, fontweight="bold", color=VERDE)
        ax5.tick_params(labelsize=6); ax5.grid(True, alpha=0.3, axis="x")

        # Top embarcadores por FOB
        ax6 = fig2.add_subplot(gs2[0, 1])
        df_emb_fob = (df.groupby("embarcador")["fob_total"].sum().reset_index()
                        .sort_values("fob_total").tail(15))
        df_emb_fob["fob_M"] = df_emb_fob["fob_total"] / 1e6
        ax6.barh(df_emb_fob["embarcador"], df_emb_fob["fob_M"], color="#1B4332")
        ax6.set_title("Top Embarcadores por FOB (US$ M)", fontsize=10, fontweight="bold", color=VERDE)
        ax6.tick_params(labelsize=6); ax6.grid(True, alpha=0.3, axis="x")

        # Tabla pivote contenedores W01-W52
        ax7 = fig2.add_subplot(gs2[1, :])
        ax7.axis("off")
        ax7.set_title("Contenedores por Semana y Año (W01-W52)", fontsize=10,
                      fontweight="bold", color=VERDE, pad=10)
        df_piv = df.groupby(["anio_src", "semana_src"]).size().reset_index(name="cont")
        pivot = (df_piv.pivot(index="semana_src", columns="anio_src", values="cont")
                       .reindex(range(1, 53)).fillna(0).astype(int))
        pivot["TOTAL"] = pivot.sum(axis=1)
        años_cols = [str(c) for c in pivot.columns]
        pivot.columns = años_cols
        # Mostrar solo semanas con datos
        pivot_datos = pivot[pivot["TOTAL"] > 0]
        cols_tabla = list(pivot.columns)
        tabla_data = [[str(idx)] + [str(pivot_datos.loc[idx, c]) if c in pivot_datos.columns else "0"
                       for c in cols_tabla] for idx in pivot_datos.index]
        tabla = ax7.table(
            cellText=tabla_data,
            colLabels=["Sem"] + cols_tabla,
            cellLoc="center", loc="center",
            bbox=[0, 0, 1, 0.95]
        )
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(6)
        for (r, c), cell in tabla.get_celld().items():
            if r == 0:
                cell.set_facecolor("#1B4332")
                cell.set_text_props(color="white", fontweight="bold")
            elif r % 2 == 0:
                cell.set_facecolor("#f0faf4")

        pdf.savefig(fig2, bbox_inches="tight")
        plt.close(fig2)

    buf.seek(0)
    return buf.getvalue()

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("<h1 style='margin-bottom:0'>PCH Global — Reporte Queneto</h1>", unsafe_allow_html=True)
prod_desc = " · ".join(sel_producto) if sel_producto else "Todos"
años_desc = " · ".join(str(a) for a in sorted(sel_año)) if sel_año else "Todos"
st.caption(f"{prod_desc} | {años_desc} | {len(df):,} registros")
st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)

def kpi(col, label, value, sub=""):
    with col:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{sub}</div>
        </div>""", unsafe_allow_html=True)

kpi(c1, "Contenedores (FCL)", f"{len(df):,}", "registros")
kpi(c2, "Peso Neto", f"{df['peso_neto'].sum()/1e6:,.1f} M kg", f"{df['peso_neto'].sum()/1e3:,.0f} ton")
kpi(c3, "FOB Total", f"US$ {df['fob_total'].sum()/1e6:.1f} M", "millones USD")
kpi(c4, "FOB / kg prom.", f"US$ {df['fob_kg'].mean():.3f}" if len(df) else "—", "USD por kg")
kpi(c5, "Embarcadores", f"{df['embarcador'].nunique():,}", "distintos")

# ── Botón descargar PDF ────────────────────────────────────────────────────────
_col_pdf, _ = st.columns([1, 4])
with _col_pdf:
    if not df.empty:
        pdf_bytes = generar_pdf(df, prod_desc, años_desc)
        st.download_button(
            label="📄 Descargar Reporte PDF",
            data=pdf_bytes,
            file_name="PCH_Queneto_Reporte.pdf",
            mime="application/pdf",
            use_container_width=True,
        )

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(["📊 Gráficos", "📋 Tabla de Datos", "📈 Análisis Semanal", "🏢 Embarcadores"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Gráficos
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        g1, g2 = st.columns(2)

        with g1:
            st.subheader("Contenedores por Semana y Año")
            df_sem = df.groupby(["anio_src","semana_src"]).size().reset_index(name="cont")
            df_sem["anio_src"] = df_sem["anio_src"].astype(str)
            fig = px.line(df_sem, x="semana_src", y="cont", color="anio_src", markers=True,
                          labels={"semana_src":"Semana","cont":"Contenedores","anio_src":"Año"},
                          color_discrete_sequence=COLORES)
            fig.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=350)
            st.plotly_chart(fig, use_container_width=True)

        with g2:
            st.subheader("FOB Total por Año (US$ M)")
            df_fob = df.groupby("anio_src")["fob_total"].sum().reset_index()
            df_fob["fob_M"] = df_fob["fob_total"] / 1e6
            df_fob["anio_src"] = df_fob["anio_src"].astype(str)
            fig2 = px.bar(df_fob, x="anio_src", y="fob_M",
                          labels={"anio_src":"Año","fob_M":"FOB (US$ M)"},
                          color="anio_src", color_discrete_sequence=COLORES, text_auto=".1f")
            fig2.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                               showlegend=False, height=350)
            st.plotly_chart(fig2, use_container_width=True)

        g3, g4 = st.columns(2)

        with g3:
            st.subheader("Top Países Destino")
            df_p = (df.groupby("pais_destino").size().reset_index(name="cont")
                      .sort_values("cont", ascending=True).tail(15))
            fig3 = px.bar(df_p, x="cont", y="pais_destino", orientation="h",
                          labels={"pais_destino":"","cont":"Contenedores"},
                          color_discrete_sequence=[VERDE_C])
            fig3.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=400)
            st.plotly_chart(fig3, use_container_width=True)

        with g4:
            st.subheader("Top Navieras")
            df_nav = (df.groupby("naviera").size().reset_index(name="cont")
                        .sort_values("cont", ascending=True).tail(15))
            fig4 = px.bar(df_nav, x="cont", y="naviera", orientation="h",
                          labels={"naviera":"","cont":"Contenedores"},
                          color_discrete_sequence=[NARANJA])
            fig4.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=400)
            st.plotly_chart(fig4, use_container_width=True)

        st.subheader("FOB/kg promedio por Semana y Año")
        df_fkg = df.groupby(["anio_src","semana_src"])["fob_kg"].mean().reset_index()
        df_fkg["anio_src"] = df_fkg["anio_src"].astype(str)
        fig5 = px.line(df_fkg, x="semana_src", y="fob_kg", color="anio_src", markers=True,
                       labels={"semana_src":"Semana","fob_kg":"FOB/kg (USD)","anio_src":"Año"},
                       color_discrete_sequence=COLORES)
        fig5.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=320)
        st.plotly_chart(fig5, use_container_width=True)

        g5, g6 = st.columns(2)
        with g5:
            st.subheader("Distribución por Producto")
            df_prod = df.groupby("producto").size().reset_index(name="cont")
            fig6 = px.pie(df_prod, names="producto", values="cont",
                          color_discrete_sequence=COLORES, hole=0.35)
            fig6.update_layout(height=320)
            st.plotly_chart(fig6, use_container_width=True)

        with g6:
            st.subheader("Contenedores por Puerto Origen")
            df_pto = (df.groupby("puerto").size().reset_index(name="cont")
                        .sort_values("cont", ascending=True).tail(10))
            fig7 = px.bar(df_pto, x="cont", y="puerto", orientation="h",
                          labels={"puerto":"","cont":"Contenedores"},
                          color_discrete_sequence=["#9B5DE5"])
            fig7.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=320)
            st.plotly_chart(fig7, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Tabla de Datos
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        ca, cb, cc = st.columns([2, 2, 1])
        with ca:
            agrupacion = st.selectbox("Agrupar por", [
                "Sin agrupar",
                "Semana y Año", "Mes y Año", "Producto y Año",
                "País Destino", "Ciudad Destino", "Puerto Destino",
                "Embarcador", "Naviera", "Consignatario",
                "Transporte", "Sector", "Variedad",
            ])
        with cb:
            max_filas = st.selectbox("Filas a mostrar", [100, 500, 1000, 5000, "Todas"], index=0)

        if agrupacion == "Sin agrupar":
            cols = ["anio_src","semana_src","mes","fecha_zarpe","producto","variedad",
                    "transporte","sector","embarcador","consignatorio","naviera",
                    "puerto","puerto_destino","pais_destino","ciudad_destino",
                    "fcl","peso_neto","fob_total","fob_kg"]
            df_show = df[cols].copy()
            df_show.columns = ["Año","Semana","Mes","Fecha Zarpe","Producto","Variedad",
                                "Transporte","Sector","Embarcador","Consignatario","Naviera",
                                "Puerto Origen","Puerto Destino","País Destino","Ciudad Destino",
                                "FCL","Peso Neto (kg)","FOB Total (USD)","FOB/kg"]
        else:
            gmap = {
                "Semana y Año":     ["anio_src","semana_src"],
                "Mes y Año":        ["anio_src","mes"],
                "Producto y Año":   ["anio_src","producto"],
                "País Destino":     ["pais_destino"],
                "Ciudad Destino":   ["ciudad_destino"],
                "Puerto Destino":   ["puerto_destino"],
                "Embarcador":       ["embarcador"],
                "Naviera":          ["naviera"],
                "Consignatario":    ["consignatorio"],
                "Transporte":       ["transporte"],
                "Sector":           ["sector"],
                "Variedad":         ["variedad"],
            }
            gcols = gmap[agrupacion]
            df_show = (df.groupby(gcols, dropna=False)
                         .agg(Contenedores=("fcl","count"),
                              Peso_Neto_kg=("peso_neto","sum"),
                              FOB_Total_USD=("fob_total","sum"),
                              FOB_kg_prom=("fob_kg","mean"))
                         .reset_index()
                         .sort_values("Contenedores", ascending=False))
            df_show.columns = [c.replace("_"," ") for c in df_show.columns]

        n = len(df_show) if max_filas == "Todas" else min(int(max_filas), len(df_show))
        st.caption(f"Mostrando {n:,} de {len(df_show):,} filas")
        st.dataframe(df_show.head(n), use_container_width=True, height=480)

        with cc:
            st.write(""); st.write("")
            buf = io.BytesIO()
            df_show.head(n).to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button("⬇️ Excel", data=buf, file_name="PCH_Queneto.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Análisis Semanal
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        st.subheader("Contenedores por Semana y Año")
        df_piv = df.groupby(["anio_src","semana_src"]).size().reset_index(name="cont")
        pivot = (df_piv.pivot(index="semana_src", columns="anio_src", values="cont")
                        .reindex(range(1,53)).fillna(0).astype(int))
        pivot.index.name = "Semana"
        pivot.columns = [str(c) for c in pivot.columns]
        pivot["TOTAL"] = pivot.sum(axis=1)
        año_cols = [c for c in pivot.columns if c != "TOTAL"]
        styled = (pivot.style
                  .background_gradient(subset=año_cols, cmap="Greens", axis=None)
                  .background_gradient(subset=["TOTAL"], cmap="Blues", axis=None))
        st.dataframe(styled, use_container_width=True, height=600)

        buf_p = io.BytesIO(); pivot.to_excel(buf_p, engine="openpyxl"); buf_p.seek(0)
        st.download_button("⬇️ Tabla Contenedores Excel", data=buf_p,
                           file_name="PCH_Contenedores_Semana.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.divider()
        st.subheader("FOB/kg promedio por Semana y Año")
        df_fob_p = df.groupby(["anio_src","semana_src"])["fob_kg"].mean().reset_index()
        piv_fob = (df_fob_p.pivot(index="semana_src", columns="anio_src", values="fob_kg")
                            .reindex(range(1,53)).round(3))
        piv_fob.index.name = "Semana"
        piv_fob.columns = [str(c) for c in piv_fob.columns]
        styled_f = piv_fob.style.background_gradient(cmap="RdYlGn", axis=None).format("{:.3f}", na_rep="—")
        st.dataframe(styled_f, use_container_width=True, height=600)

        buf_f = io.BytesIO(); piv_fob.to_excel(buf_f, engine="openpyxl"); buf_f.seek(0)
        st.download_button("⬇️ Tabla FOB/kg Excel", data=buf_f,
                           file_name="PCH_FOBkg_Semana.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Embarcadores
# ══════════════════════════════════════════════════════════════════════════════

with tab4:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        st.subheader("Ranking de Embarcadores")

        df_emb = (df.groupby("embarcador")
                    .agg(Contenedores=("fcl","count"),
                         Peso_kg=("peso_neto","sum"),
                         FOB_USD=("fob_total","sum"),
                         FOB_kg_prom=("fob_kg","mean"))
                    .reset_index()
                    .sort_values("Contenedores", ascending=False))
        df_emb["FOB_M"] = df_emb["FOB_USD"] / 1e6
        df_emb["Peso_M_kg"] = df_emb["Peso_kg"] / 1e6
        df_emb["Market Share %"] = (df_emb["Contenedores"] / df_emb["Contenedores"].sum() * 100).round(2)

        top_n = st.slider("Top N embarcadores", 5, 30, 15)

        g1, g2 = st.columns(2)
        with g1:
            fig = px.bar(df_emb.head(top_n).sort_values("Contenedores"),
                         x="Contenedores", y="embarcador", orientation="h",
                         color="Contenedores", color_continuous_scale="Greens",
                         labels={"embarcador":""})
            fig.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                              height=500, showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        with g2:
            fig2 = px.bar(df_emb.head(top_n).sort_values("FOB_M"),
                          x="FOB_M", y="embarcador", orientation="h",
                          color="FOB_M", color_continuous_scale="Oranges",
                          labels={"embarcador":"", "FOB_M":"FOB (US$ M)"})
            fig2.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                               height=500, showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig2, use_container_width=True)

        # Evolución de top 5 embarcadores por año
        top5 = df_emb.head(5)["embarcador"].tolist()
        df_top5 = (df[df["embarcador"].isin(top5)]
                     .groupby(["anio_src","embarcador"]).size().reset_index(name="cont"))
        df_top5["anio_src"] = df_top5["anio_src"].astype(str)
        fig3 = px.line(df_top5, x="anio_src", y="cont", color="embarcador",
                       markers=True, labels={"anio_src":"Año","cont":"Contenedores"},
                       color_discrete_sequence=COLORES)
        fig3.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                           height=320, title="Evolución Top 5 Embarcadores por Año")
        st.plotly_chart(fig3, use_container_width=True)

        # Tabla resumen
        st.dataframe(
            df_emb[["embarcador","Contenedores","Market Share %","FOB_M","FOB_kg_prom","Peso_M_kg"]]
            .rename(columns={"embarcador":"Embarcador","FOB_M":"FOB (US$ M)",
                             "FOB_kg_prom":"FOB/kg prom","Peso_M_kg":"Peso (M kg)"})
            .head(50),
            use_container_width=True, height=400
        )
