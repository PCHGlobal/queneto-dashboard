import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from auth_check import require_auth
require_auth()

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

# ── Credenciales (Streamlit Cloud secrets → env vars → SQLite) ────────────────

def _secret(key, default=""):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

SQL_SERVER = _secret("SQL_SERVER_HOST")
SQL_DB     = _secret("SQL_DATABASE")
SQL_USER   = _secret("SQL_USER")
SQL_PASS   = _secret("SQL_PASSWORD")
USE_AZURE  = bool(SQL_SERVER and SQL_USER and SQL_PASS)
USE_LOCAL  = USE_AZURE  # alias semántico
DB_PATH    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queneto_app.db")

PH = "%s" if USE_AZURE else "?"   # placeholder paramétrico

def _conn():
    if USE_AZURE:
        import pymssql, time
        for attempt in range(5):
            try:
                return pymssql.connect(server=SQL_SERVER, user=SQL_USER,
                                       password=SQL_PASS, database=SQL_DB,
                                       tds_version="7.4", timeout=0, login_timeout=30)
            except Exception:
                if attempt < 4:
                    time.sleep(8)
                else:
                    raise
    import sqlite3
    return sqlite3.connect(DB_PATH)

# ── Estilos ───────────────────────────────────────────────────────────────────

VERDE   = "#1B4332"
VERDE_C = "#52B788"
NARANJA = "#F4A261"
COLORES = ["#4E9AF1", "#F4A261", "#1ABC9C", "#FF6B6B", "#9B5DE5", "#E63946"]

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
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background: #f0faf4; border-radius: 6px 6px 0 0;
        font-weight: 600; color: #2D6A4F;
    }
    .stTabs [aria-selected="true"] { background: #1B4332 !important; color: white !important; }
@media print {
    [data-testid="stSidebar"], [data-testid="stSidebarNav"],
    [data-testid="collapsedControl"], section[data-testid="stSidebar"] { display: none !important; }
    .main .block-container { margin-left: 0 !important; max-width: 100% !important; }
    .print-table { width: 100%; border-collapse: collapse; font-size: 9pt; }
    .print-table th { background: #1B4332 !important; color: white !important; padding: 4px 6px; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
    .print-table td { padding: 3px 6px; border-bottom: 1px solid #ddd; }
    .print-table tr:nth-child(even) td { background: #f5f5f5 !important; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
}
.print-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.print-table th { background: #1B4332; color: white; padding: 6px 8px; text-align: left; }
.print-table td { padding: 4px 8px; border-bottom: 1px solid #e0e0e0; }
.print-table tr:nth-child(even) td { background: #f5faf7; }
.print-table tr:hover td { background: #e8f5ee; }
</style>
""", unsafe_allow_html=True)

# ── Opciones para filtros (tabla lookup pre-calculada) ────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_options():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT columna, valor FROM reporte_pch_opciones ORDER BY columna, valor")
    rows = cur.fetchall()
    conn.close()
    from collections import defaultdict
    data = defaultdict(list)
    for col, val in rows:
        data[col].append(val)
    # Construir DataFrame compatible con el resto del código
    max_len = max(len(v) for v in data.values())
    padded = {k: v + [None] * (max_len - len(v)) for k, v in data.items()}
    return pd.DataFrame(padded)

# ── Opciones filtradas en cascada ─────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_cascade_options(productos, años, continentes, paises=()):
    """Devuelve opciones de destino/transporte filtradas en cascada."""
    conn = _conn()
    cur = conn.cursor()
    PH = "%s" if USE_AZURE else "?"
    conds, params = [], []
    if productos:
        conds.append(f"producto IN ({','.join([PH]*len(productos))})")
        params.extend(productos)
    if años:
        conds.append(f"anio_src IN ({','.join([PH]*len(años))})")
        params.extend(años)
    if continentes:
        conds.append(f"continente IN ({','.join([PH]*len(continentes))})")
        params.extend(continentes)
    if paises:
        conds.append(f"pais_destino IN ({','.join([PH]*len(paises))})")
        params.extend(paises)
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    cur.execute(f"""
        SELECT DISTINCT pais_destino, ciudad_destino, puerto_destino,
                        naviera, embarcador, consignatorio, puerto, variedad
        FROM reporte_pch {where}
    """, params)
    rows = cur.fetchall()
    conn.close()
    cols = ["pais_destino","ciudad_destino","puerto_destino","naviera","embarcador","consignatorio","puerto","variedad"]
    df_cas = pd.DataFrame(rows, columns=cols)
    return {c: sorted(df_cas[c].dropna().unique()) for c in cols}

# ── CIRAD prices loader ───────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_cirad_prices(años=()):
    conn = _conn()
    cur = conn.cursor()
    PH_c = "%s" if USE_AZURE else "?"
    if años:
        q = f"SELECT semana, anio, avg_fot FROM cirad_weekly_prices WHERE anio IN ({','.join([PH_c]*len(años))}) ORDER BY anio, semana"
        cur.execute(q, tuple(años))
    else:
        cur.execute("SELECT semana, anio, avg_fot FROM cirad_weekly_prices ORDER BY anio, semana")
    cols = [d[0] for d in cur.description]
    df_c = pd.DataFrame(cur.fetchall(), columns=cols)
    conn.close()
    df_c["avg_fot"] = pd.to_numeric(df_c["avg_fot"], errors="coerce")
    return df_c

# ── Carga de datos filtrada en SQL ────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Cargando datos…")
def load_data(
    productos, años, continentes, sem_min, sem_max,
    meses, paises, embarcadores, navieras, variedades, transportes, sectores,
    puertos, puertos_dst, consignatarios, semana_col="semana_zarpe"
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

    conds.append(f"({semana_col} >= {PH} AND {semana_col} <= {PH} OR {semana_col} IS NULL)")
    params.append(sem_min); params.append(sem_max)

    where = " AND ".join(conds) if conds else "1=1"
    q = f"""
        SELECT anio_src, {semana_col} AS semana_src, mes, fecha_zarpe,
               producto, variedad, continente, pais_destino, ciudad_destino,
               puerto, puerto_destino, naviera, embarcador, consignatorio,
               transporte, sector, fcl, peso_neto, fob_total, fob_kg,
               COALESCE(cantidad, 1) AS cantidad
        FROM reporte_pch
        WHERE {where}
        ORDER BY anio_src, {semana_col}
    """
    conn = _conn()
    cur = conn.cursor()
    cur.execute(q, tuple(params) if params else ())
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    conn.close()
    for col in ("anio_src", "semana_src"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("fob_total", "fob_kg", "peso_neto", "cantidad"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["cantidad"] = df["cantidad"].fillna(1)
    return df

# ── Cargar opciones ───────────────────────────────────────────────────────────

opts = load_options()

def _opts(col):
    return sorted(opts[col].dropna().unique())

# ── Sidebar — filtros ─────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(f"<h2 style='color:{VERDE}; margin-top:0'>PCH Global</h2>", unsafe_allow_html=True)
    st.caption("Reporte Queneto — Exportaciones Peruanas")
    st.markdown("""<a href="/cirad" target="_self" style="display:block;text-align:center;padding:8px;background:#1B4332;color:white;border-radius:6px;text-decoration:none;font-weight:600;font-size:0.9rem">🥑 Reporte CIRAD</a>""", unsafe_allow_html=True)
    st.divider()
    if USE_AZURE:
        st.caption("🟢 SQL Server")
    else:
        st.caption("🟡 SQLite local")
    st.divider()

    # Filtros principales
    _anos_disponibles = _opts("anio_src")
    _ano_default = [max(_anos_disponibles)] if _anos_disponibles else []

    with st.expander("📆 Período"):
        sel_semana_tipo = st.radio("Semana de", ["ETD (salida Perú)", "ETA (llegada destino)"], horizontal=True)
        _sem_col = "semana_zarpe" if "ETD" in sel_semana_tipo else "semana_eta"
        sems = sorted(opts[_sem_col].dropna().unique().astype(int))
        sel_semana = st.slider("Semana", int(min(sems)), int(max(sems)), (int(min(sems)), int(max(sems))))
        _mes_orden = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
        sel_mes    = st.multiselect("Mes", sorted(_opts("mes"), key=lambda x: _mes_orden.index(x) if x in _mes_orden else 99), default=[])

    sel_producto  = st.multiselect("🍎 Producto",    _opts("producto"),   default=["PALTA FRESCO"])
    sel_año       = st.multiselect("📅 Año",         _anos_disponibles,   default=_ano_default)
    sel_continente= st.multiselect("🌍 Continente",  _opts("continente"), default=[])

    # Opciones secundarias filtradas en cascada según primarios
    _cas = load_cascade_options(
        tuple(sel_producto), tuple(sel_año), tuple(sel_continente)
    )

    st.divider()

    # Filtros secundarios
    with st.expander("📦 Producto / Variedad"):
        sel_variedad  = st.multiselect("Variedad",    _cas["variedad"],    default=[])
        sel_transporte= st.multiselect("Transporte",  _opts("transporte"), default=[])
        sel_sector    = st.multiselect("Sector",      _opts("sector"),     default=[])

    with st.expander("🗺️ Destino"):
        sel_pais      = st.multiselect("País destino",    _cas["pais_destino"],   default=[])
        # Re-filtrar ciudades y puertos según el país seleccionado
        _cas2 = load_cascade_options(
            tuple(sel_producto), tuple(sel_año), tuple(sel_continente), tuple(sel_pais)
        )
        sel_ciudad    = st.multiselect("Ciudad destino",  _cas2["ciudad_destino"], default=[])

    with st.expander("🚢 Transporte / Origen"):
        sel_naviera   = st.multiselect("Naviera",         _cas["naviera"],        default=[])
        sel_puerto    = st.multiselect("Puerto origen",   _cas["puerto"],         default=[])
        sel_consig    = st.multiselect("Consignatario",   _cas["consignatorio"],  default=[])
        sel_emb       = st.multiselect("Embarcador",      _cas["embarcador"],     default=[])

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
    puertos_dst  = (),
    consignatarios = tuple(sel_consig),
    semana_col   = _sem_col,
)

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

kpi(c1, "Contenedores (FCL)", f"{df['cantidad'].sum():,.0f}", f"{len(df):,} registros")
kpi(c2, "Peso Neto", f"{df['peso_neto'].sum()/1e6:,.1f} M kg", f"{df['peso_neto'].sum()/1e3:,.0f} ton")
kpi(c3, "FOB Total", f"US$ {df['fob_total'].sum()/1e6:.1f} M", "millones USD")
kpi(c4, "FOB / kg prom.", f"US$ {df['fob_kg'].mean():.3f}" if len(df) else "—", "USD por kg")
kpi(c5, "Embarcadores", f"{df['embarcador'].nunique():,}", "distintos")

st.divider()

# ── Botón imprimir ────────────────────────────────────────────────────────────
st.components.v1.html("""
<button onclick="window.parent.print()" style="
    background:#1B4332; color:white; border:none; border-radius:6px;
    padding:8px 20px; font-size:14px; font-weight:600; cursor:pointer;">
    🖨️ Imprimir / Guardar PDF
</button>
""", height=50)

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Gráficos", "📋 Tabla de Datos", "📈 Análisis Semanal", "🏢 Embarcadores", "🏭 Productores"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Gráficos
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        _multi_prod = len(sel_producto) > 1
        _titulo_sem = "Contenedores por Semana y Producto" if _multi_prod else "Contenedores por Semana y Año"
        st.subheader(_titulo_sem)
        if _multi_prod:
            df_sem = df.groupby(["producto","anio_src","semana_src"])["cantidad"].sum().reset_index(name="cont")
            df_sem["cont"] = df_sem["cont"].round().astype(int)
            df_sem["serie"] = df_sem["producto"].str.split().str[0] + " " + df_sem["anio_src"].astype(str)
            fig = px.line(df_sem, x="semana_src", y="cont",
                          color="serie", markers=True, text="cont",
                          labels={"semana_src":"Semana","cont":"Contenedores","serie":""},
                          color_discrete_sequence=COLORES)
        else:
            df_sem = df.groupby(["anio_src","semana_src"])["cantidad"].sum().reset_index(name="cont")
            df_sem["cont"] = df_sem["cont"].round().astype(int)
            df_sem["anio_src"] = df_sem["anio_src"].astype(str)
            fig = px.line(df_sem, x="semana_src", y="cont", color="anio_src", markers=True,
                          text="cont",
                          labels={"semana_src":"Semana","cont":"Contenedores","anio_src":"Año"},
                          color_discrete_sequence=COLORES)
        fig.update_traces(textposition="top center", textfont_size=9,
                          mode="lines+markers+text")
        _y_max = df_sem["cont"].max() * 1.2
        fig.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=420,
                          yaxis=dict(range=[0, _y_max]))

        # CIRAD overlay: si producto es palta (ETD o ETA)
        _show_cirad = (
            bool(sel_producto)
            and all("palta" in p.lower() for p in sel_producto)
        )
        if _show_cirad:
            _cirad_años = tuple(sorted(int(a) for a in sel_año)) if sel_año else ()
            df_cirad = load_cirad_prices(_cirad_años)
            if not df_cirad.empty:
                _cirad_colors = ["#E63946", "#9B5DE5", "#F4A261", "#1ABC9C", "#FF6B6B"]
                for _i, _anio_c in enumerate(sorted(df_cirad["anio"].unique())):
                    _dc = df_cirad[df_cirad["anio"] == _anio_c].sort_values("semana")
                    fig.add_trace(go.Scatter(
                        x=_dc["semana"], y=_dc["avg_fot"],
                        name=f"CIRAD {_anio_c} (€/caja)",
                        mode="lines+markers+text",
                        text=_dc["avg_fot"].apply(lambda v: f"{v:.2f}"),
                        textposition="top center",
                        textfont=dict(size=8),
                        yaxis="y2",
                        line=dict(dash="dash", width=2,
                                  color=_cirad_colors[_i % len(_cirad_colors)]),
                        marker=dict(size=5),
                    ))
                fig.update_layout(
                    yaxis2=dict(
                        title="Precio CIRAD (€/caja)",
                        overlaying="y",
                        side="right",
                        showgrid=False,
                        tickformat=".2f",
                    )
                )
        st.plotly_chart(fig, use_container_width=True)
        _sem_label = "ETD (fecha de zarpe — salida de Perú)" if "ETD" in sel_semana_tipo else "ETA (fecha estimada de arribo — llegada a destino)"
        st.caption(f"📌 Semana por **{_sem_label}** · Contenedores = unidades físicas (un contenedor compartido entre variedades cuenta como 1)")

        g3, g4 = st.columns(2)

        with g3:
            st.subheader("Top Países Destino")
            df_p = (df.groupby("pais_destino")["cantidad"].sum().reset_index(name="cont")
                      .assign(cont=lambda x: x["cont"].round().astype(int))
                      .sort_values("cont", ascending=True).tail(15))
            fig3 = px.bar(df_p, x="cont", y="pais_destino", orientation="h",
                          labels={"pais_destino":"","cont":"Contenedores"},
                          text_auto=".0f",
                          color_discrete_sequence=[VERDE_C])
            fig3.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=400)
            st.plotly_chart(fig3, use_container_width=True)

        with g4:
            st.subheader("Top Navieras")
            df_nav = (df.groupby("naviera")["cantidad"].sum().reset_index(name="cont")
                        .assign(cont=lambda x: x["cont"].round().astype(int))
                        .sort_values("cont", ascending=True).tail(15))
            fig4 = px.bar(df_nav, x="cont", y="naviera", orientation="h",
                          labels={"naviera":"","cont":"Contenedores"},
                          text_auto=".0f",
                          color_discrete_sequence=[NARANJA])
            fig4.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=400)
            st.plotly_chart(fig4, use_container_width=True)

        _titulo_fkg = "FOB/kg promedio por Semana y Producto" if _multi_prod else "FOB/kg promedio por Semana y Año"
        st.subheader(_titulo_fkg)
        if _multi_prod:
            df_fkg = df.groupby(["producto","anio_src","semana_src"])["fob_kg"].mean().reset_index()
            df_fkg["serie"] = df_fkg["producto"].str.split().str[0] + " " + df_fkg["anio_src"].astype(str)
            df_fkg["texto"] = df_fkg["fob_kg"].round(2).astype(str)
            fig5 = px.line(df_fkg, x="semana_src", y="fob_kg",
                           color="serie", markers=True, text="texto",
                           labels={"semana_src":"Semana","fob_kg":"FOB/kg (USD)","serie":""},
                           color_discrete_sequence=COLORES)
        else:
            df_fkg = df.groupby(["anio_src","semana_src"])["fob_kg"].mean().reset_index()
            df_fkg["anio_src"] = df_fkg["anio_src"].astype(str)
            df_fkg["texto"] = df_fkg["fob_kg"].round(2).astype(str)
            fig5 = px.line(df_fkg, x="semana_src", y="fob_kg", color="anio_src", markers=True,
                           text="texto",
                           labels={"semana_src":"Semana","fob_kg":"FOB/kg (USD)","anio_src":"Año"},
                           color_discrete_sequence=COLORES)
        fig5.update_traces(textposition="top center", textfont_size=9,
                           mode="lines+markers+text")
        _y5_max = df_fkg["fob_kg"].max() * 1.2
        fig5.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=350,
                           yaxis=dict(range=[0, _y5_max]))
        st.plotly_chart(fig5, use_container_width=True)

        g5, g6 = st.columns(2)
        with g5:
            st.subheader("Distribución por Producto")
            df_prod = df.groupby("producto")["cantidad"].sum().reset_index(name="cont")
            df_prod["cont"] = df_prod["cont"].round().astype(int)
            fig6 = px.pie(df_prod, names="producto", values="cont",
                          color_discrete_sequence=COLORES, hole=0.35)
            fig6.update_layout(height=320)
            st.plotly_chart(fig6, use_container_width=True)

        with g6:
            st.subheader("Contenedores por Puerto Origen")
            df_pto = (df.groupby("puerto")["cantidad"].sum().reset_index(name="cont")
                        .assign(cont=lambda x: x["cont"].round().astype(int))
                        .sort_values("cont", ascending=True).tail(10))
            fig7 = px.bar(df_pto, x="cont", y="puerto", orientation="h",
                          labels={"puerto":"","cont":"Contenedores"},
                          text_auto=".0f",
                          color_discrete_sequence=["#9B5DE5"])
            fig7.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA", height=320)
            st.plotly_chart(fig7, use_container_width=True)

        st.subheader("FOB Total por Año (US$ M)")
        df_fob_año = df.groupby("anio_src")["fob_total"].sum().reset_index()
        df_fob_año["fob_M"] = df_fob_año["fob_total"] / 1e6
        df_fob_año["anio_src"] = df_fob_año["anio_src"].astype(str)
        fig_fob = px.bar(df_fob_año, x="anio_src", y="fob_M",
                         labels={"anio_src":"Año","fob_M":"FOB (US$ M)"},
                         color="anio_src", color_discrete_sequence=COLORES, text_auto=".1f")
        fig_fob.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                              showlegend=False, height=350)
        st.plotly_chart(fig_fob, use_container_width=True)

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
        _multi_prod_t3 = df["producto"].nunique() > 1
        productos_t3 = sorted(df["producto"].unique()) if _multi_prod_t3 else [None]

        def _make_pivot(df_src):
            dp = df_src.groupby(["anio_src","semana_src"])["cantidad"].sum().reset_index(name="cont")
            dp["cont"] = dp["cont"].round().astype(int)
            pv = (dp.pivot(index="semana_src", columns="anio_src", values="cont")
                    .reindex(range(1,53)).fillna(0).astype(int))
            pv.index.name = "Semana"
            pv.columns = [str(c) for c in pv.columns]
            pv["TOTAL"] = pv.sum(axis=1)
            return pv

        if _multi_prod_t3:
            # Tabla combinada con columnas por producto+año
            df_piv_all = df.groupby(["producto","anio_src","semana_src"])["cantidad"].sum().reset_index(name="cont")
            df_piv_all["cont"] = df_piv_all["cont"].round().astype(int)
            df_piv_all["col"] = df_piv_all["producto"].str.split().str[0] + " " + df_piv_all["anio_src"].astype(str)
            pivot_wide = (df_piv_all.pivot_table(index="semana_src", columns="col", values="cont", aggfunc="sum")
                                    .reindex(range(1,53)).fillna(0).astype(int))
            pivot_wide.index.name = "Semana"
            pivot_wide["TOTAL"] = pivot_wide.sum(axis=1)
            st.subheader("Contenedores por Semana y Año — Todos los productos")
            tot_cols = [c for c in pivot_wide.columns if c != "TOTAL"]
            styled_wide = (pivot_wide.style
                           .background_gradient(subset=tot_cols, cmap="Greens", axis=None)
                           .background_gradient(subset=["TOTAL"], cmap="Blues", axis=None))
            st.dataframe(styled_wide, use_container_width=True, height=600)
            buf_all = io.BytesIO(); pivot_wide.to_excel(buf_all, engine="openpyxl"); buf_all.seek(0)
            st.download_button("⬇️ Excel Todos los productos", data=buf_all,
                               file_name="PCH_Contenedores_Semana_Productos.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.divider()
            # Secciones individuales por producto
            for prod in productos_t3:
                prod_label = prod.split()[0] if prod else ""
                st.subheader(f"Contenedores por Semana y Año — {prod_label}")
                pivot = _make_pivot(df[df["producto"] == prod])
                año_cols = [c for c in pivot.columns if c != "TOTAL"]
                styled = (pivot.style
                          .background_gradient(subset=año_cols, cmap="Greens", axis=None)
                          .background_gradient(subset=["TOTAL"], cmap="Blues", axis=None))
                st.dataframe(styled, use_container_width=True, height=400)
                buf_p = io.BytesIO(); pivot.to_excel(buf_p, engine="openpyxl"); buf_p.seek(0)
                st.download_button(f"⬇️ Excel {prod_label}", data=buf_p,
                                   file_name=f"PCH_Contenedores_{prod_label}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   key=f"dl_piv_{prod}")
        else:
            st.subheader("Contenedores por Semana y Año")
            pivot = _make_pivot(df)
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
                    .agg(Contenedores=("cantidad","sum"),
                         Peso_kg=("peso_neto","sum"),
                         FOB_USD=("fob_total","sum"),
                         FOB_kg_prom=("fob_kg","mean"))
                    .reset_index()
                    .sort_values("Contenedores", ascending=False))
        df_emb["Contenedores"] = df_emb["Contenedores"].round().astype(int)
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
                     .groupby(["anio_src","embarcador"])["cantidad"].sum().reset_index(name="cont"))
        df_top5["cont"] = df_top5["cont"].round().astype(int)
        df_top5["anio_src"] = df_top5["anio_src"].astype(str)
        años_orden = sorted(df_top5["anio_src"].unique())
        fig3 = px.line(df_top5, x="anio_src", y="cont", color="embarcador",
                       markers=True, text="cont",
                       labels={"anio_src":"Año","cont":"Contenedores"},
                       category_orders={"anio_src": años_orden},
                       color_discrete_sequence=COLORES)
        fig3.update_xaxes(type="category")
        fig3.update_traces(textfont_size=9, mode="lines+markers+text")
        _positions = ["top center", "bottom center", "top right", "bottom right", "top left"]
        for i, trace in enumerate(fig3.data):
            trace.textposition = _positions[i % len(_positions)]
        _y3_max = df_top5["cont"].max() * 1.2
        fig3.update_layout(plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
                           height=360, title="Evolución Top 5 Embarcadores por Año",
                           yaxis=dict(range=[0, _y3_max]))
        st.plotly_chart(fig3, use_container_width=True)

        # Tabla resumen
        st.dataframe(
            df_emb[["embarcador","Contenedores","Market Share %","FOB_M","FOB_kg_prom","Peso_M_kg"]]
            .rename(columns={"embarcador":"Embarcador","FOB_M":"FOB (US$ M)",
                             "FOB_kg_prom":"FOB/kg prom","Peso_M_kg":"Peso (M kg)"})
            .head(50),
            use_container_width=True, height=400
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Productores (comparación semanal)
# ══════════════════════════════════════════════════════════════════════════════

with tab5:
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
    else:
        st.subheader("Comparación de Embarcadores entre Semanas")

        semanas_disp = sorted(df["semana_src"].dropna().unique().astype(int))
        if len(semanas_disp) < 1:
            st.warning("No hay semanas en los datos filtrados.")
        else:
            c_a, c_b = st.columns(2)
            sem_b_default = semanas_disp[-1]
            sem_a_default = semanas_disp[-2] if len(semanas_disp) > 1 else semanas_disp[-1]
            with c_a:
                sem_a = st.selectbox("Semana A (base)", semanas_disp,
                                     index=semanas_disp.index(sem_a_default), key="t5_sem_a")
            with c_b:
                sem_b = st.selectbox("Semana B (comparar)", semanas_disp,
                                     index=semanas_disp.index(sem_b_default), key="t5_sem_b")

            # Embarcadores por semana (con conteo FCL)
            def _emb_sem(sem):
                d = df[df["semana_src"] == sem].groupby("embarcador")["cantidad"].sum().reset_index(name="fcl")
                d["fcl"] = d["fcl"].round().astype(int)
                return dict(zip(d["embarcador"], d["fcl"]))

            emb_a = _emb_sem(sem_a)
            emb_b = _emb_sem(sem_b)
            set_a, set_b = set(emb_a), set(emb_b)

            solo_a   = sorted(set_a - set_b)    # cargaron A pero NO B
            en_ambas = sorted(set_a & set_b)    # cargaron en ambas
            solo_b   = sorted(set_b - set_a)    # nuevos en B

            # Preparar DataFrames
            rows_a = [{"Embarcador": e, f"FCL S{sem_a}": emb_a[e]} for e in solo_a]
            df_a  = pd.DataFrame(rows_a).sort_values(f"FCL S{sem_a}", ascending=False) if rows_a else pd.DataFrame()

            rows_ab = [{"Embarcador": e, f"FCL S{sem_a}": emb_a[e],
                        f"FCL S{sem_b}": emb_b[e], "Δ FCL": emb_b[e] - emb_a[e]}
                       for e in en_ambas]
            df_ab = pd.DataFrame(rows_ab).sort_values(f"FCL S{sem_b}", ascending=False) if rows_ab else pd.DataFrame()

            rows_b = [{"Embarcador": e, f"FCL S{sem_b}": emb_b[e]} for e in solo_b]
            df_b  = pd.DataFrame(rows_b).sort_values(f"FCL S{sem_b}", ascending=False) if rows_b else pd.DataFrame()

            # ── KPIs resumen (para que cuadren con Tab 1) ─────────────────────
            fcl_a_total  = sum(emb_a.values())
            fcl_b_total  = sum(emb_b.values())
            embs_a_total = len(set_a)
            embs_b_total = len(set_b)
            ka1, ka2, kb1, kb2, kdelta = st.columns(5)
            ka1.metric(f"FCL total S{sem_a}",       f"{fcl_a_total:,}")
            ka2.metric(f"Embarcadores S{sem_a}",    f"{embs_a_total:,}")
            kb1.metric(f"FCL total S{sem_b}",       f"{fcl_b_total:,}",  delta=f"{fcl_b_total - fcl_a_total:+,}")
            kb2.metric(f"Embarcadores S{sem_b}",    f"{embs_b_total:,}", delta=f"{embs_b_total - embs_a_total:+,}")
            kdelta.metric("Solo en S" + str(sem_a) + " / Solo en S" + str(sem_b),
                          f"{len(solo_a)} / {len(solo_b)}", help="Embarcadores que NO repitieron / entraron nuevos")
            st.caption(f"ℹ️ Los {embs_a_total} embarcadores de S{sem_a} se dividen en: {len(solo_a)} que no cargaron S{sem_b} + {len(en_ambas)} que sí cargaron ambas semanas")
            st.divider()

            # Tabla unificada: todos los embarcadores con S_a y S_b
            todos = sorted(set_a | set_b)
            df_cmp = pd.DataFrame([{
                "Embarcador":       e,
                f"FCL S{sem_a}":   emb_a.get(e, 0),
                f"FCL S{sem_b}":   emb_b.get(e, 0),
                f"Δ FCL":          emb_b.get(e, 0) - emb_a.get(e, 0),
            } for e in todos]).sort_values(f"FCL S{sem_a}", ascending=False)

            # Colorear la columna Δ FCL en el HTML
            def _color_delta(val):
                if val > 0:   return "color:#1a7a3c; font-weight:600"
                if val < 0:   return "color:#c0392b; font-weight:600"
                return "color:#888"

            html_rows = ""
            for _, row in df_cmp.iterrows():
                delta = int(row["Δ FCL"])
                style = _color_delta(delta)
                sign  = "+" if delta > 0 else ""
                html_rows += (
                    f"<tr><td>{row['Embarcador']}</td>"
                    f"<td style='text-align:right'>{int(row[f'FCL S{sem_a}'])}</td>"
                    f"<td style='text-align:right'>{int(row[f'FCL S{sem_b}'])}</td>"
                    f"<td style='text-align:right;{style}'>{sign}{delta}</td></tr>"
                )
            html_table = f"""
            <table class="print-table">
              <thead><tr>
                <th>Embarcador</th>
                <th style="text-align:right">FCL S{sem_a}</th>
                <th style="text-align:right">FCL S{sem_b}</th>
                <th style="text-align:right">Δ FCL</th>
              </tr></thead>
              <tbody>{html_rows}</tbody>
            </table>"""
            st.markdown(html_table, unsafe_allow_html=True)

            # Excel
            buf_comp = io.BytesIO()
            df_cmp.to_excel(buf_comp, index=False, engine="openpyxl")
            buf_comp.seek(0)
            st.download_button(
                f"⬇️ Excel — Comparación S{sem_a} vs S{sem_b}",
                data=buf_comp,
                file_name=f"PCH_Comparacion_S{sem_a}_S{sem_b}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        st.divider()

        # Matriz embarcadores × semanas (top N)
        st.subheader("Actividad semanal por Embarcador")
        top_n_emb = st.slider("Top N embarcadores (por total FCL)", 10, 50, 20, key="t5_topn")
        top_emb = (df.groupby("embarcador")["cantidad"].sum()
                     .sort_values(ascending=False)
                     .head(top_n_emb).index.tolist())
        df_mat = (df[df["embarcador"].isin(top_emb)]
                    .groupby(["embarcador", "semana_src"])["cantidad"].sum()
                    .reset_index(name="fcl"))
        df_mat["fcl"] = df_mat["fcl"].round().astype(int)
        if not df_mat.empty:
            pivot_mat = (df_mat.pivot(index="embarcador", columns="semana_src", values="fcl")
                               .fillna(0).astype(int))
            pivot_mat.columns = [f"S{int(c)}" for c in pivot_mat.columns]
            pivot_mat["TOTAL"] = pivot_mat.sum(axis=1)
            pivot_mat = pivot_mat.sort_values("TOTAL", ascending=False)
            sem_cols = [c for c in pivot_mat.columns if c != "TOTAL"]

            # Calcular max por columna para intensidad de color (verde)
            col_max = {c: pivot_mat[c].max() or 1 for c in sem_cols}
            tot_max = pivot_mat["TOTAL"].max() or 1

            def _bg_green(val, mx):
                if val == 0: return "background:#fff"
                pct = val / mx
                g = int(180 + (1 - pct) * 70)
                return f"background:rgb({int(200*(1-pct))},{g},{int(180*(1-pct))})"

            def _bg_blue(val):
                if val == 0: return "background:#fff"
                pct = val / tot_max
                return f"background:rgb({int(200*(1-pct))},{int(200*(1-pct))},{int(240 - 60*pct)})"

            mat_headers = "".join(f"<th style='text-align:right'>{c}</th>" for c in sem_cols) + "<th style='text-align:right'>TOTAL</th>"
            mat_rows = ""
            for emb, row in pivot_mat.iterrows():
                cells = "".join(
                    f"<td style='text-align:right;{_bg_green(row[c], col_max[c])}'>{row[c] if row[c] else ''}</td>"
                    for c in sem_cols
                )
                _tot = row["TOTAL"]
                cells += f"<td style='text-align:right;font-weight:600;{_bg_blue(_tot)}'>{_tot}</td>"
                mat_rows += f"<tr><td>{emb}</td>{cells}</tr>"

            st.markdown(f"""
            <table class="print-table">
              <thead><tr><th>Embarcador</th>{mat_headers}</tr></thead>
              <tbody>{mat_rows}</tbody>
            </table>""", unsafe_allow_html=True)

            buf_mat = io.BytesIO()
            pivot_mat.to_excel(buf_mat, engine="openpyxl")
            buf_mat.seek(0)
            st.download_button("⬇️ Excel Matriz Embarcadores", data=buf_mat,
                               file_name="PCH_Embarcadores_Semana.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

