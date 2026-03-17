"""
PCH Global — CIRAD
Reporte semanal de precios FOT Palta Hass en Europa.
Fuente: CIRAD/FruitROP via dab SQL Server.
"""
import os
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="PCH Global — CIRAD",
    page_icon="🥑",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Conexión SQL (mismos secrets que Queneto) ─────────────────────────────────

def _secret(key, default=""):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

SQL_SERVER = _secret("SQL_SERVER_HOST")
SQL_DB     = _secret("SQL_DATABASE")
SQL_USER   = _secret("SQL_USER")
SQL_PASS   = _secret("SQL_PASSWORD")

def _conn():
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

# ── Estilos ───────────────────────────────────────────────────────────────────

VERDE   = "#1B4332"
VERDE_C = "#52B788"
NARANJA = "#F4A261"

st.markdown("""
<style>
[data-testid="stSidebar"] { display: none; }
[data-testid="collapsedControl"] { display: none; }
.block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
h1 { color: #1B4332; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────

col_logo, col_title, col_nav = st.columns([1, 5, 2])
with col_logo:
    st.markdown(f"<div style='background:{VERDE};color:white;padding:10px 16px;border-radius:8px;font-size:1.8rem;text-align:center'>🥑</div>", unsafe_allow_html=True)
with col_title:
    st.markdown(f"<h1 style='margin:0;padding-top:4px'>PCH GLOBAL — CIRAD</h1>", unsafe_allow_html=True)
    st.caption("Aguacate Hass — Precio FOT Mercado Europeo · CIRAD/FruitROP")
with col_nav:
    st.markdown("<div style='padding-top:12px'>", unsafe_allow_html=True)
    st.markdown("[← Queneto](./)")
    st.markdown("</div>", unsafe_allow_html=True)

st.divider()

# ── Carga de datos ────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_cirad():
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("SELECT semana, anio, avg_fot FROM cirad_historical_avg ORDER BY anio, semana")
        hist = pd.DataFrame(cur.fetchall(), columns=["semana", "anio", "avg_fot"])
        cur.execute("""SELECT semana, anio, ref_hass_18, fot_1214, fot_161820, fot_2224,
                              fot_26_kg, avg_fot, ata2_eur_kg
                       FROM cirad_weekly_prices ORDER BY anio, semana""")
        weekly = pd.DataFrame(cur.fetchall(),
                               columns=["semana","anio","ref_hass_18","fot_1214",
                                        "fot_161820","fot_2224","fot_26_kg","avg_fot","ata2_eur_kg"])
        conn.close()
        return hist, weekly, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), str(e)

with st.spinner("Cargando datos CIRAD..."):
    df_hist, df_weekly, err = load_cirad()

if err:
    st.error(f"Error conectando a base de datos: {err}")
    st.caption(f"Servidor: `{SQL_SERVER}` · DB: `{SQL_DB}`")
    st.stop()

if df_hist.empty and df_weekly.empty:
    st.warning("Sin datos CIRAD disponibles.")
    st.stop()

# ── Combinar histórico + semanal para gráfico ─────────────────────────────────

weekly_avg = df_weekly[["semana","anio","avg_fot"]].dropna(subset=["avg_fot"])
years_hist   = set(df_hist["anio"].unique())
years_weekly = set(weekly_avg["anio"].unique())
extra = weekly_avg[weekly_avg["anio"].isin(years_weekly - years_hist)]
df_all = pd.concat([df_hist, extra], ignore_index=True).sort_values(["anio","semana"])

# ── Semana más reciente ───────────────────────────────────────────────────────

df_w_clean = df_weekly.dropna(subset=["avg_fot"])
last = df_w_clean.iloc[-1] if not df_w_clean.empty else None
prev = df_w_clean.iloc[-2] if len(df_w_clean) > 1 else None

# ── Indicadores KPI ───────────────────────────────────────────────────────────

if last is not None:
    sem_label = f"W{int(last.semana):02d} — {int(last.anio)}"
    st.markdown(f"### Semana {sem_label}")

    k1, k2, k3, k4, k5, k6 = st.columns(6)

    def _delta(cur_val, prev_val):
        if prev is None or pd.isna(prev_val) or pd.isna(cur_val):
            return None
        d = float(cur_val) - float(prev_val)
        return f"{d:+.2f}€"

    k1.metric("Ref. Hass 18",
              f"€{last.ref_hass_18:.2f}" if pd.notna(last.ref_hass_18) else "—",
              _delta(last.ref_hass_18, prev.ref_hass_18 if prev is not None else None))
    k2.metric("Grade 12/14",
              f"€{last.fot_1214:.2f}" if pd.notna(last.fot_1214) else "—",
              _delta(last.fot_1214, prev.fot_1214 if prev is not None else None))
    k3.metric("Grade 16/18/20",
              f"€{last.fot_161820:.2f}" if pd.notna(last.fot_161820) else "—",
              _delta(last.fot_161820, prev.fot_161820 if prev is not None else None))
    k4.metric("Grade 22/24",
              f"€{last.fot_2224:.2f}" if pd.notna(last.fot_2224) else "—",
              _delta(last.fot_2224, prev.fot_2224 if prev is not None else None))
    k5.metric("Promedio FOT",
              f"€{last.avg_fot:.2f}" if pd.notna(last.avg_fot) else "—",
              _delta(last.avg_fot, prev.avg_fot if prev is not None else None))
    k6.metric("ATA+2 €/kg",
              f"€{last.ata2_eur_kg:.4f}" if pd.notna(last.ata2_eur_kg) else "—",
              _delta(last.ata2_eur_kg, prev.ata2_eur_kg if prev is not None else None))

st.divider()

# ── Gráfico histórico + tabla detalle ────────────────────────────────────────

c1, c2 = st.columns([3, 1])

PALETTE = ["#CCCCCC","#AAAACC","#999999","#888888","#F4A261","#52B788","#1B4332"]

with c1:
    st.markdown("**Evolución histórica — Promedio FOT Hass (€/caja 4kg)**")
    fig = go.Figure()
    years_sorted = sorted(df_all["anio"].unique())
    yr_max = max(years_sorted)
    for i, yr in enumerate(years_sorted):
        sub = df_all[df_all["anio"] == yr].dropna(subset=["avg_fot"])
        is_cur = (yr == yr_max)
        fig.add_trace(go.Scatter(
            x=sub["semana"], y=sub["avg_fot"],
            name=str(yr),
            mode="lines+markers" if is_cur else "lines",
            line=dict(
                color=PALETTE[i % len(PALETTE)],
                width=3 if is_cur else 1.5,
                dash="solid" if yr >= 2023 else "dot"
            ),
            marker=dict(size=6 if is_cur else 3),
            hovertemplate=f"{yr} W%{{x}}: €%{{y:.2f}}<extra></extra>"
        ))
    fig.update_layout(
        height=420, plot_bgcolor="#FAFAFA", paper_bgcolor="#FAFAFA",
        xaxis=dict(title="Semana", range=[1, 52], dtick=4),
        yaxis=dict(title="€/caja 4kg", rangemode="tozero"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        margin=dict(t=20, b=40)
    )
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.markdown(f"**Detalle {yr_max} por grade**")
    df_show = df_weekly[df_weekly["anio"] == yr_max].copy()
    df_show = df_show[["semana","ref_hass_18","fot_1214","fot_161820","fot_2224","avg_fot"]].dropna(subset=["avg_fot"])
    df_show.columns = ["Sem","Ref 18","12/14","16/18/20","22/24","Avg"]
    df_show["Sem"] = df_show["Sem"].apply(lambda x: f"W{int(x):02d}")
    for col in ["Ref 18","12/14","16/18/20","22/24","Avg"]:
        df_show[col] = df_show[col].apply(lambda x: f"€{x:.2f}" if pd.notna(x) else "—")
    st.dataframe(df_show.iloc[::-1].reset_index(drop=True), use_container_width=True, height=420)

st.divider()

# ── Tabla comparativa anual completa ─────────────────────────────────────────

st.markdown("**Histórico completo — Promedio FOT por semana y año**")
st.caption("Valores en €/caja 4kg · Promedio grades 12/14 + 16/18/20 + 22/24 · Fuente: Excel histórico 2020-2025 + PDFs CIRAD")

pivot = df_all.pivot_table(index="semana", columns="anio", values="avg_fot", aggfunc="first")
pivot.index = pivot.index.map(lambda x: f"W{int(x):02d}")
pivot.columns = [str(c) for c in pivot.columns]

def color_fot(val):
    if pd.isna(val) or val == "":
        return "color: #BBBBBB"
    try:
        v = float(str(val).replace("€",""))
        if v < 5:    return "background-color: #FF6B6B; color: white"
        if v < 7:    return "background-color: #F4A261; color: black"
        if v < 9:    return "background-color: #FFD166; color: black"
        if v < 11:   return "background-color: #A8DADC; color: black"
        return              "background-color: #52B788; color: white"
    except:
        return ""

fmt = {c: "€{:.2f}" for c in pivot.columns}
st.dataframe(
    pivot.style.format(fmt, na_rep="—").applymap(color_fot),
    use_container_width=True,
    height=600
)

st.caption("🟢 Alto >€11 · 🔵 Medio-alto €9-11 · 🟡 Medio €7-9 · 🟠 Medio-bajo €5-7 · 🔴 Bajo <€5")

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption("Fuente: CIRAD/FruitROP · Solo Palta Hass · Tránsito Perú→Europa: 20 días ≈ 3 semanas · PCH Global Operations")
