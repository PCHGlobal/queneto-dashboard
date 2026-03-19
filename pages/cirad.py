"""
PCH Global — CIRAD
Reporte semanal de precios FOT Palta Hass en Europa.
Fuente: CIRAD/FruitROP via dab SQL Server.
"""
import io
import os
import datetime
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

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

col_title, col_nav = st.columns([6, 1])
with col_title:
    st.markdown(
        f"<div style='display:flex;align-items:center;gap:14px;margin-top:6px'>"
        f"<div style='background:{VERDE};color:white;padding:8px 14px;border-radius:8px;"
        f"font-size:1.8rem;line-height:1;flex-shrink:0;margin-top:2px'>🥑</div>"
        f"<div><h1 style='margin:0;line-height:1.1'>PCH GLOBAL — CIRAD</h1>"
        f"<span style='color:#666;font-size:0.85rem'>Aguacate Hass — Precio FOT Mercado Europeo · CIRAD/FruitROP</span></div>"
        f"</div>",
        unsafe_allow_html=True,
    )
with col_nav:
    st.markdown(
        "<div style='padding-top:14px'>"
        "<a href='/' target='_self' style='"
        "display:block;text-align:center;padding:8px 12px;"
        "background:#1B4332;color:white;"
        "border-radius:6px;text-decoration:none;"
        "font-weight:600;font-size:0.9rem'>"
        "← Queneto</a></div>",
        unsafe_allow_html=True,
    )

st.divider()

# ── Carga de datos ────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800, show_spinner=False)
def load_cirad():
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("SELECT semana, anio, avg_fot FROM cirad_historical_avg ORDER BY anio, semana")
        hist = pd.DataFrame(cur.fetchall(), columns=["semana", "anio", "avg_fot"])
        cur.execute("""SELECT semana, anio, ref_hass_18, fot_1214, fot_161820, fot_2224,
                              fot_26_kg, avg_fot
                       FROM cirad_weekly_prices WHERE anio >= 2026 ORDER BY anio, semana""")
        weekly = pd.DataFrame(cur.fetchall(),
                               columns=["semana","anio","ref_hass_18","fot_1214",
                                        "fot_161820","fot_2224","fot_26_kg","avg_fot"])
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

# ── PDF Report ────────────────────────────────────────────────────────────────

def _fig_png(fig, dpi=130):
    b = io.BytesIO()
    fig.savefig(b, format="png", dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    b.seek(0)
    return b

def generate_cirad_pdf(df_weekly, df_hist, df_all, last, prev):
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors as C
    from reportlab.lib.units import cm
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                     Paragraph, Spacer, Image, PageBreak)
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

    W, H = A4
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            rightMargin=1.5*cm, leftMargin=1.5*cm,
                            topMargin=1.5*cm, bottomMargin=1.5*cm)
    cw = W - 3*cm

    VERDE   = C.HexColor("#1B4332")
    VERDE_C = C.HexColor("#52B788")
    NARANJA = C.HexColor("#F4A261")
    BG_LT   = C.HexColor("#EBF5EE")

    def _p(text, size=9, bold=False, color=C.black, align=TA_LEFT, leading=None):
        return Paragraph(text, ParagraphStyle("_",
            fontSize=size, fontName="Helvetica-Bold" if bold else "Helvetica",
            textColor=color, alignment=align, leading=leading or (size + 3)))

    sem_num  = int(last.semana) if last is not None else 0
    anio_num = int(last.anio)   if last is not None else 0
    sem_label = f"W{sem_num:02d}-{anio_num}"

    elements = []

    # ── Header ────────────────────────────────────────────────────────────────
    hdr = Table([[
        _p("<b>PCH GLOBAL</b>", 22, False, C.white),
        _p("Premium Choice Global — Análisis de Mercado", 8, False, C.HexColor("#A8D5B5")),
        _p("REPORTE SEMANAL<br/><b>CIRAD</b>", 14, False, C.white, TA_RIGHT),
        _p("Aguacate Hass — Mercado Europeo", 8, False, NARANJA, TA_RIGHT),
    ]], colWidths=[cw*0.30, cw*0.32, cw*0.20, cw*0.18])
    hdr.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), VERDE),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING", (0,0), (0,0), 14),
        ("TOPPADDING", (0,0), (-1,-1), 12),
        ("BOTTOMPADDING", (0,0), (-1,-1), 12),
    ]))
    elements.append(hdr)
    elements.append(Spacer(1, 3))

    # ── Week banner ───────────────────────────────────────────────────────────
    ref_str = (f"Referencia Hass Grade 18: <b>€{last.ref_hass_18:.2f}/caja</b>"
               if last is not None and pd.notna(last.ref_hass_18) else "")
    ata_str = (f"Promedio FOT CIRAD:<br/><b>€{last.avg_fot:.2f}/caja</b>"
               if last is not None and pd.notna(last.avg_fot) else "")
    banner = Table([[
        _p(f"<b>SEMANA {sem_num:02d} — {anio_num}</b>", 13, False, C.white),
        _p(ref_str, 10, False, C.white),
        _p(ata_str, 9, False, C.white, TA_RIGHT),
    ]], colWidths=[cw*0.25, cw*0.50, cw*0.25])
    banner.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), VERDE_C),
        ("TOPPADDING", (0,0), (-1,-1), 8), ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LEFTPADDING", (0,0), (0,0), 10), ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
    ]))
    elements.append(banner)
    elements.append(Spacer(1, 6))

    # ── KPI indicators ────────────────────────────────────────────────────────
    elements.append(_p(f"<b>Indicadores Clave — {sem_label}</b>", 10, False, VERDE))
    elements.append(Spacer(1, 4))

    def _kv(field, unit="/caja"):
        if last is None: return ("—", None)
        val = last[field] if field in last.index else None
        pval = prev[field] if (prev is not None and field in prev.index) else None
        if val is None or pd.isna(val): return ("—", None)
        dstr = None
        if pval is not None and not pd.isna(pval):
            d = float(val) - float(pval)
            dstr = f"{d:+.2f}€"
        return (f"{float(val):.2f}{unit}", dstr)

    kpis = [
        ("Ref. Hass 18",         *_kv("ref_hass_18")),
        ("FOT Grade 12/14",      *_kv("fot_1214")),
        ("FOT Grade 16/18/20",   *_kv("fot_161820")),
        ("FOT Grade 22/24",      *_kv("fot_2224")),
        ("FOT Grade 26",         *_kv("fot_26_kg", "/kg")),
        ("Promedio FOT CIRAD",   *_kv("avg_fot")),
    ]

    def _kpi_cell(label, val, delta):
        dc = ""
        if delta:
            clr = "#CC0000" if delta.startswith("-") else "#006600"
            dc = f'<br/><font color="{clr}">{delta}</font>'
        return _p(f'<font size="7" color="#555555">{label}</font><br/>'
                  f'<b><font size="13">€{val}</font></b>{dc}', 9, leading=17)

    row1 = [_kpi_cell(*k) for k in kpis[:4]]
    row2 = [_kpi_cell(*k) for k in kpis[4:7]] + [_p("")]
    kpi_tbl = Table([row1, row2], colWidths=[cw/4]*4)
    kpi_tbl.setStyle(TableStyle([
        ("BOX",         (0,0), (-1,-1), 0.5, C.HexColor("#CCCCCC")),
        ("INNERGRID",   (0,0), (-1,-1), 0.5, C.HexColor("#CCCCCC")),
        ("BACKGROUND",  (0,0), (-1,-1), BG_LT),
        ("TOPPADDING",  (0,0), (-1,-1), 6),
        ("BOTTOMPADDING",(0,0),(-1,-1), 6),
        ("LEFTPADDING", (0,0), (-1,-1), 8),
    ]))
    elements.append(kpi_tbl)
    elements.append(Spacer(1, 8))

    # ── Historical chart ──────────────────────────────────────────────────────
    elements.append(_p(f"<b>Evolución Histórica — Promedio FOT Hass (2020–{anio_num})</b>", 10, False, VERDE))
    elements.append(Spacer(1, 3))
    PAL = ["#CCCCCC","#AAAACC","#999999","#888888","#F4A261","#52B788","#1B4332"]
    years_sorted = sorted(df_all["anio"].unique())
    yr_max = max(years_sorted)
    fig1, ax1 = plt.subplots(figsize=(12, 4))
    for i, yr in enumerate(years_sorted):
        sub = df_all[df_all["anio"]==yr].dropna(subset=["avg_fot"]).sort_values("semana")
        is_cur = (yr == yr_max)
        ax1.plot(sub["semana"], sub["avg_fot"], color=PAL[i % len(PAL)],
                 lw=3 if is_cur else 1.5, ls="-" if yr >= 2023 else ":",
                 marker="o" if is_cur else None, ms=5, label=str(yr))
        if is_cur and not sub.empty:
            r = sub.iloc[-1]
            ax1.annotate(f"W{int(r.semana):02d}: {r.avg_fot:.2f}€",
                         (r.semana, r.avg_fot),
                         textcoords="offset points", xytext=(6, 4),
                         fontsize=7, color=PAL[i % len(PAL)])
    ax1.set_xlim(1, 52); ax1.set_ylim(bottom=0)
    ax1.set_xlabel("Semana", fontsize=8); ax1.set_ylabel("€/caja 4kg", fontsize=8)
    ax1.legend(loc="upper right", fontsize=7, ncol=len(years_sorted))
    ax1.grid(True, alpha=0.3); ax1.set_facecolor("#FAFAFA")
    fig1.patch.set_facecolor("white"); ax1.tick_params(labelsize=7)
    fig1.tight_layout(pad=0.5)
    elements.append(Image(_fig_png(fig1), width=cw, height=cw*0.34))
    elements.append(Spacer(1, 8))

    # ── Comparison table ──────────────────────────────────────────────────────
    elements.append(_p("<b>Precios FOT por Grade — Comparativo Semanal</b>", 10, False, VERDE))
    elements.append(Spacer(1, 3))
    df_comp = df_weekly[df_weekly["anio"] == anio_num].dropna(subset=["avg_fot"])
    hdr_row = ["Semana","Ref Hass 18","Grade 12/14","Grade 16/18/20","Grade 22/24","Grade 26 (€/kg)","Avg FOT CIRAD"]
    rows = [hdr_row]
    for _, r in df_comp.iterrows():
        is_last = (last is not None and r.semana == last.semana and r.anio == last.anio)
        rows.append([
            f"{'★ ' if is_last else ''}W{int(r.semana):02d}-{int(r.anio)}",
            f"€{r.ref_hass_18:.2f}"  if pd.notna(r.ref_hass_18)  else "—",
            f"€{r.fot_1214:.2f}"     if pd.notna(r.fot_1214)     else "—",
            f"€{r.fot_161820:.2f}"   if pd.notna(r.fot_161820)   else "—",
            f"€{r.fot_2224:.2f}"     if pd.notna(r.fot_2224)     else "—",
            f"€{r.fot_26_kg:.2f}"    if pd.notna(r.fot_26_kg)    else "—",
            f"€{r.avg_fot:.2f}"      if pd.notna(r.avg_fot)      else "—",
        ])
    cw7 = cw / 7
    ctbl = Table(rows, colWidths=[cw7*1.3] + [cw7*0.95]*6)
    ctbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,0), VERDE),
        ("TEXTCOLOR",    (0,0), (-1,0), C.white),
        ("FONTNAME",     (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTNAME",     (0,len(rows)-1), (-1,len(rows)-1), "Helvetica-Bold"),
        ("FONTSIZE",     (0,0), (-1,-1), 7.5),
        ("INNERGRID",    (0,0), (-1,-1), 0.3, C.HexColor("#DDDDDD")),
        ("BOX",          (0,0), (-1,-1), 0.5, C.HexColor("#AAAAAA")),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [C.white, BG_LT]),
        ("TOPPADDING",   (0,0), (-1,-1), 4),
        ("BOTTOMPADDING",(0,0), (-1,-1), 4),
        ("LEFTPADDING",  (0,0), (-1,-1), 4),
        ("ALIGN",        (1,0), (-1,-1), "CENTER"),
    ]))
    elements.append(ctbl)
    elements.append(Spacer(1, 8))

    # ── Grade bar charts ──────────────────────────────────────────────────────
    elements.append(_p(f"<b>Distribución de Precios por Grade — {sem_label}</b>", 10, False, VERDE))
    elements.append(Spacer(1, 3))
    if last is not None:
        fig2, (bax1, bax2) = plt.subplots(1, 2, figsize=(10, 2.8),
                                            gridspec_kw={"width_ratios": [3, 1]})
        g_lbl = ["Grado 12/14", "Grado 16/18/20", "Grado 22/24"]
        g_val = [last.fot_1214, last.fot_161820, last.fot_2224]
        bars = bax1.bar(range(3), g_val, color="#1B4332", width=0.5)
        valid = [v for v in g_val if pd.notna(v)]
        if valid: bax1.set_ylim(0, max(valid) * 1.25)
        bax1.set_xticks(range(3)); bax1.set_xticklabels(g_lbl, fontsize=8)
        bax1.set_ylabel("€ /caja 4kg", fontsize=8)
        bax1.set_title("Grades 12-24 (€ / caja 4kg)", fontsize=9)
        bax1.set_facecolor("#FAFAFA"); bax1.tick_params(labelsize=7)
        for bar, v in zip(bars, g_val):
            if pd.notna(v):
                bax1.text(bar.get_x()+bar.get_width()/2, float(v)+0.05,
                          f"€{v:.2f}", ha="center", va="bottom", fontsize=8, fontweight="bold")
        val26 = last.fot_26_kg
        if pd.notna(val26):
            bax2.bar([0], [val26], color="#1B4332", width=0.4)
            bax2.text(0, float(val26)+0.05, f"€{val26:.2f}",
                      ha="center", va="bottom", fontsize=8, fontweight="bold")
            bax2.set_ylim(0, float(val26)*1.4)
        bax2.set_xticks([0]); bax2.set_xticklabels(["Grade 26"], fontsize=8)
        bax2.set_ylabel("€ /kg", fontsize=8)
        bax2.set_title("Grade 26\n(€/kg — unidad diferente)", fontsize=9)
        bax2.set_facecolor("#FAFAFA"); bax2.tick_params(labelsize=7)
        fig2.tight_layout(pad=0.5)
        elements.append(Image(_fig_png(fig2), width=cw, height=cw*0.25))

    # ── PAGE 2 ────────────────────────────────────────────────────────────────
    elements.append(PageBreak())
    hdr2 = Table([[
        _p("PCH GLOBAL — Análisis de Mercado | Continuación", 10, True, C.white),
        _p(f"Semana {sem_num:02d} — {anio_num}", 9, False, C.white, TA_RIGHT),
    ]], colWidths=[cw*0.70, cw*0.30])
    hdr2.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), VERDE),
        ("TOPPADDING",   (0,0), (-1,-1), 6),
        ("BOTTOMPADDING",(0,0), (-1,-1), 6),
        ("LEFTPADDING",  (0,0), (0,0),  10),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
    ]))
    elements.append(hdr2)
    elements.append(Spacer(1, 10))

    # ── Avg FOT trend ─────────────────────────────────────────────────────────
    yr_max_w  = int(df_weekly["anio"].max())
    yr_prev_w = yr_max_w - 1
    elements.append(_p(f"<b>Tendencia Promedio FOT CIRAD — {yr_prev_w} a {yr_max_w} (€/caja)</b>", 10, False, VERDE))
    elements.append(Spacer(1, 3))
    fig3, ax3 = plt.subplots(figsize=(12, 2.8))
    for yr, color, lw in [(yr_prev_w, "#52B788", 1.5), (yr_max_w, "#1B4332", 2.5)]:
        sub = df_weekly[df_weekly["anio"]==yr].dropna(subset=["avg_fot"]).sort_values("semana")
        if not sub.empty:
            ax3.plot(sub["semana"], sub["avg_fot"], color=color, lw=lw,
                     marker="o", ms=4, label=f"Avg FOT € {yr}")
            for _, r in sub.iterrows():
                ax3.annotate(f"{r.avg_fot:.2f}", (r.semana, r.avg_fot),
                             textcoords="offset points", xytext=(0, 5),
                             fontsize=6, ha="center")
    ax3.set_xlim(1, 52)
    ax3.set_xlabel(f"Semana {yr_prev_w} → {yr_max_w}", fontsize=8)
    ax3.set_ylabel("Avg FOT CIRAD (€/caja)", fontsize=8)
    ax3.legend(fontsize=7); ax3.grid(True, alpha=0.3)
    ax3.set_facecolor("#FAFAFA"); fig3.patch.set_facecolor("white")
    ax3.tick_params(labelsize=7); fig3.tight_layout(pad=0.5)
    elements.append(Image(_fig_png(fig3), width=cw, height=cw*0.22))
    elements.append(Spacer(1, 10))

    # ── Full historical table ─────────────────────────────────────────────────
    elements.append(_p("<b>Histórico Promedio FOT €/caja 4kg — Comparativo Anual por Semana (datos desde 2020)</b>",
                       10, False, VERDE))
    elements.append(Spacer(1, 2))
    elements.append(_p("Valores en €/caja 4kg (promedio grades 12/14 + 16/18/20 + 22/24). "
                       "Grade 26 excluido del promedio (cotiza en €/kg). "
                       f"Columna {anio_num} = datos extraídos de PDFs CIRAD.",
                       7, False, C.HexColor("#555555")))
    elements.append(Spacer(1, 4))

    all_years = sorted(df_all["anio"].unique())
    yr_cols   = [str(int(y)) for y in all_years]
    piv_data  = {}
    for yr in all_years:
        sub = df_all[df_all["anio"]==yr].set_index("semana")["avg_fot"]
        piv_data[str(int(yr))] = sub

    def _val(sem, yc):
        if yc in piv_data and sem in piv_data[yc].index:
            v = piv_data[yc][sem]
            return f"{v:.2f}" if pd.notna(v) else "—"
        return "—"

    # Two-half layout (W01-W26 left | W27-W52 right)
    thdr = ["Sem"] + yr_cols
    trows = [thdr + thdr]
    for i in range(26):
        s1, s2 = i+1, i+27
        r1 = [f"W{s1:02d}"] + [_val(s1, y) for y in yr_cols]
        r2 = [f"W{s2:02d}"] + [_val(s2, y) for y in yr_cols]
        trows.append(r1 + r2)

    n_yr     = len(yr_cols)
    cw_sem   = 0.85*cm
    cw_yr    = (cw/2 - cw_sem) / n_yr
    col_widths = ([cw_sem] + [cw_yr]*n_yr) * 2
    htbl = Table(trows, colWidths=col_widths, repeatRows=1)

    hs = [
        ("FONTSIZE",    (0,0), (-1,-1), 6.5),
        ("FONTNAME",    (0,0), (-1,0),  "Helvetica-Bold"),
        ("BACKGROUND",  (0,0), (-1,0),  VERDE),
        ("TEXTCOLOR",   (0,0), (-1,0),  C.white),
        ("ALIGN",       (0,0), (-1,-1), "CENTER"),
        ("TOPPADDING",  (0,0), (-1,-1), 2),
        ("BOTTOMPADDING",(0,0),(-1,-1), 2),
        ("INNERGRID",   (0,0), (-1,-1), 0.2, C.HexColor("#CCCCCC")),
        ("BOX",         (0,0), (-1,-1), 0.5, C.HexColor("#AAAAAA")),
        ("LINEAFTER",   (n_yr, 0), (n_yr, -1), 1.5, C.HexColor("#888888")),
    ]
    for ri, row in enumerate(trows[1:], 1):
        for ci, v in enumerate(row):
            if v and v != "—" and ci > 0 and ci != n_yr+1:
                try:
                    fv = float(v)
                    bg = (C.HexColor("#52B788") if fv >= 11 else
                          C.HexColor("#A8DADC") if fv >= 9  else
                          C.HexColor("#FFD166") if fv >= 7  else
                          C.HexColor("#F4A261") if fv >= 5  else
                          C.HexColor("#FF6B6B"))
                    hs.append(("BACKGROUND", (ci, ri), (ci, ri), bg))
                except: pass
    htbl.setStyle(TableStyle(hs))
    elements.append(htbl)
    elements.append(Spacer(1, 8))

    # ── Footer ────────────────────────────────────────────────────────────────
    today = datetime.date.today().strftime("%d %b %Y")
    elements.append(_p(
        "Fuente: CIRAD/FruitROP — Datos: Excel histórico 2020-2025 + PDFs semanas recientes | "
        "Perú destacado como mercado clave PCH.",
        6.5, False, C.HexColor("#555555")))
    elements.append(Spacer(1, 3))
    elements.append(_p(f"Generado: {today} | PCH Global Operations", 7, False,
                       C.HexColor("#888888"), TA_RIGHT))

    doc.build(elements)
    buf.seek(0)
    return buf

# ── Indicadores KPI ───────────────────────────────────────────────────────────

if last is not None:
    sem_label = f"W{int(last.semana):02d} — {int(last.anio)}"
    st.markdown(f"### Semana {sem_label}")

    k1, k2, k3, k4, k5 = st.columns(5)

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
    k5.metric("Promedio FOT CIRAD",
              f"€{last.avg_fot:.2f}" if pd.notna(last.avg_fot) else "—",
              _delta(last.avg_fot, prev.avg_fot if prev is not None else None))

st.divider()

# ── Descargar Reporte PDF ─────────────────────────────────────────────────────

if last is not None:
    _sem_fn = f"W{int(last.semana):02d}_{int(last.anio)}"
    if st.button(f"⬇️ Descargar Reporte PDF — {_sem_fn}", type="primary"):
        with st.spinner("Generando PDF..."):
            _pdf = generate_cirad_pdf(df_weekly, df_hist, df_all, last, prev)
        st.download_button(
            label=f"📄 CIRAD_Reporte_{_sem_fn}.pdf",
            data=_pdf,
            file_name=f"CIRAD_Reporte_{_sem_fn}.pdf",
            mime="application/pdf",
        )

st.divider()

# ── Gráfico histórico + tabla detalle ────────────────────────────────────────

PALETTE = ["#CCCCCC","#AAAACC","#999999","#888888","#F4A261","#52B788","#1B4332"]

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

st.markdown(f"**Detalle {yr_max} por grade**")
df_show = df_weekly[df_weekly["anio"] == yr_max].copy()
df_show = df_show[["semana","ref_hass_18","fot_1214","fot_161820","fot_2224","avg_fot"]].dropna(subset=["avg_fot"])
df_show.columns = ["Sem","Ref 18","12/14","16/18/20","22/24","Avg"]
df_show["Sem"] = df_show["Sem"].apply(lambda x: f"W{int(x):02d}")
for col in ["Ref 18","12/14","16/18/20","22/24","Avg"]:
    df_show[col] = df_show[col].apply(lambda x: f"€{x:.2f}" if pd.notna(x) else "—")
st.dataframe(df_show.iloc[::-1].reset_index(drop=True), use_container_width=True, height=280)

st.divider()

# ── Tabla comparativa anual completa ─────────────────────────────────────────

st.markdown("**Histórico completo — Promedio FOT por semana y año**")
st.caption("Valores en €/caja 4kg · Promedio grades 12/14 + 16/18/20 + 22/24 · Fuente: Excel histórico 2020-2025 + PDFs CIRAD")

pivot = df_all.pivot_table(index="semana", columns="anio", values="avg_fot", aggfunc="first")
pivot = pivot.reindex(range(1, 53))          # siempre W01–W52
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

