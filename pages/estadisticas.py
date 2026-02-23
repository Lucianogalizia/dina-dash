# ==========================================================
# pages/estadisticas.py — Tab 2: Estadísticas
# Migración FIEL de tab_stats del app.py Streamlit original.
# ==========================================================

import dash
from dash import html, dcc, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from data.loaders     import load_din_index, load_niv_index, resolve_existing_path, load_coords_repo
from data.consolidado import build_global_consolidated
from data.parsers     import parse_extras_for_paths, EXTRA_FIELDS
from utils.helpers    import find_col, build_keys, safe_to_float, compute_sumergencia_and_base, trend_linear_per_month

dash.register_page(__name__, path="/estadisticas", name="Estadísticas", order=2)


def _df_to_table(df: pd.DataFrame) -> list[dict]:
    """
    Convierte un DataFrame a lista de dicts para DataTable de Dash.
    - Convierte Timestamps a string legible
    - Reemplaza NaN/NaT/None por "" (cadena vacía, no "nan")
    - Convierte Period a string
    - Evita tipos no serializables por JSON
    """
    d = df.copy()
    for col in d.columns:
        # Timestamps / datetime
        if pd.api.types.is_datetime64_any_dtype(d[col]):
            d[col] = d[col].dt.strftime("%Y-%m-%d %H:%M").where(d[col].notna(), "")
        # Period
        elif hasattr(d[col], "dt") and hasattr(d[col].dt, "to_timestamp"):
            try:
                d[col] = d[col].astype(str)
            except Exception:
                d[col] = d[col].apply(str)
        # Numéricos: redondear y reemplazar NaN
        elif pd.api.types.is_float_dtype(d[col]):
            d[col] = d[col].apply(lambda x: round(x, 2) if pd.notna(x) else "")
        elif pd.api.types.is_integer_dtype(d[col]):
            d[col] = d[col].apply(lambda x: int(x) if pd.notna(x) else "")
        else:
            # Todo lo demás: convertir a string, NaN → ""
            def _safe_str(x):
                try:
                    return "" if pd.isna(x) else (x if isinstance(x, str) else str(x))
                except Exception:
                    return str(x) if x is not None else ""
            d[col] = d[col].apply(_safe_str)
    return d.to_dict("records")


def _get_data():
    """Carga y construye todos los datos. Retorna (snap, df_all, din_ok_ref)."""
    df_din = load_din_index()
    df_niv = load_niv_index()

    din_no = find_col(df_din, ["pozo", "NO"])
    din_fe = find_col(df_din, ["fecha", "FE"])
    din_ho = find_col(df_din, ["hora",  "HO"])
    niv_no = find_col(df_niv, ["pozo", "NO"])
    niv_fe = find_col(df_niv, ["fecha", "FE"])
    niv_ho = find_col(df_niv, ["hora",  "HO"])

    din_k = build_keys(df_din, din_no, din_fe, din_ho) if (not df_din.empty and din_no and din_fe) else pd.DataFrame()
    niv_k = build_keys(df_niv, niv_no, niv_fe, niv_ho) if (not df_niv.empty and niv_no and niv_fe) else pd.DataFrame()

    din_ok = din_k[din_k["error"].isna()] if (not din_k.empty and "error" in din_k.columns) else din_k
    niv_ok = niv_k[niv_k["error"].isna()] if (not niv_k.empty and "error" in niv_k.columns) else niv_k

    if not din_ok.empty and "path" in din_ok.columns:
        din_ok = din_ok.copy()
        din_ok["path"] = din_ok["path"].apply(lambda x: resolve_existing_path(x) if pd.notna(x) else None)

    df_all = build_global_consolidated(din_ok, niv_ok, din_no, din_fe, din_ho, niv_no, niv_fe, niv_ho)

    if df_all.empty:
        return pd.DataFrame(), pd.DataFrame(), din_ok

    df_all = df_all.copy()
    df_all["DT_plot"] = pd.to_datetime(df_all["DT_plot"], errors="coerce")

    snap = (df_all.sort_values(["NO_key", "DT_plot"], na_position="last")
                  .dropna(subset=["DT_plot"])
                  .groupby("NO_key", as_index=False).tail(1).copy())

    # Parse extras para snapshot DIN
    if "path" in snap.columns:
        snap = snap.copy()
        snap["path_res"] = snap["path"].apply(lambda x: resolve_existing_path(x) if pd.notna(x) else None)
        din_mask  = (snap.get("ORIGEN", pd.Series(dtype=str)) == "DIN") & snap["path_res"].notna()
        din_paths = snap.loc[din_mask, "path_res"].astype(str).tolist()
        if din_paths:
            df_ex = parse_extras_for_paths(din_paths)
            df_ex.index = snap.loc[din_mask].index
            for c in EXTRA_FIELDS:
                if c not in snap.columns:
                    snap[c] = None
            for c in df_ex.columns:
                snap.loc[din_mask, c] = df_ex[c].values
    else:
        for c in EXTRA_FIELDS:
            if c not in snap.columns:
                snap[c] = None

    # Tipos numéricos
    for c in ["Sumergencia", "PB", "NM", "NC", "ND", "PE", "%Estructura", "%Balance", "Bba Llenado", "Caudal bruto efec"]:
        if c not in snap.columns:
            snap[c] = None
        snap[c] = pd.to_numeric(snap[c], errors="coerce")

    snap["Dias_desde_ultima"] = (pd.Timestamp.now() - snap["DT_plot"]).dt.total_seconds() / 86400.0

    # Merge con coords para Batería
    coords = load_coords_repo()
    if not coords.empty and "nombre_corto" in coords.columns and "nivel_5" in coords.columns:
        from utils.helpers import normalize_no_exact
        c2 = coords[["nombre_corto", "nivel_5"]].copy()
        c2["NO_key"] = c2["nombre_corto"].apply(normalize_no_exact)
        c2 = c2.drop_duplicates(subset=["NO_key"])
        snap = snap.merge(c2[["NO_key", "nivel_5"]].rename(columns={"nivel_5": "Bateria"}), on="NO_key", how="left")
    else:
        snap["Bateria"] = None

    return snap, df_all, din_ok


def _fix_range(vmin, vmax, pad=1.0):
    if vmin == vmax:
        return vmin - pad, vmax + pad
    return vmin, vmax


def layout():
    snap, df_all, _ = _get_data()

    if snap.empty:
        return dbc.Alert("No hay datos. Verificá DINAS_BUCKET.", color="warning")

    def _r(col):
        if col not in snap.columns or snap[col].dropna().empty:
            return 0.0, 1.0
        mn, mx = float(snap[col].min()), float(snap[col].max())
        return _fix_range(mn, mx)

    s_min, s_max   = _r("Sumergencia")
    e_min, e_max   = _r("%Estructura")
    b_min, b_max   = _r("%Balance")
    origenes       = sorted(snap["ORIGEN"].dropna().unique().tolist()) if "ORIGEN" in snap.columns else []

    return html.Div([
        html.H4("📊 Estadísticas (última medición por pozo)"),

        # ── Filtros snapshot ──
        dbc.Row([
            dbc.Col([dbc.Label("Origen (snapshot)"),
                     dcc.Dropdown(id="est-origen", options=[{"label":o,"value":o} for o in origenes],
                                  value=origenes, multi=True, style={"color":"#000"})], md=3),
            dbc.Col([dbc.Label("Rango Sumergencia (snapshot)"),
                     dcc.RangeSlider(id="est-sum-range", min=s_min, max=s_max,
                                     value=[s_min,s_max], marks=None, tooltip={"placement":"bottom"})], md=3),
            dbc.Col([dbc.Label("Rango %Estructura (DIN-only)"),
                     dcc.RangeSlider(id="est-est-range", min=e_min, max=e_max,
                                     value=[e_min,e_max], marks=None, tooltip={"placement":"bottom"})], md=3),
            dbc.Col([dbc.Label("Rango %Balance (DIN-only)"),
                     dcc.RangeSlider(id="est-bal-range", min=b_min, max=b_max,
                                     value=[b_min,b_max], marks=None, tooltip={"placement":"bottom"})], md=3),
        ], className="mb-3"),

        # KPIs
        html.Div(id="est-kpis"),
        html.Hr(),

        # ── Tabla snapshot ──
        html.H5("📋 Pozos (última medición) — filtrados"),
        dcc.Loading(html.Div(id="est-tabla-snap"), type="circle"),
        html.Hr(),

        # ── Gráficos snapshot ──
        html.H5("📈 Gráficos (snapshot, DIN+NIV mezclados)"),
        dbc.Row([
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-origen"),     type="circle"), md=6),
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-antiguedad"), type="circle"), md=6),
        ]),
        dbc.Row([
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-sumer"),      type="circle"), md=6),
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-pb"),         type="circle"), md=6),
        ]),
        html.Hr(),

        # ── DIN-only ──
        html.H5("🧰 DIN-only (%Estructura / %Balance)"),
        dbc.Row([
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-eb"), type="circle"), md=6),
            dbc.Col(dcc.Loading(html.Div(id="est-tabla-eb"),         type="circle"), md=6),
        ]),
        html.Hr(),

        # ── Pozos por mes ──
        html.H5("🛢️ Pozos medidos por mes (nunique) — tabla"),
        dcc.Loading(html.Div(id="est-pozos-mes"), type="circle"),
        html.Hr(),

        # ── Cobertura DIN vs NIV ──
        html.H5("✅ Cobertura DIN vs NIV (histórico) — con filtro por fecha"),
        dbc.Row([
            dbc.Col([dbc.Label("Modo"),
                     dcc.Dropdown(id="est-cov-modo",
                                  options=[{"label":"Última por pozo (snapshot)","value":"snap"},
                                           {"label":"Todas las mediciones (histórico)","value":"hist"}],
                                  value="hist", clearable=False, style={"color":"#000"})], md=3),
            dbc.Col([dbc.Label("Desde"), dcc.DatePickerSingle(id="est-cov-from", display_format="YYYY-MM-DD")], md=3),
            dbc.Col([dbc.Label("Hasta"), dcc.DatePickerSingle(id="est-cov-to",   display_format="YYYY-MM-DD")], md=3),
        ], className="mb-2"),
        dcc.Loading(html.Div(id="est-cobertura"), type="circle"),
        html.Hr(),

        # ── Calidad del dato ──
        html.H5("🧪 Calidad del dato (snapshot filtrado)"),
        dcc.Loading(html.Div(id="est-calidad"), type="circle"),
        html.Hr(),

        # ── Tendencia ──
        html.H5("📈 Pozos con tendencia en aumento"),
        dbc.Row([
            dbc.Col([dbc.Label("Variable"),
                     dcc.Dropdown(id="est-trend-var",
                                  options=[{"label":v,"value":v} for v in
                                           ["Sumergencia","PB","NM","NC","ND","%Estructura","%Balance","GPM","Caudal bruto efec"]],
                                  value="Sumergencia", clearable=False, style={"color":"#000"})], md=3),
            dbc.Col([dbc.Label("Mín. puntos"),
                     dcc.Slider(id="est-trend-minpts", min=2, max=20, step=1, value=4,
                                marks=None, tooltip={"placement":"bottom"})], md=3),
            dbc.Col([dbc.Label("Solo pendiente positiva"),
                     dbc.Switch(id="est-trend-only-up", value=True, label="Sí")], md=3),
        ], className="mb-2"),
        dbc.Row([
            dbc.Col(dcc.Loading(html.Div(id="est-trend-tabla"), type="circle"), md=7),
            dbc.Col(dcc.Loading(dcc.Graph(id="est-trend-graf"),  type="circle"), md=5),
        ]),
        html.Hr(),

        # ── Semáforo AIB ──
        html.H2("🚦 Semáforo AIB (SE = AIB) — independiente de filtros"),
        dbc.Row([
            dbc.Col([dbc.Label("Origen (AIB)"),
                     dcc.Dropdown(id="aib-origen", options=[{"label":o,"value":o} for o in origenes],
                                  value=origenes, multi=True, style={"color":"#000"})], md=3),
            dbc.Col([dbc.Label("Solo SE = AIB"),
                     dbc.Switch(id="aib-only-se", value=True, label="Sí")], md=2),
            dbc.Col([dbc.Label("Solo con Bba Llenado"),
                     dbc.Switch(id="aib-only-llen", value=False, label="Sí")], md=2),
        ], className="mb-2"),
        html.H6("⚙️ Umbrales Semáforo AIB"),
        dbc.Row([
            dbc.Col([dbc.Label("Umbral Sumer. media (m)"),
                     dbc.Input(id="aib-sum-media", type="number", value=200, step=10)], md=3),
            dbc.Col([dbc.Label("Umbral Sumer. alta (m)"),
                     dbc.Input(id="aib-sum-alta",  type="number", value=250, step=10)], md=3),
            dbc.Col([dbc.Label("Llenado OK (≥ %)"),
                     dbc.Input(id="aib-llen-ok",   type="number", value=70,  step=5)],  md=3),
            dbc.Col([dbc.Label("Llenado bajo (< %)"),
                     dbc.Input(id="aib-llen-bajo",  type="number", value=50,  step=5)],  md=3),
        ], className="mb-2"),
        dcc.Loading(html.Div(id="aib-resultado"), type="circle"),
    ])


# ─────────────────────────────────────────────────────────────────────
# Callback principal: snapshot filtrado → KPIs + tabla + gráficos
# ─────────────────────────────────────────────────────────────────────
@callback(
    Output("est-kpis",           "children"),
    Output("est-tabla-snap",     "children"),
    Output("est-graf-origen",    "figure"),
    Output("est-graf-antiguedad","figure"),
    Output("est-graf-sumer",     "figure"),
    Output("est-graf-pb",        "figure"),
    Output("est-graf-eb",        "figure"),
    Output("est-tabla-eb",       "children"),
    Output("est-pozos-mes",      "children"),
    Output("est-calidad",        "children"),
    Input("est-origen",    "value"),
    Input("est-sum-range", "value"),
    Input("est-est-range", "value"),
    Input("est-bal-range", "value"),
)
def update_snap(origenes, sum_range, est_range, bal_range):
    empty = go.Figure(layout={"template":"plotly_dark"})
    snap, df_all, _ = _get_data()

    if snap.empty:
        msg = dbc.Alert("No se encontraron datos.", color="warning")
        return msg, msg, empty, empty, empty, empty, empty, msg, msg, msg

    s = snap.copy()
    if origenes:
        s = s[s["ORIGEN"].isin(origenes)]
    if sum_range and "Sumergencia" in s.columns:
        s = s[s["Sumergencia"].isna() | s["Sumergencia"].between(*sum_range)]
    if est_range and "%Estructura" in s.columns:
        s = s[s["%Estructura"].isna() | s["%Estructura"].between(*est_range)]
    if bal_range and "%Balance" in s.columns:
        s = s[s["%Balance"].isna() | s["%Balance"].between(*bal_range)]

    # KPIs
    kpis = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Pozos snapshot"),    html.H4(f"{len(s):,}".replace(",","."))])), md=2),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Última = DIN"),      html.H4(f"{(s.get('ORIGEN',pd.Series())=='DIN').sum():,}".replace(",","."))])), md=2),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Última = NIV"),      html.H4(f"{(s.get('ORIGEN',pd.Series())=='NIV').sum():,}".replace(",","."))])), md=2),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Con Sumergencia"),   html.H4(f"{s['Sumergencia'].notna().sum() if 'Sumergencia' in s else 0:,}".replace(",","."))])), md=2),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Con PB"),            html.H4(f"{s['PB'].notna().sum() if 'PB' in s else 0:,}".replace(",","."))])), md=2),
    ], className="mb-3")

    # Tabla snapshot
    cols_snap = [c for c in [
        "NO_key","pozo","Bateria","Tipo AIB","ORIGEN","SE","DT_plot","Dias_desde_ultima",
        "PE","PB","NM","NC","ND","Sumergencia","Sumergencia_base",
        "AIB Carrera","Sentido giro","Tipo Contrapesos","Distancia contrapesos (cm)",
        "Contrapeso actual","Contrapeso ideal","AIBEB_Torque max contrapeso",
        "Bba Diam Pistón","Bba Llenado","GPM","Caudal bruto efec",
        "Polea Motor","Potencia Motor","RPM Motor","%Estructura","%Balance",
    ] if c in s.columns]
    df_show = s[cols_snap].sort_values(["Dias_desde_ultima"], na_position="last") if "Dias_desde_ultima" in s.columns else s[cols_snap]
    tabla_snap = dash_table.DataTable(
        data=df_show.pipe(_df_to_table),
        columns=[{"name":c,"id":c} for c in df_show.columns],
        page_size=15, style_table={"overflowX":"auto"},
        style_cell={"fontSize":"12px","padding":"4px"},
        style_header={"fontWeight":"bold","backgroundColor":"#2c2c2c","color":"white"},
        filter_action="native", sort_action="native",
    )

    # Gráficos snapshot
    fig_or  = px.bar(s.groupby("ORIGEN").size().reset_index(name="Pozos"),
                     x="ORIGEN", y="Pozos", title="Pozos por ORIGEN (snapshot)", template="plotly_dark") \
              if "ORIGEN" in s.columns and not s.empty else empty
    fig_age = px.histogram(s.dropna(subset=["Dias_desde_ultima"]), x="Dias_desde_ultima", nbins=30,
                           title="Antigüedad de última medición (días)", template="plotly_dark") \
              if "Dias_desde_ultima" in s.columns and s["Dias_desde_ultima"].notna().any() else empty
    fig_s   = px.histogram(s.dropna(subset=["Sumergencia"]), x="Sumergencia", nbins=30,
                           title="Distribución de Sumergencia (snapshot)", template="plotly_dark") \
              if "Sumergencia" in s.columns and s["Sumergencia"].notna().any() else empty
    fig_pb  = px.histogram(s.dropna(subset=["PB"]) if "PB" in s.columns else pd.DataFrame(),
                           x="PB", nbins=30, title="Distribución de PB (snapshot)", template="plotly_dark") \
              if "PB" in s.columns and s["PB"].notna().any() else empty

    # DIN-only %Estructura vs %Balance
    eb = s.dropna(subset=["%Estructura","%Balance"]).copy() if ("%Estructura" in s.columns and "%Balance" in s.columns) else pd.DataFrame()
    fig_eb = px.scatter(eb, x="%Estructura", y="%Balance", hover_name="NO_key",
                        title="%Estructura vs %Balance (snapshot, DIN-only)", template="plotly_dark") \
             if not eb.empty else empty
    tabla_eb_div = dash_table.DataTable(
        data=eb[["NO_key","ORIGEN","%Estructura","%Balance"]].sort_values("%Estructura",na_position="last").pipe(_df_to_table),
        columns=[{"name":c,"id":c} for c in ["NO_key","ORIGEN","%Estructura","%Balance"]],
        page_size=12, style_table={"overflowX":"auto"},
        style_cell={"fontSize":"12px","padding":"4px"},
        style_header={"fontWeight":"bold"},
    ) if not eb.empty else html.P("No hay %Estructura/%Balance suficiente (suelen venir solo de DIN).")

    # Pozos por mes
    pozos_mes_div = html.P("Sin fechas para armar pozos por mes.")
    if not df_all.empty:
        dm = df_all.copy()
        dm["DT_plot"] = pd.to_datetime(dm["DT_plot"], errors="coerce")
        dm = dm.dropna(subset=["DT_plot"])
        dm["Mes"] = dm["DT_plot"].dt.to_period("M").astype(str)
        p_counts = dm.groupby("Mes")["NO_key"].nunique().reset_index(name="Pozos_medidos").sort_values("Mes")
        if not p_counts.empty:
            last = p_counts.tail(1)
            pozos_mes_div = html.Div([
                html.P(f"📌 Último mes ({last['Mes'].values[0]}): {int(last['Pozos_medidos'].values[0])} pozos medidos",
                       style={"fontWeight":"bold"}),
                dash_table.DataTable(
                    data=p_counts.to_dict("records"),
                    columns=[{"name":c,"id":c} for c in p_counts.columns],
                    page_size=15, style_table={"overflowX":"auto"},
                    style_cell={"fontSize":"12px","padding":"4px"},
                    style_header={"fontWeight":"bold"},
                )
            ])

    # Calidad del dato
    bad_sum = s[(s["Sumergencia"].notna()) & (s["Sumergencia"] < 0)].copy() if "Sumergencia" in s.columns else pd.DataFrame()
    pb_nn = s["PB"].dropna() if "PB" in s.columns else pd.Series(dtype=float)
    bad_pb_count = 0
    if len(pb_nn) >= 10:
        q1v, q3v = pb_nn.quantile(0.25), pb_nn.quantile(0.75)
        iqr = q3v - q1v
        bad_pb = s[s["PB"].notna() & ((s["PB"] < q1v-1.5*iqr) | (s["PB"] > q3v+1.5*iqr))].copy()
        bad_pb_count = len(bad_pb)
    else:
        bad_pb = pd.DataFrame()

    calidad_div = html.Div([
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Sumergencia < 0"), html.H4(len(bad_sum))])), md=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("PB anómalo (IQR)"), html.H4(bad_pb_count)])), md=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("PB faltante"), html.H4(s["PB"].isna().sum() if "PB" in s else 0)])), md=3),
        ], className="mb-2"),
        html.Details([
            html.Summary(f"Ver pozos con Sumergencia < 0 ({len(bad_sum)})"),
            dash_table.DataTable(
                data=bad_sum[[c for c in ["NO_key","ORIGEN","DT_plot","PB","NM","NC","ND","Sumergencia","Sumergencia_base"] if c in bad_sum.columns]].pipe(_df_to_table),
                columns=[{"name":c,"id":c} for c in [c for c in ["NO_key","ORIGEN","DT_plot","PB","NM","NC","ND","Sumergencia","Sumergencia_base"] if c in bad_sum.columns]],
                page_size=10, style_table={"overflowX":"auto"}, style_cell={"fontSize":"11px"},
            ) if not bad_sum.empty else html.P("No hay pozos con Sumergencia < 0.")
        ]) if not bad_sum.empty else html.P("✅ No se detectaron pozos con Sumergencia < 0."),
    ])

    return kpis, tabla_snap, fig_or, fig_age, fig_s, fig_pb, fig_eb, tabla_eb_div, pozos_mes_div, calidad_div


# ─────────────────────────────────────────────────────────────────────
# Callback cobertura DIN vs NIV
# ─────────────────────────────────────────────────────────────────────
@callback(
    Output("est-cobertura", "children"),
    Input("est-cov-modo",  "value"),
    Input("est-cov-from",  "date"),
    Input("est-cov-to",    "date"),
)
def update_cobertura(modo, from_date, to_date):
    _, df_all, _ = _get_data()
    if df_all.empty:
        return html.P("Sin datos.")

    dm = df_all.copy()
    dm["DT_plot"] = pd.to_datetime(dm["DT_plot"], errors="coerce")
    dm = dm.dropna(subset=["DT_plot"])

    dmin = dm["DT_plot"].min()
    dmax = dm["DT_plot"].max()
    cov_from = pd.to_datetime(from_date) if from_date else dmin
    cov_to   = pd.to_datetime(to_date)   if to_date   else dmax

    dc = dm[(dm["DT_plot"] >= cov_from) & (dm["DT_plot"] <= cov_to)].copy()

    if modo == "snap":
        dc = dc.sort_values(["NO_key","DT_plot"],na_position="last").groupby("NO_key",as_index=False).tail(1)

    has_din   = set(dc[dc["ORIGEN"]=="DIN"]["NO_key"].dropna().unique())
    all_pozos = set(dc["NO_key"].dropna().unique())
    never_din = sorted(list(all_pozos - has_din))

    return html.Div([
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Pozos en ventana"),     html.H4(len(all_pozos))])), md=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Con DIN en ventana"),   html.H4(len(has_din))])),   md=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Sin DIN en ventana"),   html.H4(len(never_din))])), md=3),
        ], className="mb-2"),
        html.Details([
            html.Summary(f"Ver lista de pozos sin DIN ({len(never_din)})"),
            html.P(", ".join(never_din)) if never_din else html.P("Ninguno."),
        ]),
    ])


# ─────────────────────────────────────────────────────────────────────
# Callback tendencia
# ─────────────────────────────────────────────────────────────────────
@callback(
    Output("est-trend-tabla", "children"),
    Output("est-trend-graf",  "figure"),
    Input("est-trend-var",     "value"),
    Input("est-trend-minpts",  "value"),
    Input("est-trend-only-up", "value"),
)
def update_tendencia(var, min_pts, only_up):
    empty = go.Figure(layout={"template":"plotly_dark"})
    if not var:
        return html.P("Seleccioná variable."), empty

    _, df_all, _ = _get_data()
    if df_all.empty:
        return html.P("Sin datos."), empty

    df = df_all.copy()
    df["DT_plot"] = pd.to_datetime(df["DT_plot"], errors="coerce")
    df = df.dropna(subset=["DT_plot"])

    if var not in df.columns:
        # intentar cargar extras
        if "path" in df.columns:
            df["path_res"] = df["path"].apply(lambda x: resolve_existing_path(x) if pd.notna(x) else None)
            mask = (df.get("ORIGEN","") == "DIN") & df["path_res"].notna()
            paths = df.loc[mask,"path_res"].astype(str).tolist()
            if paths:
                df_ex = parse_extras_for_paths(paths)
                df_ex.index = df.loc[mask].index
                for c in EXTRA_FIELDS:
                    if c not in df.columns: df[c] = None
                for c in df_ex.columns:
                    df.loc[mask,c] = df_ex[c].values

    if var not in df.columns:
        return html.P(f"Variable '{var}' no disponible."), empty

    df[var] = pd.to_numeric(df[var], errors="coerce")

    rows = []
    for no, g in df.groupby("NO_key"):
        res = trend_linear_per_month(g, var)
        if res is None: continue
        slope, y0, y1, npts = res
        if npts < (min_pts or 2): continue
        rows.append({
            "NO_key": no, "n_puntos": npts,
            "pendiente_por_mes": slope, "valor_inicial": y0, "valor_final": y1,
            "delta_total": y1-y0,
            "fecha_inicial": str(g["DT_plot"].min())[:10],
            "fecha_final":   str(g["DT_plot"].max())[:10],
        })

    df_tr = pd.DataFrame(rows)
    if df_tr.empty:
        return html.P("No hay pozos con suficientes puntos."), empty

    if only_up:
        df_tr = df_tr[df_tr["pendiente_por_mes"] > 0]
    df_tr = df_tr.sort_values("pendiente_por_mes", ascending=False)

    tabla = dash_table.DataTable(
        data=df_tr.head(100).round(3).pipe(_df_to_table),
        columns=[{"name":c,"id":c} for c in df_tr.columns],
        page_size=15, style_table={"overflowX":"auto"},
        style_cell={"fontSize":"11px","padding":"4px"},
        style_header={"fontWeight":"bold"},
        sort_action="native",
    )

    topn = df_tr.head(30).copy()
    fig = px.bar(topn.sort_values("pendiente_por_mes", ascending=True),
                 x="pendiente_por_mes", y="NO_key", orientation="h",
                 title=f"Top 30 — Pendiente por mes ({var})", template="plotly_dark") \
          if not topn.empty else empty

    return tabla, fig


# ─────────────────────────────────────────────────────────────────────
# Callback Semáforo AIB
# ─────────────────────────────────────────────────────────────────────
def _compute_semaforo(row, sum_media, sum_alta, llen_ok, llen_bajo):
    se = str(row.get("SE","")).strip().upper()
    if se != "AIB":
        return "NO APLICA"
    s    = safe_to_float(row.get("Sumergencia"))
    llen = safe_to_float(row.get("Bba Llenado"))
    if s is None or llen is None:
        return "SIN DATOS"
    if s < sum_media or llen >= llen_ok:
        return "🟢 NORMAL"
    if s > sum_alta and llen < llen_bajo:
        return "🔴 CRÍTICO"
    return "🟡 ALERTA"


@callback(
    Output("aib-resultado", "children"),
    Input("aib-origen",    "value"),
    Input("aib-only-se",   "value"),
    Input("aib-only-llen", "value"),
    Input("aib-sum-media", "value"),
    Input("aib-sum-alta",  "value"),
    Input("aib-llen-ok",   "value"),
    Input("aib-llen-bajo", "value"),
)
def update_aib(origenes, only_se, only_llen, sum_media, sum_alta, llen_ok, llen_bajo):
    snap, _, _ = _get_data()
    if snap.empty:
        return html.P("Sin datos.")

    aib = snap.copy()
    if origenes and "ORIGEN" in aib.columns:
        aib = aib[aib["ORIGEN"].isin(origenes)]
    if only_se and "SE" in aib.columns:
        aib = aib[aib["SE"].astype(str).str.strip().str.upper() == "AIB"]
    if only_llen and "Bba Llenado" in aib.columns:
        aib = aib[aib["Bba Llenado"].notna()]

    sm = float(sum_media or 200)
    sa = float(sum_alta  or 250)
    lo = float(llen_ok   or 70)
    lb = float(llen_bajo or 50)

    aib["Semaforo_AIB"] = aib.apply(lambda r: _compute_semaforo(r,sm,sa,lo,lb), axis=1)

    aib_total  = (aib.get("SE",pd.Series()).astype(str).str.upper()=="AIB").sum()
    aib_ok     = (aib["Semaforo_AIB"]=="🟢 NORMAL").sum()
    aib_alerta = (aib["Semaforo_AIB"]=="🟡 ALERTA").sum()
    aib_crit   = (aib["Semaforo_AIB"]=="🔴 CRÍTICO").sum()
    aib_sd     = (aib["Semaforo_AIB"]=="SIN DATOS").sum()

    crit = aib[aib["Semaforo_AIB"]=="🔴 CRÍTICO"].copy()

    cols_aib = [c for c in ["NO_key","pozo","ORIGEN","DT_plot","Dias_desde_ultima","SE",
                             "PB","Sumergencia","Bba Llenado","Sumergencia_base",
                             "%Estructura","%Balance","GPM","Caudal bruto efec","Semaforo_AIB"]
                if c in aib.columns]

    return html.Div([
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Pozos AIB"),    html.H4(int(aib_total))])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("🟢 Normal"),    html.H4(int(aib_ok))])),    md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("🟡 Alerta"),    html.H4(int(aib_alerta))])),md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("🔴 Crítico"),   html.H4(int(aib_crit))])),  md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Sin datos"),    html.H4(int(aib_sd))])),     md=2),
        ], className="mb-3"),
        html.H5("🔴 AIB Crítico — prioridad") if not crit.empty else html.Span(),
        dash_table.DataTable(
            data=crit[cols_aib].sort_values(["Sumergencia","Bba Llenado"] if "Bba Llenado" in crit.columns else ["Sumergencia"],
                                            ascending=[False,True] if "Bba Llenado" in crit.columns else [False],
                                            na_position="last").pipe(_df_to_table),
            columns=[{"name":c,"id":c} for c in cols_aib],
            page_size=10, style_table={"overflowX":"auto"},
            style_cell={"fontSize":"11px","padding":"4px"},
            style_header={"fontWeight":"bold"},
        ) if not crit.empty else html.P("No hay pozos en 🔴 CRÍTICO con los umbrales actuales."),
        html.H5("📋 Semáforo AIB — tabla completa"),
        dash_table.DataTable(
            data=aib[cols_aib].sort_values(["Semaforo_AIB","Dias_desde_ultima"] if "Dias_desde_ultima" in aib.columns else ["Semaforo_AIB"],
                                            na_position="last").pipe(_df_to_table),
            columns=[{"name":c,"id":c} for c in cols_aib],
            page_size=20, style_table={"overflowX":"auto"},
            style_cell={"fontSize":"11px","padding":"4px"},
            style_header={"fontWeight":"bold"},
            sort_action="native", filter_action="native",
        ) if not aib.empty else html.P("Sin datos para Semáforo AIB."),
    ])
