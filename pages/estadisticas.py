# ==========================================================
# pages/estadisticas.py — Tab 2: Estadísticas
#
# CLAVE vs Streamlit:
#   - Cada gráfico tiene su propio callback
#   - Los sliders/filtros actualizan solo los componentes afectados
#   - build_global_consolidated() se ejecuta una sola vez (lru_cache)
# ==========================================================

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from data.loaders     import load_din_index, load_niv_index, load_coords_repo, resolve_existing_path
from data.consolidado import build_global_consolidated
from data.parsers     import parse_extras_for_paths, EXTRA_FIELDS
from utils.helpers    import (
    find_col, build_keys, normalize_no_exact, safe_to_float, trend_linear_per_month
)

dash.register_page(__name__, path="/estadisticas", name="Estadísticas", order=2)


def _load_all():
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
        din_ok["path"] = din_ok["path"].apply(lambda x: resolve_existing_path(x) if pd.notna(x) else None)

    df_all = build_global_consolidated(
        din_ok, niv_ok,
        din_no, din_fe, din_ho,
        niv_no, niv_fe, niv_ho,
    )
    return df_all, din_ok


def layout():
    return html.Div([
        html.H4("📊 Estadísticas — última medición por pozo"),

        # Filtros
        dbc.Row([
            dbc.Col([
                dbc.Label("Origen"),
                dcc.Dropdown(id="est-origen", multi=True, style={"color": "#000"}),
            ], md=3),
            dbc.Col([
                dbc.Label("Rango Sumergencia"),
                dcc.RangeSlider(id="est-sum-range", tooltip={"placement": "bottom"}, marks=None),
            ], md=3),
            dbc.Col([
                dbc.Label("Rango %Estructura"),
                dcc.RangeSlider(id="est-est-range", tooltip={"placement": "bottom"}, marks=None),
            ], md=3),
            dbc.Col([
                dbc.Label("Rango %Balance"),
                dcc.RangeSlider(id="est-bal-range", tooltip={"placement": "bottom"}, marks=None),
            ], md=3),
        ], className="mb-4"),

        # KPIs
        dcc.Loading(html.Div(id="est-kpis"), type="circle"),

        html.Hr(),

        # Gráficos principales (cada uno independiente)
        dbc.Row([
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-origen"),    type="circle"), md=6),
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-antiguedad"),type="circle"), md=6),
        ]),
        dbc.Row([
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-sumer"),     type="circle"), md=6),
            dbc.Col(dcc.Loading(dcc.Graph(id="est-graf-pb"),        type="circle"), md=6),
        ]),

        html.Hr(),

        # Pozos por mes
        html.H5("🛢️ Pozos medidos por mes"),
        dcc.Loading(dcc.Graph(id="est-graf-mes"), type="circle"),

        html.Hr(),

        # Tendencia
        html.H5("📈 Tendencia por pozo"),
        dbc.Row([
            dbc.Col([
                dbc.Label("Variable"),
                dcc.Dropdown(
                    id      = "est-trend-var",
                    options = [{"label": v, "value": v} for v in
                               ["Sumergencia","PB","NM","NC","ND","%Estructura","%Balance","GPM","Caudal bruto efec"]],
                    value   = "Sumergencia",
                    style   = {"color": "#000"},
                    clearable=False,
                ),
            ], md=3),
            dbc.Col([
                dbc.Label("Mín. puntos"),
                dcc.Slider(id="est-trend-minpts", min=2, max=20, step=1, value=4, marks=None,
                           tooltip={"placement": "bottom"}),
            ], md=3),
            dbc.Col([
                dbc.Label("Solo pendiente positiva"),
                dbc.Switch(id="est-trend-only-up", value=True, label="Sí"),
            ], md=3),
        ], className="mb-3"),
        dcc.Loading(dcc.Graph(id="est-graf-trend"), type="circle"),

        # Store interno con datos filtrados
        dcc.Store(id="est-store-snap"),
        dcc.Store(id="est-store-all"),

        # Trigger inicial
        dcc.Interval(id="est-init", interval=1, max_intervals=1),
    ])


# ── Callback de inicialización: carga datos y setea rangos de sliders ──
@callback(
    Output("est-origen",    "options"),
    Output("est-origen",    "value"),
    Output("est-sum-range", "min"),
    Output("est-sum-range", "max"),
    Output("est-sum-range", "value"),
    Output("est-est-range", "min"),
    Output("est-est-range", "max"),
    Output("est-est-range", "value"),
    Output("est-bal-range", "min"),
    Output("est-bal-range", "max"),
    Output("est-bal-range", "value"),
    Output("est-store-all", "data"),
    Input("est-init",       "n_intervals"),
)
def init_estadisticas(_):
    df_all, din_ok = _load_all()
    if df_all.empty:
        return [], [], 0, 1, [0,1], 0, 100, [0,100], 0, 100, [0,100], {}

    df_all["DT_plot"] = pd.to_datetime(df_all["DT_plot"], errors="coerce")
    snap = df_all.sort_values(["NO_key","DT_plot"], na_position="last") \
                 .dropna(subset=["DT_plot"]) \
                 .groupby("NO_key", as_index=False).tail(1).copy()

    # Parse extras DIN
    if "path" in snap.columns:
        snap["path_res"] = snap["path"].apply(lambda x: resolve_existing_path(x) if pd.notna(x) else None)
        din_mask  = (snap.get("ORIGEN","") == "DIN") & snap["path_res"].notna()
        din_paths = snap.loc[din_mask, "path_res"].astype(str).tolist()
        if din_paths:
            df_ex = parse_extras_for_paths(din_paths)
            df_ex.index = snap.loc[din_mask].index
            for c in EXTRA_FIELDS:
                if c not in snap.columns:
                    snap[c] = None
            for c in df_ex.columns:
                snap.loc[din_mask, c] = df_ex[c].values

    for c in ["Sumergencia", "%Estructura", "%Balance"]:
        if c not in snap.columns:
            snap[c] = None
        snap[c] = pd.to_numeric(snap[c], errors="coerce")

    def _range(col):
        v = snap[col].dropna()
        if v.empty:
            return 0.0, 1.0
        mn, mx = float(v.min()), float(v.max())
        return (mn - 1, mx + 1) if mn == mx else (mn, mx)

    s_min, s_max   = _range("Sumergencia")
    e_min, e_max   = _range("%Estructura")
    b_min, b_max   = _range("%Balance")

    origenes = sorted(snap["ORIGEN"].dropna().unique().tolist()) if "ORIGEN" in snap.columns else []
    opts     = [{"label": o, "value": o} for o in origenes]

    # Serializar snap para el store
    snap_json = snap.to_json(date_format="iso")

    return (
        opts, origenes,
        s_min, s_max, [s_min, s_max],
        e_min, e_max, [e_min, e_max],
        b_min, b_max, [b_min, b_max],
        snap_json,
    )


# ── Callback principal: filtra snap y actualiza KPIs + gráficos ──
@callback(
    Output("est-kpis",          "children"),
    Output("est-graf-origen",   "figure"),
    Output("est-graf-antiguedad","figure"),
    Output("est-graf-sumer",    "figure"),
    Output("est-graf-pb",       "figure"),
    Output("est-store-snap",    "data"),
    Input("est-origen",         "value"),
    Input("est-sum-range",      "value"),
    Input("est-est-range",      "value"),
    Input("est-bal-range",      "value"),
    Input("est-store-all",      "data"),
)
def update_graficos(origenes, sum_range, est_range, bal_range, snap_json):
    if not snap_json:
        empty = go.Figure()
        return html.P("Cargando..."), empty, empty, empty, empty, {}

    snap = pd.read_json(snap_json)
    snap["DT_plot"] = pd.to_datetime(snap["DT_plot"], errors="coerce")

    # Filtros
    if origenes:
        snap = snap[snap["ORIGEN"].isin(origenes)]
    if sum_range:
        snap = snap[snap["Sumergencia"].isna() | snap["Sumergencia"].between(*sum_range)]
    if est_range:
        snap = snap[snap["%Estructura"].isna() | snap["%Estructura"].between(*est_range)]
    if bal_range:
        snap = snap[snap["%Balance"].isna() | snap["%Balance"].between(*bal_range)]

    now = pd.Timestamp.now()
    snap["Dias_desde_ultima"] = (now - snap["DT_plot"]).dt.total_seconds() / 86400.0

    # KPIs
    kpis = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Pozos"), html.H4(len(snap))])),          md=2),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("DIN"),   html.H4((snap.get("ORIGEN","")=="DIN").sum())])), md=2),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("NIV"),   html.H4((snap.get("ORIGEN","")=="NIV").sum())])), md=2),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Con Sumergencia"), html.H4(snap["Sumergencia"].notna().sum())])), md=2),
        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Con PB"), html.H4(snap["PB"].notna().sum() if "PB" in snap else 0)])), md=2),
    ], className="mb-3")

    # Gráfico origen
    if "ORIGEN" in snap.columns:
        fig_or = px.bar(
            snap.groupby("ORIGEN").size().reset_index(name="Pozos"),
            x="ORIGEN", y="Pozos", title="Pozos por Origen", template="plotly_dark",
        )
    else:
        fig_or = go.Figure()

    # Antigüedad
    fig_age = px.histogram(
        snap.dropna(subset=["Dias_desde_ultima"]),
        x="Dias_desde_ultima", nbins=30,
        title="Antigüedad última medición (días)", template="plotly_dark",
    ) if snap["Dias_desde_ultima"].notna().any() else go.Figure()

    # Sumergencia
    fig_s = px.histogram(
        snap.dropna(subset=["Sumergencia"]),
        x="Sumergencia", nbins=30,
        title="Distribución Sumergencia", template="plotly_dark",
    ) if snap["Sumergencia"].notna().any() else go.Figure()

    # PB
    fig_pb = px.histogram(
        snap.dropna(subset=["PB"]) if "PB" in snap else pd.DataFrame(),
        x="PB", nbins=30,
        title="Distribución PB", template="plotly_dark",
    ) if ("PB" in snap and snap["PB"].notna().any()) else go.Figure()

    return kpis, fig_or, fig_age, fig_s, fig_pb, snap.to_json(date_format="iso")


@callback(
    Output("est-graf-mes",  "figure"),
    Input("est-store-all",  "data"),
)
def update_mes(snap_json):
    if not snap_json:
        return go.Figure()
    df = pd.read_json(snap_json)
    df["DT_plot"] = pd.to_datetime(df["DT_plot"], errors="coerce")
    df = df.dropna(subset=["DT_plot"])
    df["Mes"] = df["DT_plot"].dt.to_period("M").astype(str)
    counts = df.groupby("Mes")["NO_key"].nunique().reset_index(name="Pozos_medidos")
    return px.bar(counts.sort_values("Mes"), x="Mes", y="Pozos_medidos",
                  title="Pozos medidos por mes (nunique)", template="plotly_dark")


@callback(
    Output("est-graf-trend", "figure"),
    Input("est-trend-var",   "value"),
    Input("est-trend-minpts","value"),
    Input("est-trend-only-up","value"),
    Input("est-store-all",   "data"),
)
def update_tendencia(var, min_pts, only_up, snap_json):
    if not snap_json or not var:
        return go.Figure()
    df = pd.read_json(snap_json)
    df["DT_plot"] = pd.to_datetime(df["DT_plot"], errors="coerce")
    df = df.dropna(subset=["DT_plot"])
    if var not in df.columns:
        return go.Figure(layout={"title": f"Variable '{var}' no disponible en este dataset"})
    df[var] = pd.to_numeric(df[var], errors="coerce")

    rows = []
    for no, g in df.groupby("NO_key"):
        res = trend_linear_per_month(g, var)
        if res is None:
            continue
        slope, y0, y1, npts = res
        if npts < (min_pts or 2):
            continue
        rows.append({"NO_key": no, "pendiente_por_mes": slope, "delta_total": y1-y0, "n_puntos": npts})

    df_tr = pd.DataFrame(rows)
    if df_tr.empty:
        return go.Figure(layout={"title": "Sin pozos con suficientes puntos"})
    if only_up:
        df_tr = df_tr[df_tr["pendiente_por_mes"] > 0]
    df_tr = df_tr.sort_values("pendiente_por_mes", ascending=True).head(30)

    return px.bar(
        df_tr, x="pendiente_por_mes", y="NO_key", orientation="h",
        title=f"Top 30 — Pendiente por mes ({var})", template="plotly_dark",
    )
