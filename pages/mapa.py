# ==========================================================
# pages/mapa.py — Tab 3: Mapa de sumergencia
# ==========================================================

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

from data.loaders     import load_din_index, load_niv_index, load_coords_repo, resolve_existing_path, GCS_BUCKET, GCS_PREFIX
from data.consolidado import build_last_snapshot_for_map
from utils.helpers    import find_col, build_keys, normalize_no_exact, safe_to_float

dash.register_page(__name__, path="/mapa", name="Mapa", order=3)


def _load_snap():
    df_din = load_din_index()
    df_niv = load_niv_index()
    din_no = find_col(df_din, ["pozo","NO"])
    din_fe = find_col(df_din, ["fecha","FE"])
    din_ho = find_col(df_din, ["hora","HO"])
    niv_no = find_col(df_niv, ["pozo","NO"])
    niv_fe = find_col(df_niv, ["fecha","FE"])
    niv_ho = find_col(df_niv, ["hora","HO"])
    din_k = build_keys(df_din, din_no, din_fe, din_ho) if (not df_din.empty and din_no and din_fe) else pd.DataFrame()
    niv_k = build_keys(df_niv, niv_no, niv_fe, niv_ho) if (not df_niv.empty and niv_no and niv_fe) else pd.DataFrame()
    din_ok = din_k[din_k["error"].isna()] if (not din_k.empty and "error" in din_k.columns) else din_k
    niv_ok = niv_k[niv_k["error"].isna()] if (not niv_k.empty and "error" in niv_k.columns) else niv_k
    if not din_ok.empty and "path" in din_ok.columns:
        din_ok["path"] = din_ok["path"].apply(lambda x: resolve_existing_path(x) if pd.notna(x) else None)
    return build_last_snapshot_for_map(din_ok, niv_ok)


def layout():
    return html.Div([
        html.H4("🗺️ Mapa de sumergencia — última medición por pozo"),

        dbc.Row([
            dbc.Col([
                dbc.Label("Batería (nivel_5)"),
                dcc.Dropdown(id="mapa-bat", multi=True, style={"color":"#000"}),
            ], md=4),
            dbc.Col([
                dbc.Label("Rango Sumergencia"),
                dcc.RangeSlider(id="mapa-sum", marks=None, tooltip={"placement":"bottom"}),
            ], md=4),
            dbc.Col([
                dbc.Label("Días desde última medición"),
                dcc.RangeSlider(id="mapa-dias", marks=None, tooltip={"placement":"bottom"}),
            ], md=4),
        ], className="mb-3"),

        dcc.Loading(dcc.Graph(id="mapa-graf", style={"height": "600px"}), type="circle"),

        html.Hr(),
        html.H5("📋 Pozos filtrados"),
        dcc.Loading(html.Div(id="mapa-tabla"), type="circle"),

        dcc.Interval(id="mapa-init", interval=1, max_intervals=1),
        dcc.Store(id="mapa-store"),
    ])


@callback(
    Output("mapa-bat",  "options"),
    Output("mapa-bat",  "value"),
    Output("mapa-sum",  "min"),
    Output("mapa-sum",  "max"),
    Output("mapa-sum",  "value"),
    Output("mapa-dias", "min"),
    Output("mapa-dias", "max"),
    Output("mapa-dias", "value"),
    Output("mapa-store","data"),
    Input("mapa-init",  "n_intervals"),
)
def init_mapa(_):
    snap = _load_snap()
    if snap.empty:
        return [], [], 0, 1, [0,1], 0, 1, [0,1], {}

    # Merge coordenadas
    coords = load_coords_repo()
    if not coords.empty and "nombre_corto" in coords.columns:
        coords["NO_key"] = coords["nombre_corto"].apply(normalize_no_exact)
        snap = snap.merge(
            coords[["NO_key","nombre_corto","nivel_5","GEO_LATITUDE","GEO_LONGITUDE"]],
            on="NO_key", how="left"
        ).rename(columns={"GEO_LATITUDE":"lat","GEO_LONGITUDE":"lon"})
    else:
        snap["lat"] = None
        snap["lon"] = None
        snap["nivel_5"] = None

    snap["lat"] = pd.to_numeric(snap["lat"], errors="coerce")
    snap["lon"] = pd.to_numeric(snap["lon"], errors="coerce")
    snap["Sumergencia"] = pd.to_numeric(snap["Sumergencia"], errors="coerce")
    snap["DT_plot"]     = pd.to_datetime(snap["DT_plot"], errors="coerce")
    now = pd.Timestamp.now()
    snap["Dias_desde_ultima"] = (now - snap["DT_plot"]).dt.total_seconds() / 86400.0

    m = snap.dropna(subset=["lat","lon"])

    bats = sorted(m["nivel_5"].dropna().astype(str).unique().tolist()) if "nivel_5" in m.columns else []
    bat_opts = [{"label": b, "value": b} for b in bats]

    s_ok = m["Sumergencia"].dropna()
    d_ok = m["Dias_desde_ultima"].dropna()

    s_min = float(s_ok.min()) if not s_ok.empty else 0.0
    s_max = float(s_ok.max()) if not s_ok.empty else 1.0
    d_min = float(d_ok.min()) if not d_ok.empty else 0.0
    d_max = float(d_ok.max()) if not d_ok.empty else 1.0

    if s_min == s_max: s_min, s_max = s_min-1, s_max+1
    if d_min == d_max: d_min, d_max = d_min-1, d_max+1

    snap["DT_plot_str"] = snap["DT_plot"].dt.strftime("%Y-%m-%d %H:%M")

    return (
        bat_opts, bats,
        s_min, s_max, [s_min, s_max],
        d_min, d_max, [d_min, d_max],
        snap.to_json(date_format="iso"),
    )


@callback(
    Output("mapa-graf",  "figure"),
    Output("mapa-tabla", "children"),
    Input("mapa-bat",    "value"),
    Input("mapa-sum",    "value"),
    Input("mapa-dias",   "value"),
    Input("mapa-store",  "data"),
)
def update_mapa(bats, sum_range, dias_range, snap_json):
    empty_fig = px.scatter_mapbox(pd.DataFrame({"lat":[], "lon":[]}),
                                  lat="lat", lon="lon",
                                  mapbox_style="open-street-map", zoom=8)
    if not snap_json:
        return empty_fig, html.P("Cargando...")

    m = pd.read_json(snap_json)
    m["lat"] = pd.to_numeric(m["lat"], errors="coerce")
    m["lon"] = pd.to_numeric(m["lon"], errors="coerce")
    m["Sumergencia"]      = pd.to_numeric(m["Sumergencia"], errors="coerce")
    m["Dias_desde_ultima"]= pd.to_numeric(m["Dias_desde_ultima"], errors="coerce")

    m = m.dropna(subset=["lat","lon"])

    if bats and "nivel_5" in m.columns:
        m = m[m["nivel_5"].isin(bats)]
    if sum_range:
        m = m[m["Sumergencia"].between(*sum_range, inclusive="both")]
    if dias_range:
        m = m[m["Dias_desde_ultima"].between(*dias_range, inclusive="both")]

    if m.empty:
        return empty_fig, html.P("Sin pozos con los filtros actuales.")

    fig = px.scatter_mapbox(
        m, lat="lat", lon="lon",
        color="Sumergencia",
        color_continuous_scale="RdYlGn_r",
        hover_name="NO_key",
        hover_data={"nivel_5": True, "ORIGEN": True, "Sumergencia": True, "Dias_desde_ultima": ":.0f"},
        zoom=9,
        mapbox_style="open-street-map",
        title=f"Sumergencia (última medición) — {len(m)} pozos",
        template="plotly_dark",
    )

    show_cols = [c for c in ["NO_key","nivel_5","ORIGEN","DT_plot_str","Sumergencia","Dias_desde_ultima","lat","lon"] if c in m.columns]
    tabla = dbc.Table.from_dataframe(
        m[show_cols].sort_values("Sumergencia", ascending=False).head(200).round(2),
        striped=True, bordered=True, hover=True, size="sm", responsive=True,
        style={"fontSize":"0.8rem"},
    )

    return fig, tabla
