# ==========================================================
# pages/mapa.py — Tab 3: Mapa de sumergencia
# Migración FIEL de tab_map del app.py Streamlit original.
# ==========================================================

import dash
from dash import html, dcc, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
import io

from data.loaders     import load_din_index, load_niv_index, resolve_existing_path, load_coords_repo
from data.consolidado import build_last_snapshot_for_map
from utils.helpers    import find_col, build_keys, normalize_no_exact
from components.validaciones_logic import (
    load_all_validaciones, save_validaciones, get_validacion,
    set_validacion, make_fecha_key, filtrar_por_validacion,
)

dash.register_page(__name__, path="/mapa", name="Mapa", order=3)

def _df_to_table(df: pd.DataFrame) -> list[dict]:
    """
    Convierte un DataFrame a lista de dicts para Dash DataTable.
    Maneja StringDtype, BooleanDtype, Int64Dtype y otros extension types de Pandas
    que to_dict("records") no serializa bien (devuelven None/pd.NA en vez de "").
    """
    d = df.copy()
    # Paso 1: convertir extension types a object nativo
    for col in d.columns:
        dtype = d[col].dtype
        if isinstance(dtype, (pd.StringDtype, pd.BooleanDtype,
                               pd.Int8Dtype,  pd.Int16Dtype,
                               pd.Int32Dtype, pd.Int64Dtype,
                               pd.UInt8Dtype, pd.UInt16Dtype,
                               pd.UInt32Dtype, pd.UInt64Dtype,
                               pd.Float32Dtype, pd.Float64Dtype)):
            d[col] = d[col].astype(object)
    # Paso 2: serializar por tipo
    for col in d.columns:
        if pd.api.types.is_datetime64_any_dtype(d[col]):
            d[col] = d[col].dt.strftime("%Y-%m-%d %H:%M").where(d[col].notna(), "")
        elif pd.api.types.is_float_dtype(d[col]):
            d[col] = d[col].apply(lambda x: round(x, 2) if pd.notna(x) else "")
        elif pd.api.types.is_integer_dtype(d[col]):
            d[col] = d[col].apply(lambda x: int(x) if pd.notna(x) else "")
        else:
            def _safe_str(x):
                try:
                    return "" if pd.isna(x) else (x if isinstance(x, str) else str(x))
                except Exception:
                    return str(x) if x is not None else ""
            d[col] = d[col].apply(_safe_str)
    return d.to_dict("records")


GCS_BUCKET = __import__("os").environ.get("DINAS_BUCKET", "").strip()
GCS_PREFIX = __import__("os").environ.get("DINAS_GCS_PREFIX", "").strip().strip("/")


def _get_snap_map():
    df_din = load_din_index()
    df_niv = load_niv_index()
    din_no = find_col(df_din, ["pozo","NO"])
    din_fe = find_col(df_din, ["fecha","FE"])
    din_ho = find_col(df_din, ["hora","HO"])
    niv_no = find_col(df_niv, ["pozo","NO"])
    niv_fe = find_col(df_niv, ["fecha","FE"])
    niv_ho = find_col(df_niv, ["hora","HO"])
    din_k  = build_keys(df_din, din_no, din_fe, din_ho) if (not df_din.empty and din_no and din_fe) else pd.DataFrame()
    niv_k  = build_keys(df_niv, niv_no, niv_fe, niv_ho) if (not df_niv.empty and niv_no and niv_fe) else pd.DataFrame()
    din_ok = din_k[din_k["error"].isna()] if (not din_k.empty and "error" in din_k.columns) else din_k
    niv_ok = niv_k[niv_k["error"].isna()] if (not niv_k.empty and "error" in niv_k.columns) else niv_k
    if not din_ok.empty and "path" in din_ok.columns:
        din_ok = din_ok.copy()
        din_ok["path"] = din_ok["path"].apply(lambda x: resolve_existing_path(x) if pd.notna(x) else None)
    return build_last_snapshot_for_map(din_ok, niv_ok)


def _merge_coords(snap_map):
    coords = load_coords_repo()
    if coords.empty:
        snap_map["lat"] = None
        snap_map["lon"] = None
        snap_map["nivel_5"] = None
        return snap_map
    coords = coords.copy()
    coords["NO_key"] = coords["nombre_corto"].apply(normalize_no_exact)
    snap_map["NO_key"] = snap_map["NO_key"].apply(normalize_no_exact)
    m = snap_map.merge(
        coords[["NO_key","nombre_corto","nivel_5","GEO_LATITUDE","GEO_LONGITUDE"]],
        on="NO_key", how="left"
    ).rename(columns={"GEO_LATITUDE":"lat","GEO_LONGITUDE":"lon"})
    m["lat"] = pd.to_numeric(m["lat"], errors="coerce")
    m["lon"] = pd.to_numeric(m["lon"], errors="coerce")
    if "nivel_5" in m.columns:
        m["nivel_5"] = m["nivel_5"].astype("string").str.strip()
    return m


def layout():
    snap = _get_snap_map()
    if snap.empty:
        return dbc.Alert("No hay datos para el mapa.", color="warning")

    snap["DT_plot"] = pd.to_datetime(snap["DT_plot"], errors="coerce")
    snap = snap.dropna(subset=["DT_plot"])
    snap["Dias_desde_ultima"] = (pd.Timestamp.now() - snap["DT_plot"]).dt.total_seconds() / 86400.0
    snap["Sumergencia"] = pd.to_numeric(snap.get("Sumergencia"), errors="coerce")
    m = _merge_coords(snap)
    m = m[m["lat"].notna() & m["lon"].notna()].copy()

    batt_opts = sorted(m["nivel_5"].dropna().astype(str).unique().tolist()) if "nivel_5" in m.columns else []
    s_ok = m["Sumergencia"].dropna()
    smin = float(s_ok.min()) if not s_ok.empty else 0.0
    smax = float(s_ok.max()) if not s_ok.empty else 1.0
    if smin == smax: smin, smax = smin-1, smax+1
    d_ok = m["Dias_desde_ultima"].dropna()
    dmin = float(d_ok.min()) if not d_ok.empty else 0.0
    dmax = float(d_ok.max()) if not d_ok.empty else 1.0
    if dmin == dmax: dmin, dmax = dmin-1, dmax+1

    return html.Div([
        html.H4("🗺️ Mapa de sumergencia (heatmap densidad — última medición por pozo)"),

        html.H5("Filtros"),
        dbc.Row([
            dbc.Col([dbc.Label("Batería (nivel_5)"),
                     dcc.Dropdown(id="map-batt", options=[{"label":b,"value":b} for b in batt_opts],
                                  value=batt_opts, multi=True, style={"color":"#000"})], md=5),
            dbc.Col([dbc.Label("Rango Sumergencia"),
                     dcc.RangeSlider(id="map-sum-range", min=smin, max=smax, value=[smin,smax],
                                     marks=None, tooltip={"placement":"bottom"})], md=4),
            dbc.Col([dbc.Label("Días desde última medición"),
                     dcc.RangeSlider(id="map-dias-range", min=dmin, max=dmax, value=[dmin,dmax],
                                     marks=None, tooltip={"placement":"bottom"}, step=0.1)], md=3),
        ], className="mb-2"),

        dbc.Row([
            dbc.Col([dbc.Label("Filtrar por validación"),
                     dbc.RadioItems(id="map-filtro-val",
                                    options=[{"label":"Todos","value":"todos"},
                                             {"label":"Solo validadas","value":"validadas"},
                                             {"label":"Solo no validadas","value":"no_validadas"}],
                                    value="todos", inline=True)], md=6),
        ], className="mb-3"),

        dcc.Loading(dcc.Graph(id="map-mapa", style={"height":"500px"}), type="circle"),
        html.Div(id="map-caption"),

        html.Hr(),
        html.H5("📋 Pozos filtrados (selección, validación y exportación)"),
        dcc.Loading(html.Div(id="map-tabla-div"), type="circle"),

        html.Hr(),
        html.H5("💬 Agregar / editar comentario"),
        dbc.Row([
            dbc.Col([dbc.Label("Pozo"), dcc.Dropdown(id="map-val-pozo", style={"color":"#000"})], md=3),
            dbc.Col([dbc.Label("Fecha"), dcc.Dropdown(id="map-val-fecha", style={"color":"#000"})], md=3),
        ], className="mb-2"),
        dbc.Row([
            dbc.Col([dbc.Label("✅ Válida"), dbc.Switch(id="map-val-check", value=True, label="Sí")], md=2),
            dbc.Col([dbc.Label("Comentario"), dbc.Input(id="map-val-comentario", type="text", placeholder="Comentario...")], md=5),
            dbc.Col([dbc.Label("Tu nombre"), dbc.Input(id="map-val-usuario", type="text", placeholder="ej: jperez")], md=3),
        ], className="mb-2"),
        dbc.Button("💾 Guardar comentario", id="map-val-guardar", color="primary", className="mb-3"),
        html.Div(id="map-val-msg"),

        dcc.Store(id="map-store-data"),
    ])


@callback(
    Output("map-mapa",       "figure"),
    Output("map-caption",    "children"),
    Output("map-tabla-div",  "children"),
    Output("map-val-pozo",   "options"),
    Output("map-val-pozo",   "value"),
    Output("map-store-data", "data"),
    Input("map-batt",        "value"),
    Input("map-sum-range",   "value"),
    Input("map-dias-range",  "value"),
    Input("map-filtro-val",  "value"),
)
def update_mapa(batt_sel, sum_range, dias_range, filtro_val):
    empty_fig = go.Figure(layout={"template":"plotly_dark"})

    snap = _get_snap_map()
    if snap.empty:
        return empty_fig, "Sin datos.", html.P("Sin datos."), [], None, None

    snap["DT_plot"] = pd.to_datetime(snap["DT_plot"], errors="coerce")
    snap = snap.dropna(subset=["DT_plot"])
    snap["Dias_desde_ultima"] = (pd.Timestamp.now() - snap["DT_plot"]).dt.total_seconds() / 86400.0
    snap["Sumergencia"] = pd.to_numeric(snap.get("Sumergencia"), errors="coerce")

    m = _merge_coords(snap)
    m = m[m["lat"].notna() & m["lon"].notna()].copy()

    # Filtro batería
    if batt_sel and "nivel_5" in m.columns:
        m = m[m["nivel_5"].isin(batt_sel)].copy()

    # Filtro validación
    if filtro_val != "todos":
        solo_val = (filtro_val == "validadas")
        m = filtrar_por_validacion(m, GCS_BUCKET, GCS_PREFIX, normalize_no_exact, solo_validadas=solo_val)
        if m.empty:
            return empty_fig, f"No hay pozos '{filtro_val}'.", html.P("Sin pozos."), [], None, None

    # Filtro sumergencia
    if sum_range:
        m = m[m["Sumergencia"].isna() | m["Sumergencia"].between(*sum_range)]

    # Filtro días
    if dias_range:
        before = len(m)
        mask = m["Dias_desde_ultima"].between(dias_range[0], dias_range[1], inclusive="both")
        m = m[mask].copy()
        caption = (f"Filtro días: {dias_range[0]:.1f}–{dias_range[1]:.1f} | "
                   f"filas: {before} → {len(m)}")
    else:
        caption = f"Total pozos con coordenadas: {len(m)}"

    if m.empty:
        return empty_fig, "Sin pozos con los filtros seleccionados.", html.P("Sin pozos."), [], None, None

    # Mapa con plotly (reemplazo pydeck)
    m["DT_str"] = m["DT_plot"].dt.strftime("%Y-%m-%d %H:%M")
    fig = px.density_mapbox(
        m.dropna(subset=["Sumergencia"]),
        lat="lat", lon="lon", z="Sumergencia",
        radius=30,
        center={"lat": float(m["lat"].mean()), "lon": float(m["lon"].mean())},
        zoom=8,
        mapbox_style="open-street-map",
        hover_name="NO_key",
        hover_data={"nivel_5":True, "ORIGEN":True, "DT_str":True, "Sumergencia":True, "Dias_desde_ultima":":.1f"},
        title="Heatmap Sumergencia (última medición por pozo)",
    )
    fig.update_layout(height=500, margin={"r":0,"t":40,"l":0,"b":0})

    # Tabla
    show_cols = [c for c in ["NO_key","nivel_5","ORIGEN","DT_plot","Dias_desde_ultima",
                              "Sumergencia","PE","PB","NM","NC","ND","Sumergencia_base","lat","lon"]
                 if c in m.columns]
    t = m[show_cols].sort_values("Sumergencia", ascending=False, na_position="last").reset_index(drop=True)

    # Cargar validaciones para la tabla
    pozos = t["NO_key"].dropna().unique().tolist()
    todas_val = load_all_validaciones(GCS_BUCKET, pozos, GCS_PREFIX) if GCS_BUCKET else {}
    validadas, comentarios, usuarios = [], [], []
    for _, row in t.iterrows():
        nk  = normalize_no_exact(str(row.get("NO_key","")))
        fk  = make_fecha_key(row.get("DT_plot"))
        est = get_validacion(todas_val.get(nk,{}), fk)
        hist = est.get("historial",[])
        validadas.append("✅" if est.get("validada",True) else "❌")
        comentarios.append(est.get("comentario",""))
        usuarios.append(hist[-1].get("usuario","") if hist else "")
    t.insert(0, "Válida", validadas)
    t["Comentario"] = comentarios
    t["Usuario"]    = usuarios

    tabla = html.Div([
        html.P(f"Total: {len(t)} pozos"),
        dash_table.DataTable(
            id="map-tabla",
            data=t.pipe(_df_to_table),
            columns=[{"name":c,"id":c} for c in t.columns],
            page_size=15, style_table={"overflowX":"auto"},
            style_cell={"fontSize":"11px","padding":"4px"},
            style_header={"fontWeight":"bold"},
            sort_action="native", filter_action="native",
        ),
        dbc.Row([
            dbc.Col(html.A("⬇️ Descargar CSV",
                           id="map-dl-csv",
                           download="pozos_sumergencia.csv",
                           href="data:text/csv;charset=utf-8," + t.to_csv(index=False),
                           className="btn btn-sm btn-outline-secondary"), md=3),
        ], className="mt-2"),
    ])

    pozo_opts = [{"label":p,"value":p} for p in sorted(t["NO_key"].dropna().unique().tolist())]
    store_data = t.to_json(date_format="iso", orient="split")

    return fig, caption, tabla, pozo_opts, (pozo_opts[0]["value"] if pozo_opts else None), store_data


@callback(
    Output("map-val-fecha",      "options"),
    Output("map-val-fecha",      "value"),
    Output("map-val-check",      "value"),
    Output("map-val-comentario", "value"),
    Input("map-val-pozo",        "value"),
    State("map-store-data",      "data"),
)
def update_fecha_opts(pozo_sel, store_data):
    if not pozo_sel or not store_data:
        return [], None, True, ""
    try:
        t = pd.read_json(io.StringIO(store_data), orient="split")
    except Exception:
        return [], None, True, ""

    rows = t[t["NO_key"] == pozo_sel]
    if rows.empty:
        return [], None, True, ""

    fechas = rows["DT_plot"].astype(str).unique().tolist()
    opts   = [{"label":f,"value":f} for f in fechas]
    primera = fechas[0] if fechas else None

    nk  = normalize_no_exact(str(pozo_sel))
    todas_val = load_all_validaciones(GCS_BUCKET, [nk], GCS_PREFIX) if GCS_BUCKET else {}
    est = get_validacion(todas_val.get(nk,{}), make_fecha_key(primera)) if primera else {}

    return opts, primera, est.get("validada",True), est.get("comentario","")


@callback(
    Output("map-val-msg", "children"),
    Input("map-val-guardar", "n_clicks"),
    State("map-val-pozo",        "value"),
    State("map-val-fecha",       "value"),
    State("map-val-check",       "value"),
    State("map-val-comentario",  "value"),
    State("map-val-usuario",     "value"),
    prevent_initial_call=True,
)
def guardar_validacion(n, pozo, fecha, validada, comentario, usuario):
    if not pozo or not fecha or not GCS_BUCKET:
        return dbc.Alert("Completá todos los campos y verificá DINAS_BUCKET.", color="warning")
    nk = normalize_no_exact(str(pozo))
    fk = make_fecha_key(fecha)
    todas_val = load_all_validaciones(GCS_BUCKET, [nk], GCS_PREFIX)
    val_data  = todas_val.get(nk, {})
    val_data  = set_validacion(val_data, nk, fk, bool(validada), (comentario or "").strip(), (usuario or "anónimo").strip())
    ok = save_validaciones(GCS_BUCKET, nk, val_data, GCS_PREFIX)
    if ok:
        return dbc.Alert(f"✅ Guardado: [{pozo}] {fecha}", color="success", duration=3000)
    return dbc.Alert("❌ Error al guardar en GCS.", color="danger")
