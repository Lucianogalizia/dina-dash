# ==========================================================
# pages/mediciones.py — Tab 1: Mediciones por pozo
#
# CLAVE vs Streamlit:
#   - El selector de pozo actualiza SOLO el gráfico (callback parcial)
#   - El multiselect de DINs actualiza SOLO la carta
#   - La tabla y el histórico son callbacks independientes
#   → No re-ejecuta NADA más al cambiar un filtro
# ==========================================================

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd

from data.loaders    import load_din_index, load_niv_index, load_coords_repo, resolve_existing_path
from data.parsers    import parse_din_surface_points, parse_din_extras, EXTRA_FIELDS
from data.consolidado import build_global_consolidated
from utils.helpers   import (
    find_col, build_keys, normalize_no_exact, safe_to_float,
    compute_sumergencia_and_base, infer_dt_plot, make_unique_columns, make_display_label
)

dash.register_page(__name__, path="/mediciones", name="Mediciones", order=1)


# ---------- Helpers de preparación ----------

def _get_pozos():
    df_din = load_din_index()
    df_niv = load_niv_index()

    din_no = find_col(df_din, ["pozo", "NO"])
    niv_no = find_col(df_niv, ["pozo", "NO"])
    din_fe = find_col(df_din, ["fecha", "FE"])
    niv_fe = find_col(df_niv, ["fecha", "FE"])
    din_ho = find_col(df_din, ["hora",  "HO"])
    niv_ho = find_col(df_niv, ["hora",  "HO"])

    din_k = build_keys(df_din, din_no, din_fe, din_ho) if (not df_din.empty and din_no and din_fe) else pd.DataFrame()
    niv_k = build_keys(df_niv, niv_no, niv_fe, niv_ho) if (not df_niv.empty and niv_no and niv_fe) else pd.DataFrame()

    din_ok = din_k[din_k["error"].isna()] if (not din_k.empty and "error" in din_k.columns) else din_k
    niv_ok = niv_k[niv_k["error"].isna()] if (not niv_k.empty and "error" in niv_k.columns) else niv_k

    if not din_ok.empty and "path" in din_ok.columns:
        din_ok["path"] = din_ok["path"].apply(lambda x: resolve_existing_path(x) if pd.notna(x) else None)

    pozos = sorted(
        pd.Series(
            list(din_ok["NO_key"].tolist() if not din_ok.empty else []) +
            list(niv_ok["NO_key"].tolist() if not niv_ok.empty else [])
        ).dropna().map(normalize_no_exact).loc[lambda s: s != ""].unique().tolist()
    )
    return pozos, din_ok, niv_ok, din_no, din_fe, din_ho, niv_no, niv_fe, niv_ho


# ---------- Layout ----------

def layout():
    pozos, _, _, _, _, _, _, _, _ = _get_pozos()

    return html.Div([
        # Store para guardar el pozo seleccionado
        dcc.Store(id="store-pozo-sel", data=pozos[0] if pozos else ""),

        dbc.Row([
            dbc.Col([
                dbc.Label("Pozo (NO=)"),
                dcc.Dropdown(
                    id     = "dd-pozo",
                    options= [{"label": p, "value": p} for p in pozos],
                    value  = pozos[0] if pozos else None,
                    clearable=False,
                    style  = {"color": "#000"},
                ),
            ], md=4),
        ], className="mb-3"),

        # Tabla de mediciones
        html.H5("Mediciones (DIN + NIV)"),
        dcc.Loading(html.Div(id="tabla-mediciones"), type="circle"),

        html.Hr(),

        # Histórico sumergencia
        html.H5("📉 Histórico — Sumergencia vs Tiempo"),
        dcc.Loading(dcc.Graph(id="graf-historico"), type="circle"),

        html.Hr(),

        # Selector de DINs para carta
        html.H5("Carta Dinamométrica — Superficie (CS)"),
        dcc.Loading(html.Div([
            dcc.Dropdown(id="dd-dins", multi=True, style={"color": "#000"}, placeholder="Seleccioná mediciones DIN..."),
            dcc.Graph(id="graf-carta"),
        ]), type="circle"),
    ])


# ---------- Callbacks ----------

@callback(
    Output("tabla-mediciones", "children"),
    Output("graf-historico",   "figure"),
    Output("dd-dins",          "options"),
    Output("dd-dins",          "value"),
    Input("dd-pozo",           "value"),
)
def update_mediciones(pozo_sel):
    """
    Se ejecuta SOLO cuando cambia el pozo.
    Devuelve tabla + histórico + opciones del multiselect de DINs.
    """
    if not pozo_sel:
        return html.P("Seleccioná un pozo."), go.Figure(), [], []

    pozos, din_ok, niv_ok, din_no, din_fe, din_ho, niv_no, niv_fe, niv_ho = _get_pozos()

    din_p = din_ok[din_ok["NO_key"] == pozo_sel].copy() if not din_ok.empty else pd.DataFrame()
    niv_p = niv_ok[niv_ok["NO_key"] == pozo_sel].copy() if not niv_ok.empty else pd.DataFrame()

    # Dedup
    if not niv_p.empty:
        sort_c = [c for c in ["niv_datetime", "mtime"] if c in niv_p.columns]
        niv_p  = niv_p.sort_values(sort_c).drop_duplicates(["NO_key","FE_key","HO_key"], keep="last").reset_index(drop=True)
    if not din_p.empty:
        sort_c = [c for c in ["din_datetime", "mtime"] if c in din_p.columns]
        din_p  = din_p.sort_values(sort_c)
        sub    = ["path"] if "path" in din_p.columns else ["NO_key","FE_key","HO_key"]
        din_p  = din_p.drop_duplicates(subset=sub, keep="last").reset_index(drop=True)

    # Merge DIN + NIV
    din_join = din_p.copy()
    if not din_join.empty:
        din_join["ORIGEN"] = "DIN"
        if not niv_p.empty:
            din_join = din_join.merge(niv_p, on=["NO_key","FE_key","HO_key"], how="left", suffixes=("","_niv"))

    niv_only = pd.DataFrame()
    if not niv_p.empty:
        if din_p.empty:
            niv_only = niv_p.copy()
        else:
            niv_only = niv_p.merge(din_p[["NO_key","FE_key","HO_key"]].drop_duplicates(),
                                   on=["NO_key","FE_key","HO_key"], how="left", indicator=True)
            niv_only = niv_only[niv_only["_merge"]=="left_only"].drop(columns=["_merge"])
        if not niv_only.empty:
            niv_only["ORIGEN"] = "NIV"

    dfp = pd.concat([din_join, niv_only], ignore_index=True, sort=False)
    dfp = make_unique_columns(dfp)

    if dfp.empty:
        return html.P("Sin datos para este pozo."), go.Figure(), [], []

    # Columnas base
    for src, dst in [(din_no or niv_no, "pozo"), (din_fe or niv_fe, "fecha"), (din_ho or niv_ho, "hora")]:
        if src and src in dfp.columns:
            dfp[dst] = dfp[src]
    for c in ["NM","NC","ND","PE","PB"]:
        if c in dfp.columns:
            dfp[c] = dfp[c].apply(safe_to_float)
    tmp = dfp.apply(compute_sumergencia_and_base, axis=1, result_type="expand")
    dfp["Sumergencia"] = tmp[0]
    dfp["DT_plot"]     = infer_dt_plot(dfp)

    # ── Tabla ──
    show_cols = [c for c in ["ORIGEN","pozo","fecha","hora","NM","NC","ND","PE","PB","Sumergencia"] if c in dfp.columns]
    tabla = dbc.Table.from_dataframe(
        dfp[show_cols].head(100),
        striped=True, bordered=True, hover=True, size="sm", responsive=True,
        style={"fontSize": "0.8rem"}
    )

    # ── Histórico sumergencia ──
    hist = dfp.dropna(subset=["DT_plot","Sumergencia"]).sort_values("DT_plot").copy()
    fig_hist = go.Figure()
    if not hist.empty:
        fig_hist.add_trace(go.Scatter(
            x=hist["DT_plot"], y=hist["Sumergencia"],
            mode="lines+markers", name="Sumergencia",
        ))
        fig_hist.update_layout(
            xaxis_title="Fecha / Hora",
            yaxis_title="Sumergencia (PB - nivel)",
            template="plotly_dark", height=380,
        )

    # ── Opciones multiselect DINs ──
    din_rows = dfp[dfp.get("ORIGEN","") == "DIN"].copy() if "ORIGEN" in dfp.columns else pd.DataFrame()
    din_opts = []
    if not din_rows.empty and "path" in din_rows.columns:
        din_rows["label"] = din_rows.apply(make_display_label, axis=1)
        din_opts = [{"label": r["label"], "value": str(r["path"])} for _, r in din_rows.iterrows() if r.get("path")]

    default_sel = [din_opts[0]["value"]] if din_opts else []

    return tabla, fig_hist, din_opts, default_sel


@callback(
    Output("graf-carta", "figure"),
    Input("dd-dins",     "value"),
)
def update_carta(sel_paths):
    """
    Se ejecuta SOLO cuando cambia la selección de DINs.
    No toca la tabla ni el histórico.
    """
    fig = go.Figure()
    fig.update_layout(
        title="Carta Dinamométrica — Superficie (CS)",
        xaxis_title="X (posición / carrera)",
        yaxis_title="Y (carga)",
        template="plotly_dark", height=600,
    )
    if not sel_paths:
        return fig

    for path in sel_paths:
        p_res = resolve_existing_path(path)
        if not p_res:
            continue
        try:
            pts = parse_din_surface_points(str(p_res))
            if not pts.empty:
                fig.add_trace(go.Scatter(x=pts["X"], y=pts["Y"], mode="lines", name=path.split("/")[-1]))
        except Exception:
            pass

    return fig
