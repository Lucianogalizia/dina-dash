# ==========================================================
# pages/diagnosticos.py — Tab 4: Diagnósticos IA
# Wrapper Dash — la lógica real está en components/diagnosticos_logic.py
# ==========================================================

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd

from data.loaders  import load_din_index, load_niv_index, load_coords_repo, resolve_existing_path, GCS_BUCKET, GCS_PREFIX, gcs_download_to_temp
from utils.helpers import find_col, build_keys, normalize_no_exact

# Importar lógica original (sin cambios)
from components.diagnosticos_logic import (
    generar_diagnostico, _load_diag_from_gcs, _load_all_diags_from_gcs,
    _necesita_regenerar, _build_global_table, _get_openai_key,
    SEVERIDAD_COLOR, SEVERIDAD_EMOJI, ESTADO_EMOJI, ESTADO_COLOR
)

dash.register_page(__name__, path="/diagnosticos", name="Diagnósticos", order=4)


def _setup():
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
    return din_ok, niv_ok


def _bat_map():
    coords = load_coords_repo()
    bm = {}
    if not coords.empty and "nombre_corto" in coords.columns and "nivel_5" in coords.columns:
        for _, row in coords.iterrows():
            bm[normalize_no_exact(str(row["nombre_corto"]))] = str(row["nivel_5"])
    return bm


def layout():
    din_ok, _ = _setup()
    pozos = sorted(din_ok["NO_key"].dropna().map(normalize_no_exact).loc[lambda s: s != ""].unique().tolist()) if not din_ok.empty else []

    return html.Div([
        html.H4("🤖 Diagnósticos IA — Cartas Dinamométricas"),

        # Diagnóstico individual
        dbc.Card([
            dbc.CardHeader("🔍 Diagnóstico individual"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Pozo"),
                        dcc.Dropdown(
                            id="diag-pozo",
                            options=[{"label": p, "value": p} for p in pozos],
                            value=pozos[0] if pozos else None,
                            style={"color": "#000"},
                            clearable=False,
                        ),
                    ], md=4),
                    dbc.Col([
                        dbc.Button("🔄 Generar / Actualizar", id="diag-btn-gen",
                                   color="primary", className="mt-4"),
                    ], md=3),
                ]),
                html.Br(),
                dcc.Loading(html.Div(id="diag-resultado"), type="circle"),
            ]),
        ], className="mb-4"),

        # Tabla global
        dbc.Card([
            dbc.CardHeader("📋 Tabla global — todos los pozos diagnosticados"),
            dbc.CardBody([
                dbc.Button("🔄 Actualizar tabla global", id="diag-btn-global",
                           color="secondary", className="mb-3"),
                dcc.Loading(html.Div(id="diag-tabla-global"), type="circle"),
            ]),
        ]),
    ])


@callback(
    Output("diag-resultado", "children"),
    Input("diag-btn-gen",    "n_clicks"),
    State("diag-pozo",       "value"),
    prevent_initial_call=True,
)
def generar_individual(_, pozo_sel):
    if not pozo_sel:
        return dbc.Alert("Seleccioná un pozo.", color="warning")

    api_key = _get_openai_key()
    if not api_key:
        return dbc.Alert("API Key de OpenAI no encontrada. Configurá OPENAI_API_KEY en GCP Secret Manager.", color="danger")

    din_ok, niv_ok = _setup()
    bat_m = _bat_map()

    # Verificar caché
    diag = None
    if GCS_BUCKET:
        diag = _load_diag_from_gcs(GCS_BUCKET, pozo_sel, GCS_PREFIX)

    if _necesita_regenerar(diag, din_ok, pozo_sel):
        diag = generar_diagnostico(
            no_key=pozo_sel, din_ok=din_ok, niv_ok=niv_ok,
            resolve_path_fn=resolve_existing_path,
            gcs_download_fn=gcs_download_to_temp,
            gcs_bucket=GCS_BUCKET, gcs_prefix=GCS_PREFIX,
            api_key=api_key,
        )

    if not diag or "error" in diag:
        return dbc.Alert(f"Error: {diag.get('error','desconocido') if diag else 'Sin resultado'}", color="danger")

    return _render_diag(diag, pozo_sel, bat_m)


@callback(
    Output("diag-tabla-global", "children"),
    Input("diag-btn-global",    "n_clicks"),
    prevent_initial_call=True,
)
def cargar_tabla_global(_):
    if not GCS_BUCKET:
        return dbc.Alert("La vista global requiere GCS (DINAS_BUCKET).", color="warning")
    din_ok, _ = _setup()
    pozos = sorted(din_ok["NO_key"].dropna().map(normalize_no_exact).loc[lambda s: s != ""].unique().tolist()) if not din_ok.empty else []
    diags = _load_all_diags_from_gcs(GCS_BUCKET, pozos, GCS_PREFIX)
    if not diags:
        return dbc.Alert("No hay diagnósticos en GCS todavía.", color="info")
    bat_m  = _bat_map()
    df_g   = _build_global_table(diags, bat_m, normalize_no_exact)
    if df_g.empty:
        return html.P("Sin datos.")
    df_show = df_g.drop(columns=["_prob_lista"], errors="ignore")
    return dbc.Table.from_dataframe(
        df_show.head(300),
        striped=True, bordered=True, hover=True, size="sm", responsive=True,
        style={"fontSize": "0.8rem"},
    )


def _render_diag(diag: dict, no_key: str, bat_map: dict):
    bateria      = bat_map.get(no_key, "N/D")
    meds         = diag.get("mediciones", [])
    total_act    = sum(1 for m in meds for p in m.get("problemáticas",[]) if p.get("estado")=="ACTIVA")
    total_res    = sum(1 for m in meds for p in m.get("problemáticas",[]) if p.get("estado")=="RESUELTA")

    cards_meds = []
    for med in meds:
        fecha  = med.get("fecha","?")
        label  = med.get("label","")
        probs  = med.get("problemáticas", [])
        probs_sorted = sorted(probs, key=lambda x: (
            0 if x.get("estado")=="ACTIVA" else 1,
            {"CRÍTICA":0,"ALTA":1,"MEDIA":2,"BAJA":3}.get(x.get("severidad","BAJA"), 9)
        ))
        prob_items = []
        for p in probs_sorted:
            sev   = p.get("severidad","BAJA")
            color = SEVERIDAD_COLOR.get(sev,"#6c757d")
            prob_items.append(html.Li([
                ESTADO_EMOJI.get(p.get("estado","ACTIVA"),""),
                SEVERIDAD_EMOJI.get(sev,"⚪"),
                f" {p.get('nombre','?')} — ",
                html.Span(sev, style={"color": color, "fontWeight":"bold"}),
                html.Br(),
                html.Small(p.get("descripcion",""), className="text-muted"),
            ]))

        cards_meds.append(dbc.Card([
            dbc.CardHeader(f"📅 {fecha} — {label}"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(dbc.Card(dbc.CardBody([html.H6("Llenado"), html.H5(f"{med.get('llenado_pct','N/D')}%")])), md=3),
                    dbc.Col(dbc.Card(dbc.CardBody([html.H6("Sumergencia"), html.H5(f"{med.get('sumergencia_m','N/D')} m")])), md=3),
                    dbc.Col(dbc.Card(dbc.CardBody([html.H6("Caudal"), html.H5(f"{med.get('caudal_bruto','N/D')} m³/d")])), md=3),
                    dbc.Col(dbc.Card(dbc.CardBody([html.H6("%Balance"), html.H5(f"{med.get('pct_balance','N/D')}%")])), md=3),
                ], className="mb-2"),
                html.Ul(prob_items) if prob_items else dbc.Alert("Sin problemáticas.", color="success", className="py-1"),
            ]),
        ], className="mb-2"))

    return html.Div([
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Batería"),       html.H5(bateria)])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("DINs"),          html.H5(len(meds))])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Confianza"),     html.H5(diag.get("confianza","?"))])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("⚠️ Activas"),    html.H5(total_act)])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("✅ Resueltas"),  html.H5(total_res)])), md=2),
        ], className="mb-3"),
        dbc.Alert(diag.get("resumen",""), color="info"),
        html.H6("Detalle por medición:"),
        *cards_meds,
        dbc.Alert([html.Strong("💡 Recomendación: "), diag.get("recomendacion","")], color="success"),
    ])
