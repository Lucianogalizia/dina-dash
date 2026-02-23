# ==========================================================
# pages/diagnosticos.py — Tab 4: Diagnósticos IA
# Migración FIEL de render_tab_diagnosticos del original.
# ==========================================================

import io
import dash
from dash import html, dcc, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from data.loaders      import load_din_index, load_niv_index, resolve_existing_path, load_coords_repo
from utils.helpers     import find_col, build_keys, normalize_no_exact
from components.diagnosticos_logic import (
    generar_diagnostico,
    _load_diag_from_gcs,
    _load_all_diags_from_gcs,
    _necesita_regenerar,
    _build_global_table,
    _get_openai_key,
    SEVERIDAD_COLOR, SEVERIDAD_EMOJI, ESTADO_EMOJI, ESTADO_COLOR,
    SEVERIDAD_ORDEN, DIAG_SCHEMA_VERSION,
)

dash.register_page(__name__, path="/diagnosticos", name="Diagnósticos", order=4)

def _df_to_table(df):
    """Convierte DataFrame a list[dict] para Dash DataTable, sin NaN/Timestamp."""
    d = df.copy()
    for col in d.columns:
        if pd.api.types.is_datetime64_any_dtype(d[col]):
            d[col] = d[col].dt.strftime("%Y-%m-%d %H:%M").where(d[col].notna(), "")
        elif pd.api.types.is_float_dtype(d[col]):
            d[col] = d[col].apply(lambda x: round(x, 2) if pd.notna(x) else "")
        elif pd.api.types.is_integer_dtype(d[col]):
            d[col] = d[col].apply(lambda x: int(x) if pd.notna(x) else "")
        else:
            def _s(x):
                try: return "" if pd.isna(x) else (x if isinstance(x, str) else str(x))
                except: return str(x) if x is not None else ""
            d[col] = d[col].apply(_s)
    return d.to_dict("records")


import os
GCS_BUCKET = os.environ.get("DINAS_BUCKET", "").strip()
GCS_PREFIX = os.environ.get("DINAS_GCS_PREFIX", "").strip().strip("/")


def _get_din_niv():
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
    return din_ok, niv_ok


def _get_bat_map():
    coords = load_coords_repo()
    bat_map = {}
    if not coords.empty and "nombre_corto" in coords.columns and "nivel_5" in coords.columns:
        for _, row in coords.iterrows():
            k = normalize_no_exact(str(row["nombre_corto"]))
            bat_map[k] = str(row["nivel_5"])
    return bat_map


def _get_pozos_con_din(din_ok):
    if din_ok.empty or "NO_key" not in din_ok.columns:
        return []
    return sorted(
        din_ok["NO_key"].dropna()
             .map(normalize_no_exact)
             .loc[lambda s: s != ""]
             .unique().tolist()
    )


def _gcs_download(gs_url: str) -> str:
    from google.cloud import storage
    import tempfile
    from pathlib import Path
    client = storage.Client()
    u = gs_url.strip()[5:]
    bucket_name, _, blob_name = u.partition("/")
    blob = client.bucket(bucket_name).blob(blob_name)
    safe_name = blob_name.replace("/", "__")
    local_path = f"{tempfile.gettempdir()}/{safe_name}"
    if not __import__("os").path.exists(local_path):
        blob.download_to_filename(local_path)
    return local_path


def layout():
    din_ok, _ = _get_din_niv()
    pozos     = _get_pozos_con_din(din_ok)
    bat_map   = _get_bat_map()

    diags_cache = _load_all_diags_from_gcs(GCS_BUCKET, pozos, GCS_PREFIX) if GCS_BUCKET else {}
    ya_listos   = len(diags_cache)
    pendientes  = sum(1 for p in pozos if _necesita_regenerar(diags_cache.get(p), din_ok, p))

    return html.Div([
        html.H4("🤖 Diagnósticos IA — Análisis de cartas dinamométricas"),

        # ── Generación en lote ──
        dbc.Card([
            dbc.CardHeader("⚙️ Generación en lote — todos los pozos",
                           style={"cursor":"pointer"}, id="diag-lote-header"),
            dbc.Collapse([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col(dbc.Card(dbc.CardBody([html.H6("Total pozos con DIN"), html.H4(len(pozos))])), md=3),
                        dbc.Col(dbc.Card(dbc.CardBody([html.H6("✅ Con diagnóstico"),   html.H4(ya_listos)])),   md=3),
                        dbc.Col(dbc.Card(dbc.CardBody([html.H6("⏳ Pendientes"),         html.H4(pendientes)])),  md=3),
                    ], className="mb-3"),
                    dbc.Row([
                        dbc.Col([dbc.Switch(id="diag-solo-pend", value=True,
                                            label="Saltear pozos ya actualizados")], md=4),
                        dbc.Col([html.Div(id="diag-lote-info")], md=4),
                    ], className="mb-2"),
                    dbc.Button("🚀 Generar todos los diagnósticos", id="diag-btn-todos",
                               color="primary", className="mb-2"),
                    dcc.Loading(html.Div(id="diag-lote-resultado"), type="circle"),
                ])
            ], id="diag-lote-collapse", is_open=False),
        ], className="mb-4"),

        html.Hr(),

        # ── Diagnóstico individual ──
        html.H5(id="diag-individual-titulo", children=f"🔍 Diagnóstico individual"),
        dbc.Row([
            dbc.Col([dbc.Label("Pozo"),
                     dcc.Dropdown(id="diag-pozo-sel",
                                  options=[{"label":p,"value":p} for p in pozos],
                                  value=pozos[0] if pozos else None,
                                  style={"color":"#000"})], md=4),
        ], className="mb-3"),
        dcc.Loading(html.Div(id="diag-individual-div"), type="circle"),

        html.Hr(),

        # ── Tabla global ──
        html.H5("📋 Tabla global — una fila por medición"),
        dcc.Loading(html.Div(id="diag-tabla-global-div"), type="circle"),
    ])


# ── Toggle lote ──
@callback(
    Output("diag-lote-collapse", "is_open"),
    Input("diag-lote-header",    "n_clicks"),
    State("diag-lote-collapse",  "is_open"),
    prevent_initial_call=True,
)
def toggle_lote(n, is_open):
    return not is_open


# ── Info lote ──
@callback(
    Output("diag-lote-info", "children"),
    Input("diag-solo-pend",  "value"),
)
def update_lote_info(solo_pend):
    din_ok, _ = _get_din_niv()
    pozos     = _get_pozos_con_din(din_ok)
    diags     = _load_all_diags_from_gcs(GCS_BUCKET, pozos, GCS_PREFIX) if GCS_BUCKET else {}
    pendientes = sum(1 for p in pozos if _necesita_regenerar(diags.get(p), din_ok, p))
    cant = pendientes if solo_pend else len(pozos)
    seg  = cant * 10
    t    = f"{seg//60}m {seg%60}s" if seg >= 60 else f"{seg}s"
    return html.P(f"A generar: {cant} pozos | Tiempo estimado: ~{t}")


# ── Generar todos (síncrono, puede tardar) ──
@callback(
    Output("diag-lote-resultado", "children"),
    Input("diag-btn-todos",       "n_clicks"),
    State("diag-solo-pend",       "value"),
    prevent_initial_call=True,
)
def generar_todos(n, solo_pend):
    if not n:
        return ""
    api_key = _get_openai_key()
    if not api_key:
        return dbc.Alert("No encontré OPENAI_API_KEY.", color="danger")

    din_ok, niv_ok = _get_din_niv()
    pozos          = _get_pozos_con_din(din_ok)
    diags_cache    = _load_all_diags_from_gcs(GCS_BUCKET, pozos, GCS_PREFIX) if GCS_BUCKET else {}

    pozos_a_proc = [p for p in pozos
                    if not solo_pend or _necesita_regenerar(diags_cache.get(p), din_ok, p)]

    ok, errores, salteados = [], [], []
    for p in pozos_a_proc:
        try:
            d = generar_diagnostico(
                no_key=p, din_ok=din_ok,
                resolve_path_fn=resolve_existing_path,
                gcs_download_fn=_gcs_download,
                gcs_bucket=GCS_BUCKET, gcs_prefix=GCS_PREFIX,
                api_key=api_key, niv_ok=niv_ok,
            )
            (errores if "error" in d else ok).append(p)
        except Exception as e:
            errores.append(f"{p}: {e}")

    salteados = [p for p in pozos if p not in pozos_a_proc]
    return dbc.Alert(
        f"✅ {len(ok)} generados | ❌ {len(errores)} errores | ⏭️ {len(salteados)} salteados",
        color="success" if not errores else "warning"
    )


# ── Diagnóstico individual ──
@callback(
    Output("diag-individual-div",    "children"),
    Output("diag-individual-titulo", "children"),
    Input("diag-pozo-sel",           "value"),
)
def update_individual(pozo_sel):
    if not pozo_sel:
        return html.P("Seleccioná un pozo."), "🔍 Diagnóstico individual"

    din_ok, niv_ok = _get_din_niv()
    bat_map        = _get_bat_map()
    api_key        = _get_openai_key()

    if not api_key:
        return dbc.Alert("No encontré OPENAI_API_KEY.", color="danger"), f"🔍 {pozo_sel}"

    pozos = _get_pozos_con_din(din_ok)
    if pozo_sel not in pozos:
        return dbc.Alert(f"El pozo {pozo_sel} no tiene DIN indexados.", color="info"), f"🔍 {pozo_sel}"

    diag_cache = _load_diag_from_gcs(GCS_BUCKET, pozo_sel, GCS_PREFIX) if GCS_BUCKET else None

    if _necesita_regenerar(diag_cache, din_ok, pozo_sel):
        diag = generar_diagnostico(
            no_key=pozo_sel, din_ok=din_ok,
            resolve_path_fn=resolve_existing_path,
            gcs_download_fn=_gcs_download,
            gcs_bucket=GCS_BUCKET, gcs_prefix=GCS_PREFIX,
            api_key=api_key, niv_ok=niv_ok,
        )
        cache_info = ""
    else:
        diag      = diag_cache
        meta      = diag.get("_meta", {})
        cache_info = f"✅ Caché GCS | Generado: {meta.get('generado_utc','?')[:19].replace('T',' ')} UTC | DIN más reciente: {meta.get('fecha_din_mas_reciente','?')}"

    return _render_individual(diag, pozo_sel, bat_map, cache_info), f"🔍 Diagnóstico individual — Pozo: {pozo_sel}"


def _render_individual(diag, no_key, bat_map, cache_info):
    if not diag or "error" in diag:
        return dbc.Alert(f"Error: {diag.get('error','desconocido')}", color="danger")

    bateria     = bat_map.get(normalize_no_exact(no_key), "N/D")
    confianza   = diag.get("confianza", "?")
    meds        = diag.get("mediciones", [])
    total_act   = sum(1 for m in meds for p in m.get("problemáticas",[]) if p.get("estado")=="ACTIVA")
    total_res   = sum(1 for m in meds for p in m.get("problemáticas",[]) if p.get("estado")=="RESUELTA")

    items = [
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Batería"),         html.H4(bateria)])),   md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("DINs analizados"), html.H4(len(meds))])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Confianza"),       html.H4(confianza)])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("⚠️ Activas"),      html.H4(total_act)])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("✅ Resueltas"),    html.H4(total_res)])), md=2),
        ], className="mb-3"),
    ]

    if cache_info:
        items.append(html.P(cache_info, style={"fontSize":"12px","color":"#aaa"}))

    items.append(html.H6("📝 Resumen ejecutivo"))
    items.append(dbc.Alert(diag.get("resumen","Sin resumen."), color="info"))
    items.append(html.H6("🔒 Variables operativas sin cambio"))
    items.append(html.P(diag.get("variables_sin_cambio","N/D"), style={"fontSize":"12px"}))

    if meds:
        items.append(html.H6("📋 Detalle por medición"))
        for med in meds:
            fecha  = med.get("fecha","?")
            label  = med.get("label","")
            probs  = med.get("problemáticas",[])
            llenado = med.get("llenado_pct")
            sumer   = med.get("sumergencia_m")
            sumer_n = med.get("sumergencia_nivel","N/D")
            caudal  = med.get("caudal_bruto")
            balance = med.get("pct_balance")

            probs_sorted = sorted(probs, key=lambda x:(
                0 if x.get("estado")=="ACTIVA" else 1,
                SEVERIDAD_ORDEN.get(x.get("severidad","BAJA"),9)
            ))

            prob_items = []
            for p in probs_sorted:
                sev   = p.get("severidad","BAJA")
                estado = p.get("estado","ACTIVA")
                prob_items.append(html.Div([
                    html.Span(f"{ESTADO_EMOJI.get(estado,'')} {SEVERIDAD_EMOJI.get(sev,'')} "),
                    html.Strong(p.get("nombre","?")),
                    html.Span(f" — {sev}", style={"color":SEVERIDAD_COLOR.get(sev,"#666"),"fontWeight":"bold"}),
                    html.Span(f" | {estado}", style={"color":ESTADO_COLOR.get(estado,"#666"),"fontWeight":"bold"}),
                    html.Br(),
                    html.Small(p.get("descripcion",""), style={"color":"#ccc"}),
                ], className="mb-2"))

            is_reciente = label in ("Más reciente","Única medición")
            items.append(dbc.Card([
                dbc.CardHeader(
                    dbc.Row([
                        dbc.Col(html.Strong(f"📅 {fecha}  —  {label}")),
                        dbc.Col([
                            dbc.Badge(f"Llenado: {llenado}%" if llenado is not None else "Llenado: N/D", color="secondary", className="me-1"),
                            dbc.Badge(f"Sumer: {sumer}m ({sumer_n})" if sumer is not None else "Sumer: N/D", color="secondary", className="me-1"),
                            dbc.Badge(f"Caudal: {caudal} m³/d" if caudal is not None else "Caudal: N/D", color="secondary", className="me-1"),
                            dbc.Badge(f"%Balance: {balance}%" if balance is not None else "%Balance: N/D", color="secondary"),
                        ]),
                    ])
                ),
                dbc.CardBody(prob_items if prob_items else [html.P("✅ Sin problemáticas en esta medición.")]),
            ], className="mb-2", color="dark", outline=True))

    items.append(html.H6("💡 Recomendación"))
    items.append(dbc.Alert(diag.get("recomendacion","Sin recomendación."), color="success"))

    return html.Div(items)


# ── Tabla global ──
@callback(
    Output("diag-tabla-global-div", "children"),
    Input("diag-pozo-sel",          "value"),  # refrescar cuando cambia pozo
)
def update_tabla_global(_):
    if not GCS_BUCKET:
        return dbc.Alert("La tabla global requiere GCS (variable DINAS_BUCKET).", color="warning")

    din_ok, _ = _get_din_niv()
    pozos     = _get_pozos_con_din(din_ok)
    bat_map   = _get_bat_map()

    diags = _load_all_diags_from_gcs(GCS_BUCKET, pozos, GCS_PREFIX)
    if not diags:
        return dbc.Alert("Todavía no hay diagnósticos en GCS. Generalos con el panel ⚙️ de arriba.", color="info")

    df = _build_global_table(diags, bat_map, normalize_no_exact)
    if df.empty:
        return html.P("No hay datos para mostrar.")

    # KPIs
    pozos_unicos = df["Pozo"].nunique()
    criticos     = df[df["Sev. máx"]=="CRÍTICA"]["Pozo"].nunique()
    altos        = df[df["Sev. máx"]=="ALTA"]["Pozo"].nunique()
    sin_prob     = df[df["Sev. máx"]=="NINGUNA"]["Pozo"].nunique()

    # Filtros
    baterias  = sorted(df["Batería"].dropna().unique().tolist())
    sevs      = ["CRÍTICA","ALTA","MEDIA","BAJA","RESUELTA","NINGUNA"]
    med_labels = sorted(df["Medición"].dropna().unique().tolist())
    todas_probs = sorted(set(n for lista in df["_prob_lista"] for n in lista))

    df_mostrar = df.drop(columns=["_prob_lista"])

    # Gráficos
    color_sev = {"BAJA":"#28a745","MEDIA":"#ffc107","ALTA":"#fd7e14",
                 "CRÍTICA":"#dc3545","NINGUNA":"#adb5bd","RESUELTA":"#6c757d"}
    df_ultimo = df.sort_values("Fecha DIN").groupby("Pozo").last().reset_index()
    sev_counts = df_ultimo["Sev. máx"].value_counts().reset_index()
    sev_counts.columns = ["Severidad","Pozos"]
    fig_sev = px.bar(sev_counts, x="Severidad", y="Pozos", color="Severidad",
                     color_discrete_map=color_sev,
                     title="Pozos por severidad (última medición)",
                     category_orders={"Severidad":["CRÍTICA","ALTA","MEDIA","BAJA","RESUELTA","NINGUNA"]},
                     template="plotly_dark")

    prob_freq: dict = {}
    for lista in df["_prob_lista"]:
        for n in lista:
            prob_freq[n] = prob_freq.get(n,0)+1
    fig_freq = go.Figure(layout={"template":"plotly_dark"})
    if prob_freq:
        df_freq = pd.DataFrame(list(prob_freq.items()), columns=["Problemática","Ocurrencias"]).sort_values("Ocurrencias")
        fig_freq = px.bar(df_freq, y="Problemática", x="Ocurrencias", orientation="h",
                          title="Frecuencia de problemáticas",
                          height=max(300,len(df_freq)*28),
                          color_discrete_sequence=["#fd7e14"],
                          template="plotly_dark")

    return html.Div([
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Pozos diagnosticados"), html.H4(pozos_unicos)])), md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("Mediciones totales"),   html.H4(len(df))])),      md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("🔴 Pozos CRÍTICOS"),    html.H4(criticos)])),     md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("🟠 Pozos ALTA sev."),   html.H4(altos)])),        md=2),
            dbc.Col(dbc.Card(dbc.CardBody([html.H6("🟢 Sin problemáticas"), html.H4(sin_prob)])),     md=2),
        ], className="mb-3"),

        html.H6("Filtros"),
        dbc.Row([
            dbc.Col([dbc.Label("Batería"),
                     dcc.Dropdown(id="diag-g-bat", options=[{"label":b,"value":b} for b in baterias],
                                  value=baterias, multi=True, style={"color":"#000"})], md=3),
            dbc.Col([dbc.Label("Severidad máx."),
                     dcc.Dropdown(id="diag-g-sev", options=[{"label":s,"value":s} for s in sevs],
                                  value=sevs, multi=True, style={"color":"#000"})], md=3),
            dbc.Col([dbc.Label("Medición"),
                     dcc.Dropdown(id="diag-g-med", options=[{"label":m,"value":m} for m in med_labels],
                                  value=med_labels, multi=True, style={"color":"#000"})], md=3),
            dbc.Col([dbc.Label("Tiene problemática"),
                     dcc.Dropdown(id="diag-g-prob", options=[{"label":p,"value":p} for p in todas_probs],
                                  value=[], multi=True, placeholder="Filtrar...", style={"color":"#000"})], md=3),
        ], className="mb-3"),

        dcc.Store(id="diag-g-store", data=df_mostrar.to_json(orient="split")),
        html.Div(id="diag-g-tabla-div"),

        html.H6("📊 Distribución"),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_sev),  md=6),
            dbc.Col(dcc.Graph(figure=fig_freq), md=6),
        ]),

        html.H6("⬇️ Exportar"),
        html.A("Descargar CSV",
               href="data:text/csv;charset=utf-8," + df_mostrar.to_csv(index=False),
               download="diagnosticos_mediciones.csv",
               className="btn btn-sm btn-outline-secondary"),
    ])


@callback(
    Output("diag-g-tabla-div", "children"),
    Input("diag-g-bat",   "value"),
    Input("diag-g-sev",   "value"),
    Input("diag-g-med",   "value"),
    Input("diag-g-prob",  "value"),
    State("diag-g-store", "data"),
)
def filtrar_tabla_global(bat_sel, sev_sel, med_sel, prob_sel, store_data):
    if not store_data:
        return html.P("Sin datos.")
    df = pd.read_json(io.StringIO(store_data), orient="split")

    # Necesitamos _prob_lista para filtrar — reconstruir desde store no es posible,
    # así que re-cargamos. Es rápido porque _load_all_diags usa cache GCS.
    din_ok, _ = _get_din_niv()
    pozos     = _get_pozos_con_din(din_ok)
    bat_map   = _get_bat_map()
    diags     = _load_all_diags_from_gcs(GCS_BUCKET, pozos, GCS_PREFIX)
    df_full   = _build_global_table(diags, bat_map, normalize_no_exact)
    if df_full.empty:
        return html.P("Sin datos.")

    df_f = df_full.copy()
    if bat_sel:  df_f = df_f[df_f["Batería"].isin(bat_sel)]
    if sev_sel:  df_f = df_f[df_f["Sev. máx"].isin(sev_sel)]
    if med_sel:  df_f = df_f[df_f["Medición"].isin(med_sel)]
    if prob_sel:
        df_f = df_f[df_f["_prob_lista"].apply(lambda l: any(p in l for p in prob_sel))]

    df_show = df_f.drop(columns=["_prob_lista"])
    return html.Div([
        html.P(f"Mostrando {len(df_show)} mediciones ({df_f['Pozo'].nunique()} pozos)",
               style={"fontSize":"12px"}),
        dash_table.DataTable(
            data=df_show.pipe(_df_to_table),
            columns=[{"name":c,"id":c} for c in df_show.columns],
            page_size=20, style_table={"overflowX":"auto"},
            style_cell={"fontSize":"11px","padding":"4px","whiteSpace":"pre-line"},
            style_header={"fontWeight":"bold","backgroundColor":"#2c2c2c","color":"white"},
            sort_action="native", filter_action="native",
        ),
    ])
