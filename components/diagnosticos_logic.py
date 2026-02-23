# ==========================================================
# components/diagnosticos_logic.py
# Lógica pura de diagnósticos IA — SIN dependencias de Streamlit.
# Extraída y limpiada de diagnostico_tab.py del repo original.
# ==========================================================

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DIAG_SCHEMA_VERSION = 11

CATALOGO_PROBLEMATICAS = [
    "Llenado bajo de bomba", "Golpeo de fondo", "Fuga en válvula viajera",
    "Fuga en válvula fija", "Interferencia de fluido", "Bomba asentada parcialmente",
    "Gas en bomba", "Desbalance de contrapesos", "Sobrecarga estructural",
    "Subcarrera / carrera insuficiente", "Desgaste de bomba", "Sumergencia crítica",
    "Tendencia de declinación de caudal", "Rotura / desgaste de varillas",
    "Exceso de fricción en varillas",
]

SEVERIDAD_ORDEN = {"CRÍTICA": 0, "ALTA": 1, "MEDIA": 2, "BAJA": 3}
SEVERIDAD_COLOR = {"BAJA": "#28a745", "MEDIA": "#ffc107", "ALTA": "#fd7e14", "CRÍTICA": "#dc3545"}
SEVERIDAD_EMOJI = {"BAJA": "🟢", "MEDIA": "🟡", "ALTA": "🟠", "CRÍTICA": "🔴"}
ESTADO_EMOJI    = {"ACTIVA": "⚠️", "RESUELTA": "✅"}
ESTADO_COLOR    = {"ACTIVA": "#dc3545", "RESUELTA": "#28a745"}


# ------------------------------------------------------------------ #
#  GCS helpers
# ------------------------------------------------------------------ #

def _get_gcs_client():
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception:
        return None


def _get_openai_key() -> str | None:
    try:
        from google.cloud import secretmanager
        project_id  = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
        secret_name = os.environ.get("OPENAI_SECRET_NAME", "OPENAI_API_KEY")
        if project_id:
            client   = secretmanager.SecretManagerServiceClient()
            name     = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            key      = response.payload.data.decode("UTF-8").strip()
            if key:
                return key
    except Exception:
        pass
    return os.environ.get("OPENAI_API_KEY", "").strip() or None


def _load_diag_from_gcs(bucket_name: str, no_key: str, prefix: str = "") -> dict | None:
    client = _get_gcs_client()
    if not client:
        return None
    blob_name = f"{prefix}/diagnosticos/{no_key}/diagnostico.json" if prefix else f"diagnosticos/{no_key}/diagnostico.json"
    try:
        blob = client.bucket(bucket_name).blob(blob_name)
        return json.loads(blob.download_as_text(encoding="utf-8")) if blob.exists() else None
    except Exception:
        return None


def _save_diag_to_gcs(bucket_name: str, no_key: str, diag: dict, prefix: str = "") -> bool:
    client = _get_gcs_client()
    if not client:
        return False
    blob_name = f"{prefix}/diagnosticos/{no_key}/diagnostico.json" if prefix else f"diagnosticos/{no_key}/diagnostico.json"
    try:
        client.bucket(bucket_name).blob(blob_name).upload_from_string(
            json.dumps(diag, ensure_ascii=False, indent=2, default=str),
            content_type="application/json"
        )
        return True
    except Exception:
        return False


def _load_all_diags_from_gcs(bucket_name: str, pozos: list[str], prefix: str) -> dict[str, dict]:
    client = _get_gcs_client()
    if not client:
        return {}
    results, bucket = {}, client.bucket(bucket_name)
    for no_key in pozos:
        blob_name = f"{prefix}/diagnosticos/{no_key}/diagnostico.json" if prefix else f"diagnosticos/{no_key}/diagnostico.json"
        try:
            blob = bucket.blob(blob_name)
            if blob.exists():
                data = json.loads(blob.download_as_text(encoding="utf-8"))
                if "error" not in data:
                    results[no_key] = data
        except Exception:
            pass
    return results


# ------------------------------------------------------------------ #
#  Parseo .din
# ------------------------------------------------------------------ #

def _read_text(path: str) -> str:
    p = Path(path)
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return p.read_text(encoding=enc, errors="strict")
        except Exception:
            pass
    return p.read_text(encoding="latin-1", errors="ignore")


def _safe_float(v) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    if "=" in s:
        s = s.split("=")[-1].strip()
    try:
        return float(s)
    except Exception:
        return None


def _parse_din_full(path_str: str) -> dict:
    import re
    SECTION_RE = re.compile(r"^\s*\[(.+?)\]\s*$")
    KV_RE      = re.compile(r"^\s*([^=]+?)\s*=\s*(.*?)\s*$")
    POINT_RE   = re.compile(r"^(X|Y)\s*(\d+)$", re.IGNORECASE)
    txt = _read_text(path_str)
    sections: dict[str, dict] = {}
    section, xs, ys, in_cs = None, {}, {}, False
    for line in txt.splitlines():
        m = SECTION_RE.match(line)
        if m:
            section = m.group(1).strip().upper()
            in_cs   = (section == "CS")
            sections.setdefault(section, {})
            continue
        m = KV_RE.match(line)
        if not m or not section:
            continue
        k, v = m.group(1).strip(), m.group(2).strip()
        if in_cs:
            mp = POINT_RE.match(k)
            if mp:
                xy, idx = mp.group(1).upper(), int(mp.group(2))
                try:
                    (xs if xy == "X" else ys)[idx] = float(v.replace(",", "."))
                except Exception:
                    pass
                continue
        sections[section][k] = v
    idxs = sorted(set(xs) & set(ys))
    return {"sections": sections, "cs_points": [{"X": xs[i], "Y": ys[i]} for i in idxs]}


def _extract_variables(parsed: dict) -> dict:
    secs = parsed.get("sections", {})
    def g(sec, key): return secs.get(sec.upper(), {}).get(key)
    v = {
        "Tipo_AIB": g("AIB","MA"), "Carrera_pulg": _safe_float(g("AIB","CS")),
        "Golpes_min": _safe_float(g("AIB","GM")), "Diam_piston_pulg": _safe_float(g("BOMBA","DP")),
        "Prof_bomba_m": _safe_float(g("BOMBA","PB")), "Llenado_pct": _safe_float(g("BOMBA","CA")),
        "PE_m": _safe_float(g("NIV","PE")), "PB_m": _safe_float(g("NIV","PB")),
        "NM_m": _safe_float(g("NIV","NM")), "NC_m": _safe_float(g("NIV","NC")),
        "ND_m": _safe_float(g("NIV","ND")), "Pct_estructura": _safe_float(g("RARE","SE")),
        "Pct_balance": _safe_float(g("RARR","PC")), "Caudal_bruto": _safe_float(g("RBO","CF")),
        "Potencia_motor": _safe_float(g("MOTOR","PN")), "RPM_motor": _safe_float(g("MOTOR","RM")),
    }
    pb = v.get("Prof_bomba_m")
    for nk in ["NC_m", "NM_m", "ND_m"]:
        nv = v.get(nk)
        if pb is not None and nv is not None:
            v["Sumergencia_m"]    = round(pb - nv, 1)
            v["Base_sumergencia"] = nk.replace("_m", "")
            break
    else:
        v["Sumergencia_m"] = None
        v["Base_sumergencia"] = None
    return v


def _describe_cs_shape(cs_points: list[dict]) -> str:
    if not cs_points:
        return "Sin datos CS."
    xs, ys = [p["X"] for p in cs_points], [p["Y"] for p in cs_points]
    n = len(cs_points)
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    carrera     = round(x_max - x_min, 1)
    rango_carga = round(y_max - y_min, 1)
    inversiones = sum(1 for i in range(1, n-1) if (ys[i]-ys[i-1])*(ys[i+1]-ys[i]) < 0)
    if (y_max > 0 and rango_carga/y_max < 0.03) or inversiones > n*0.40:
        return f"CARTA_DEGENERADA=True | n_puntos={n} | carrera={carrera}"
    area = abs(sum(xs[i]*ys[(i+1)%n] - xs[(i+1)%n]*ys[i] for i in range(n))) / 2
    rect = carrera * rango_carga
    fill = round(area/rect, 2) if rect > 0 else 0
    ratio = round(y_min/y_max, 3) if y_max > 0 else None
    return (f"n_puntos={n} | carrera={carrera} | carga_max={round(y_max,1)} | "
            f"fill_ratio={fill} | ratio_carga_min_max={ratio}")


# ------------------------------------------------------------------ #
#  OpenAI
# ------------------------------------------------------------------ #

def _build_prompt(no_key: str, mediciones: list[dict]) -> str:
    catalogo_str = "\n".join(f"  - {p}" for p in CATALOGO_PROBLEMATICAS)
    lineas = []
    for i, m in enumerate(mediciones):
        label = "Única medición" if len(mediciones)==1 else ["Más antigua","Intermedia","Más reciente"][min(i,2)]
        v = m["vars"]
        sumer = v.get("Sumergencia_m")
        if sumer is None: sumer_str = "N/D"
        elif sumer < 50:  sumer_str = f"{sumer} m — CRÍTICA"
        elif sumer < 150: sumer_str = f"{sumer} m — BAJA"
        elif sumer < 400: sumer_str = f"{sumer} m — NORMAL"
        else:             sumer_str = f"{sumer} m — ALTA"
        lineas.append(f"\n### [{label}] {m['fecha']}\n  Sumergencia: {sumer_str} | Llenado: {v.get('Llenado_pct','N/D')}% | Carta: {m['cs_shape']}")

    return f"""Ingeniero senior pozos petroleros. Analizá el pozo {no_key}.
Catálogo: {catalogo_str}
Mediciones: {"".join(lineas)}
Respondé SOLO con JSON válido:
{{"pozo":"{no_key}","fecha_analisis":"<ISO>","resumen":"<texto>","recomendacion":"<texto>","confianza":"<ALTA|MEDIA|BAJA>",
"mediciones":[{{"fecha":"<f>","label":"<l>","llenado_pct":<n>,"sumergencia_m":<n>,"sumergencia_nivel":"<n>","caudal_bruto":<n>,"pct_balance":<n>,
"problemáticas":[{{"nombre":"<n>","severidad":"<BAJA|MEDIA|ALTA|CRÍTICA>","estado":"<ACTIVA|RESUELTA>","descripcion":"<texto>"}}]}}]}}"""


def _call_openai(prompt: str, api_key: str) -> dict:
    from openai import OpenAI
    response = OpenAI(api_key=api_key).chat.completions.create(
        model="gpt-5.2-chat-latest",
        messages=[{"role": "user", "content": prompt}],
        max_completion_tokens=2500,
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.lower().startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip().rstrip("```"))


# ------------------------------------------------------------------ #
#  Generar / verificar diagnóstico
# ------------------------------------------------------------------ #

def generar_diagnostico(no_key, din_ok, resolve_path_fn, gcs_download_fn,
                        gcs_bucket, gcs_prefix, api_key, niv_ok=None) -> dict:
    din_p = din_ok[din_ok["NO_key"] == no_key].copy()
    if din_p.empty or "path" not in din_p.columns:
        return {"error": "Sin archivos DIN."}
    sort_cols = [c for c in ["din_datetime","mtime"] if c in din_p.columns]
    if sort_cols:
        din_p = din_p.sort_values(sort_cols, na_position="last")
    din_p = din_p.dropna(subset=["path"]).drop_duplicates(subset=["path"]).tail(3)

    mediciones = []
    for _, row in din_p.iterrows():
        p_res = resolve_path_fn(str(row.get("path","")))
        if not p_res:
            continue
        local = gcs_download_fn(p_res) if str(p_res).lower().startswith("gs://") else p_res
        try:
            parsed = _parse_din_full(local)
        except Exception:
            continue
        vars_  = _extract_variables(parsed)
        fecha  = row.get("din_datetime") or row.get("mtime") or "Desconocida"
        if hasattr(fecha, "strftime"):
            fecha = fecha.strftime("%Y-%m-%d %H:%M")
        mediciones.append({"fecha": str(fecha), "path": str(p_res), "vars": vars_,
                           "cs_shape": _describe_cs_shape(parsed.get("cs_points",[]))})

    if not mediciones:
        return {"error": "No se pudieron parsear archivos DIN."}

    try:
        diag = _call_openai(_build_prompt(no_key, mediciones), api_key)
    except Exception as e:
        return {"error": f"Error OpenAI: {e}"}

    for med in diag.get("mediciones", []):
        for p in med.get("problemáticas", []):
            p["estado"] = "RESUELTA" if str(p.get("estado","")).upper() == "RESUELTA" else "ACTIVA"

    diag["_meta"] = {
        "generado_utc": datetime.now(timezone.utc).isoformat(),
        "fecha_din_mas_reciente": mediciones[-1]["fecha"],
        "n_mediciones": len(mediciones),
        "schema_version": DIAG_SCHEMA_VERSION,
    }
    if gcs_bucket:
        _save_diag_to_gcs(gcs_bucket, no_key, diag, gcs_prefix)
    return diag


def _necesita_regenerar(diag, din_ok, no_key) -> bool:
    if not diag or "error" in diag:
        return True
    meta = diag.get("_meta", {})
    if meta.get("schema_version", 0) < DIAG_SCHEMA_VERSION:
        return True
    try:
        fecha_diag = pd.to_datetime(meta.get("generado_utc"), utc=True)
    except Exception:
        return True
    din_p = din_ok[din_ok["NO_key"] == no_key]
    if din_p.empty:
        return False
    sort_cols = [c for c in ["din_datetime","mtime"] if c in din_p.columns]
    if not sort_cols:
        return False
    latest = pd.to_datetime(din_p[sort_cols[0]], errors="coerce", utc=True).max()
    return not pd.isna(latest) and latest > fecha_diag


def _build_global_table(diags, bat_map, normalize_no_fn) -> pd.DataFrame:
    rows = []
    for no_key, diag in diags.items():
        bateria = bat_map.get(normalize_no_fn(no_key), "N/D")
        for med in diag.get("mediciones", []):
            probs  = med.get("problemáticas", [])
            probs_s = sorted(probs, key=lambda x: (
                0 if x.get("estado")=="ACTIVA" else 1,
                SEVERIDAD_ORDEN.get(x.get("severidad","BAJA"), 9)
            ))
            activas = [p for p in probs_s if p.get("estado")=="ACTIVA"]
            sev_max = (min(activas, key=lambda x: SEVERIDAD_ORDEN.get(x.get("severidad","BAJA"),9))
                       .get("severidad","BAJA") if activas else "NINGUNA")
            rows.append({
                "Pozo": no_key, "Batería": bateria,
                "Fecha DIN": med.get("fecha","?"), "Medición": med.get("label",""),
                "Llenado %": f"{med.get('llenado_pct')}%" if med.get("llenado_pct") is not None else "N/D",
                "Sumergencia": f"{med.get('sumergencia_m')} m" if med.get("sumergencia_m") is not None else "N/D",
                "Sev. máx": sev_max, "Act.": len(activas), "Res.": len(probs_s)-len(activas),
                "Recomendación": diag.get("recomendacion",""),
                "_prob_lista": [p.get("nombre","?") for p in probs_s],
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    sev_ord = {"CRÍTICA":0,"ALTA":1,"MEDIA":2,"BAJA":3,"NINGUNA":4}
    df["_sev_ord"] = df["Sev. máx"].map(sev_ord).fillna(9)
    return df.sort_values(["_sev_ord","Batería","Pozo"]).drop(columns=["_sev_ord"]).reset_index(drop=True)
