# ==========================================================
# components/validaciones_logic.py
# Lógica pura de validaciones — SIN dependencias de Streamlit.
# Extraída de validaciones_tab.py del repo original.
# ==========================================================

from __future__ import annotations
import json
import os
from datetime import datetime, timezone
import pandas as pd


def _get_gcs_client():
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception:
        return None


def _blob_name(no_key: str, prefix: str = "") -> str:
    name = f"validaciones/{no_key}/validaciones.json"
    return f"{prefix}/{name}" if prefix else name


def load_validaciones(bucket_name: str, no_key: str, prefix: str = "") -> dict:
    client = _get_gcs_client()
    if not client or not bucket_name:
        return {}
    try:
        blob = client.bucket(bucket_name).blob(_blob_name(no_key, prefix))
        return json.loads(blob.download_as_text(encoding="utf-8")) if blob.exists() else {}
    except Exception:
        return {}


def save_validaciones(bucket_name: str, no_key: str, data: dict, prefix: str = "") -> bool:
    client = _get_gcs_client()
    if not client or not bucket_name:
        return False
    try:
        client.bucket(bucket_name).blob(_blob_name(no_key, prefix)).upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            content_type="application/json"
        )
        return True
    except Exception:
        return False


def load_all_validaciones(bucket_name: str, pozos: list[str], prefix: str = "") -> dict[str, dict]:
    client = _get_gcs_client()
    if not client or not bucket_name:
        return {}
    results, bucket = {}, client.bucket(bucket_name)
    for no_key in pozos:
        try:
            blob = bucket.blob(_blob_name(no_key, prefix))
            if blob.exists():
                results[no_key] = json.loads(blob.download_as_text(encoding="utf-8"))
        except Exception:
            pass
    return results


def make_fecha_key(fecha) -> str:
    if hasattr(fecha, "strftime"):
        return fecha.strftime("%Y-%m-%d %H:%M")
    return str(fecha)[:16] if fecha else ""


def get_validacion(val_data: dict, fecha_key: str) -> dict:
    return val_data.get("mediciones", {}).get(fecha_key,
           {"validada": True, "comentario": "", "historial": []})


def set_validacion(val_data: dict, no_key: str, fecha_key: str,
                   validada: bool, comentario: str, usuario: str) -> dict:
    if "pozo" not in val_data:
        val_data["pozo"] = no_key
    val_data.setdefault("mediciones", {})
    entrada = val_data["mediciones"].get(fecha_key, {"historial": []})
    historial = entrada.get("historial", [])
    if entrada.get("validada") != validada or entrada.get("comentario","") != comentario:
        historial.append({
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "usuario":   usuario or "anónimo",
            "validada":  validada,
            "comentario": comentario,
        })
    val_data["mediciones"][fecha_key] = {
        "validada": validada, "comentario": comentario, "historial": historial
    }
    return val_data


def filtrar_por_validacion(snap_map: pd.DataFrame, gcs_bucket: str, gcs_prefix: str,
                           normalize_no_fn, solo_validadas: bool) -> pd.DataFrame:
    if not gcs_bucket:
        return snap_map
    pozos    = snap_map["NO_key"].dropna().unique().tolist()
    todas_val = load_all_validaciones(gcs_bucket, pozos, gcs_prefix)
    def es_valida(row):
        nk = normalize_no_fn(str(row.get("NO_key","")))
        fk = make_fecha_key(row.get("DT_plot"))
        return get_validacion(todas_val.get(nk,{}), fk).get("validada", True)
    mask = snap_map.apply(es_valida, axis=1)
    return snap_map[mask].copy() if solo_validadas else snap_map[~mask].copy()
