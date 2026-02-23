# ==========================================================
# data/consolidado.py
# Lógica de consolidación DIN + NIV — extraída del app.py original.
# Se llama una sola vez y se cachea en memoria (no recalcula en cada click).
# ==========================================================

import pandas as pd

from utils.helpers import (
    make_unique_columns, safe_to_float, compute_sumergencia_and_base,
    infer_dt_plot, normalize_no_exact
)


def dedup_niv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    sort_cols = [c for c in ["niv_datetime", "mtime"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, na_position="last")
    return out.drop_duplicates(subset=["NO_key", "FE_key", "HO_key"], keep="last").reset_index(drop=True)


def dedup_din(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    sort_cols = [c for c in ["din_datetime", "mtime"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, na_position="last")
    if "path" in out.columns:
        return out.drop_duplicates(subset=["path"], keep="last").reset_index(drop=True)
    return out.drop_duplicates(subset=["NO_key", "FE_key", "HO_key"], keep="last").reset_index(drop=True)


def build_global_consolidated(
    din_ok: pd.DataFrame, niv_ok: pd.DataFrame,
    din_no_col, din_fe_col, din_ho_col,
    niv_no_col, niv_fe_col, niv_ho_col,
) -> pd.DataFrame:
    """
    Une DIN + NIV en un DataFrame global.
    Se ejecuta una vez y el resultado se cachea en el servidor — 
    no se recalcula por cada interacción del usuario.
    """
    din_d = dedup_din(din_ok)  if din_ok  is not None else pd.DataFrame()
    niv_d = dedup_niv(niv_ok) if niv_ok is not None else pd.DataFrame()

    din_join = din_d.copy()
    if not din_join.empty:
        din_join["ORIGEN"] = "DIN"
        if not niv_d.empty:
            din_join = din_join.merge(
                niv_d, on=["NO_key", "FE_key", "HO_key"],
                how="left", suffixes=("", "_niv")
            )

    niv_only = pd.DataFrame()
    if not niv_d.empty:
        if din_d.empty:
            niv_only = niv_d.copy()
        else:
            key_din  = din_d[["NO_key", "FE_key", "HO_key"]].drop_duplicates()
            niv_only = niv_d.merge(key_din, on=["NO_key", "FE_key", "HO_key"],
                                   how="left", indicator=True)
            niv_only = niv_only[niv_only["_merge"] == "left_only"].drop(columns=["_merge"])
        if not niv_only.empty:
            niv_only["ORIGEN"] = "NIV"

    dfp = pd.concat([din_join, niv_only], ignore_index=True, sort=False)
    dfp = make_unique_columns(dfp)

    if not dfp.empty:
        for src_col, dst_col in [
            (din_no_col or niv_no_col, "pozo"),
            (din_fe_col or niv_fe_col, "fecha"),
            (din_ho_col or niv_ho_col, "hora"),
        ]:
            if src_col and src_col in dfp.columns:
                dfp[dst_col] = dfp[src_col]
            elif dst_col not in dfp.columns:
                key_map = {"pozo": "NO_key", "fecha": "FE_key", "hora": "HO_key"}
                dfp[dst_col] = dfp.get(key_map[dst_col], "")

    for c in ["CO", "empresa", "SE", "NM", "NC", "ND", "PE", "PB", "CM", "niv_datetime"]:
        if c not in dfp.columns and f"{c}_niv" in dfp.columns:
            dfp[c] = dfp[f"{c}_niv"]

    for c in ["NM", "NC", "ND", "PE", "PB"]:
        if c in dfp.columns:
            dfp[c] = dfp[c].apply(safe_to_float)

    _sumer = dfp.apply(compute_sumergencia_and_base, axis=1)
    dfp["Sumergencia"]      = _sumer.apply(lambda x: x[0] if isinstance(x, tuple) else None)
    dfp["Sumergencia_base"] = _sumer.apply(lambda x: x[1] if isinstance(x, tuple) else None)
    dfp["DT_plot"]          = infer_dt_plot(dfp)

    return dfp


def build_last_snapshot_for_map(din_ok: pd.DataFrame, niv_ok: pd.DataFrame) -> pd.DataFrame:
    """1 fila por pozo con la última medición (DIN o NIV)."""

    def _prep(df, origen, dt_candidates):
        if df is None or df.empty or "NO_key" not in df.columns:
            return pd.DataFrame()
        keep = [c for c in ["NO_key", "mtime", "din_datetime", "niv_datetime",
                             "FE_key", "HO_key", "PB", "NM", "NC", "ND", "PE"] if c in df.columns]
        d = df[keep].copy()
        d["NO_key"]  = d["NO_key"].astype(str).str.strip()
        d = d[d["NO_key"] != ""]
        d["ORIGEN"]  = origen
        for col in dt_candidates:
            if col in d.columns:
                s = pd.to_datetime(d[col], errors="coerce")
                if not s.isna().all():
                    d["DT_plot"] = s
                    break
        else:
            d["DT_plot"] = pd.NaT
        for c in ["PB", "NM", "NC", "ND", "PE"]:
            if c not in d.columns:
                d[c] = None
            d[c] = pd.to_numeric(d[c], errors="coerce")
        d = d.sort_values(["NO_key", "DT_plot"], na_position="last")
        last = d.groupby("NO_key", as_index=False).tail(1).copy()
        return last[["NO_key", "ORIGEN", "DT_plot", "PB", "NM", "NC", "ND", "PE"]]

    din_last = _prep(din_ok, "DIN", ["din_datetime", "mtime"])
    niv_last = _prep(niv_ok, "NIV", ["niv_datetime", "mtime"])
    both = pd.concat([din_last, niv_last], ignore_index=True, sort=False)
    if both.empty:
        return pd.DataFrame()
    both = both.sort_values(["NO_key", "DT_plot"], na_position="last")
    snap = both.groupby("NO_key", as_index=False).tail(1).copy()
    _sumer = snap.apply(compute_sumergencia_and_base, axis=1)
    snap["Sumergencia"]      = _sumer.apply(lambda x: x[0] if isinstance(x, tuple) else None)
    snap["Sumergencia_base"] = _sumer.apply(lambda x: x[1] if isinstance(x, tuple) else None)
    return snap.reset_index(drop=True)
