# ==========================================================
# utils/helpers.py
# Funciones utilitarias — copiadas del original sin cambios.
# ==========================================================

import re
from datetime import datetime

import pandas as pd


SECTION_RE = re.compile(r"^\s*\[(.+?)\]\s*$")
KV_RE      = re.compile(r"^\s*([^=]+?)\s*=\s*(.*?)\s*$")


def normalize_no_exact(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if not s or s.upper() in ("<NA>", "NAN", "NONE"):
        return ""
    s = s.replace("–", "-").replace("—", "-").replace("−", "-")
    s = re.sub(r"\s+", "", s)
    return s.casefold().upper()


def normalize_fe_date(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return pd.to_datetime(x).date()
    s = str(x).strip()
    if not s:
        return None
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return dt.date() if not pd.isna(dt) else None


def normalize_ho_str(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if not s:
        return ""
    try:
        t = pd.to_datetime(s, errors="coerce").time()
        if t:
            return f"{t.hour:02d}:{t.minute:02d}"
    except Exception:
        pass
    m = re.match(r"^(\d{1,2}):(\d{2})", s)
    if m:
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    return s


def safe_to_float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    if "=" in s:
        s = s.split("=")[-1].strip()
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def find_col(df: pd.DataFrame, candidates):
    if df is None or df.empty:
        return None
    cols_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    return None


def make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    seen, new_cols = {}, []
    for c in df.columns:
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}_dup{seen[c]}")
    df.columns = new_cols
    return df


def build_keys(df: pd.DataFrame, no_col: str, fe_col: str, ho_col: str | None) -> pd.DataFrame:
    df = df.copy()
    df["NO_key"] = df[no_col].apply(normalize_no_exact) if no_col in df.columns else ""
    df["FE_key"] = df[fe_col].apply(normalize_fe_date)  if fe_col in df.columns else None
    df["HO_key"] = df[ho_col].apply(normalize_ho_str)   if (ho_col and ho_col in df.columns) else ""
    return df


def compute_sumergencia_and_base(row):
    pb = safe_to_float(row.get("PB"))
    if pb is None:
        return None, None
    for key, label in [("NC", "NC"), ("NM", "NM"), ("ND", "ND")]:
        val = safe_to_float(row.get(key))
        if val is not None:
            return pb - val, label
    return None, None


def make_display_label(row) -> str:
    parts = [str(row.get("fecha", "")), str(row.get("hora", "")), str(row.get("ORIGEN", ""))]
    parts = [p for p in parts if p and p not in ("None", "nan")]
    return " | ".join(parts) if parts else "SIN_FECHA"


def infer_dt_plot(dfp: pd.DataFrame) -> pd.Series:
    for col in ["din_datetime", "niv_datetime"]:
        if col in dfp.columns:
            dt = pd.to_datetime(dfp[col], errors="coerce")
            if not dt.isna().all():
                return dt
    if "FE_key" in dfp.columns:
        try:
            ho = dfp.get("HO_key", "").astype(str)
            return pd.to_datetime(dfp["FE_key"].astype(str) + " " + ho, errors="coerce", dayfirst=True)
        except Exception:
            pass
    return pd.Series([pd.NaT] * len(dfp))


def trend_linear_per_month(df_one: pd.DataFrame, ycol: str):
    if df_one is None or df_one.empty or ycol not in df_one.columns or "DT_plot" not in df_one.columns:
        return None
    d = df_one[["DT_plot", ycol]].dropna().copy()
    if d.empty:
        return None
    d["DT_plot"] = pd.to_datetime(d["DT_plot"], errors="coerce")
    d = d.dropna(subset=["DT_plot"]).sort_values("DT_plot")
    if d.shape[0] < 2:
        return None
    t0      = d["DT_plot"].iloc[0]
    x_days  = (d["DT_plot"] - t0).dt.total_seconds() / 86400.0
    x_months = x_days / 30.4375
    y = pd.to_numeric(d[ycol], errors="coerce")
    good = (~x_months.isna()) & (~y.isna())
    x, yv = x_months[good].to_numpy(), y[good].to_numpy()
    if len(x) < 2:
        return None
    x_mean, y_mean = x.mean(), yv.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return None
    b = ((x - x_mean) * (yv - y_mean)).sum() / denom
    return float(b), float(yv[0]), float(yv[-1]), int(len(x))
