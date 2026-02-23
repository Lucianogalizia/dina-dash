# ==========================================================
# data/parsers.py
# Parseo de archivos .din — sin cambios respecto al original.
# ==========================================================

import re
from functools import lru_cache
from pathlib import Path

import pandas as pd

from data.loaders import is_gs_path, gcs_download_to_temp

SECTION_RE  = re.compile(r"^\s*\[(.+?)\]\s*$")
KV_RE       = re.compile(r"^\s*([^=]+?)\s*=\s*(.*?)\s*$")
POINT_KEY_RE = re.compile(r"^(X|Y)\s*(\d+)$", re.IGNORECASE)

EXTRA_FIELDS = {
    "Tipo AIB":                   ("AIB",       "MA"),
    "AIB Carrera":                ("AIB",       "CS"),
    "Sentido giro":               ("AIB",       "SG"),
    "Tipo Contrapesos":           ("CONTRAPESO", "TP"),
    "Distancia contrapesos (cm)": ("CONTRAPESO", "DE"),
    "Contrapeso actual":          ("RARE",       "CA"),
    "Contrapeso ideal":           ("RARE",       "CM"),
    "AIBEB_Torque max contrapeso":("RAEB",       "TM"),
    "%Estructura":                ("RARE",       "SE"),
    "%Balance":                   ("RARR",       "PC"),
    "Bba Diam Pistón":            ("BOMBA",      "DP"),
    "Bba Prof":                   ("BOMBA",      "PB"),
    "Bba Llenado":                ("BOMBA",      "CA"),
    "GPM":                        ("AIB",        "GM"),
    "Caudal bruto efec":          ("RBO",        "CF"),
    "Polea Motor":                ("MOTOR",      "DP"),
    "Potencia Motor":             ("MOTOR",      "PN"),
    "RPM Motor":                  ("MOTOR",      "RM"),
}


def read_text_best_effort(path: Path) -> str:
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return path.read_text(encoding=enc, errors="strict")
        except Exception:
            pass
    return path.read_text(encoding="latin-1", errors="ignore")


def _resolve_local(path_str: str) -> str:
    """Si es gs://, descarga primero. Devuelve path local."""
    if is_gs_path(path_str):
        return gcs_download_to_temp(path_str)
    return path_str


@lru_cache(maxsize=512)
def parse_din_surface_points(path_str: str) -> pd.DataFrame:
    local = _resolve_local(path_str)
    txt   = read_text_best_effort(Path(local))

    section = None
    xs, ys  = {}, {}
    in_cs   = False

    for line in txt.splitlines():
        m = SECTION_RE.match(line)
        if m:
            section = m.group(1).strip().upper()
            in_cs   = (section == "CS")
            continue
        m = KV_RE.match(line)
        if not m or not section or not in_cs:
            continue
        k_raw, v_raw = m.group(1).strip(), m.group(2).strip()
        mk = POINT_KEY_RE.match(k_raw)
        if mk:
            xy  = mk.group(1).upper()
            idx = int(mk.group(2))
            try:
                val = float(v_raw.replace(",", "."))
            except Exception:
                continue
            (xs if xy == "X" else ys)[idx] = val

    idxs = sorted(set(xs) & set(ys))
    return pd.DataFrame({"i": idxs, "X": [xs[i] for i in idxs], "Y": [ys[i] for i in idxs]})


@lru_cache(maxsize=512)
def parse_din_extras(path_str: str) -> dict:
    local = _resolve_local(path_str)
    txt   = read_text_best_effort(Path(local))

    wanted  = {(s.upper(), k.upper()): col for col, (s, k) in EXTRA_FIELDS.items()}
    out     = {col: None for col in EXTRA_FIELDS}
    section = None

    for line in txt.splitlines():
        m = SECTION_RE.match(line)
        if m:
            section = m.group(1).strip().upper()
            continue
        m = KV_RE.match(line)
        if not m or not section:
            continue
        k = m.group(1).strip().upper()
        v = m.group(2).strip()
        if (section, k) in wanted:
            col = wanted[(section, k)]
            from utils.helpers import safe_to_float
            fv = safe_to_float(v)
            out[col] = fv if fv is not None else (v if v else None)

    return out


def parse_extras_for_paths(paths: list[str]) -> pd.DataFrame:
    rows = []
    for pth in paths:
        try:
            rows.append(parse_din_extras(str(pth)) if pth else {k: None for k in EXTRA_FIELDS})
        except Exception:
            rows.append({k: None for k in EXTRA_FIELDS})
    return pd.DataFrame(rows)
