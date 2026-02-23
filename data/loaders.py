# ==========================================================
# data/loaders.py
# Carga de índices DIN y NIV desde GCS o local.
# Sin cambios respecto al Streamlit original — mismas fuentes de datos.
# ==========================================================

import os
import tempfile
from functools import lru_cache
from pathlib import Path

import pandas as pd

# ---------- Configuración GCS ----------
GCS_BUCKET = os.environ.get("DINAS_BUCKET", "").strip()
GCS_PREFIX = os.environ.get("DINAS_GCS_PREFIX", "").strip().strip("/")

PROYECTO_DIR = r"C:\Users\dgalizia\Desktop\Proyectos de IA\Interfaz Dinas"

INDEX_PARQUET_LOCAL = os.path.join(PROYECTO_DIR, "din_index.parquet")
INDEX_CSV_LOCAL     = os.path.join(PROYECTO_DIR, "din_index.csv")
NIV_INDEX_LOCAL     = os.path.join(PROYECTO_DIR, "niv_index.parquet")

BASE_DIR           = Path(__file__).resolve().parent.parent
COORDS_XLSX_REPO   = BASE_DIR / "assets" / "Nombres-Pozo_con_coordenadas.xlsx"

DATA_ROOTS = [
    r"O:\Petroleum\Upstream\Desarrollo Operativo\Mediciones Fisicas",
    PROYECTO_DIR,
]


def _gcs_join(*parts: str) -> str:
    parts = [p.strip("/").replace("\\", "/") for p in parts if p and str(p).strip()]
    suffix = "/".join(parts)
    if GCS_PREFIX:
        suffix = f"{GCS_PREFIX}/{suffix}"
    return f"gs://{GCS_BUCKET}/{suffix}"


def is_gs_path(p) -> bool:
    return bool(p) and str(p).strip().lower().startswith("gs://")


@lru_cache(maxsize=1)
def get_gcs_client():
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception:
        return None


def parse_gs_url(gs_url: str):
    u = gs_url.strip()[5:]
    bucket, _, blob = u.partition("/")
    return bucket, blob


def gcs_download_to_temp(gs_url: str) -> str:
    """Descarga un archivo GCS a /tmp y devuelve el path local. Reutiliza si ya existe."""
    client = get_gcs_client()
    if client is None:
        raise RuntimeError("google-cloud-storage no disponible.")

    bucket_name, blob_name = parse_gs_url(gs_url)
    safe_name  = blob_name.replace("/", "__")
    local_path = os.path.join(tempfile.gettempdir(), safe_name)

    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return local_path

    client.bucket(bucket_name).blob(blob_name).download_to_filename(local_path)
    return local_path


def map_local_datastore_to_gcs(path_str) -> str | None:
    if not path_str or not GCS_BUCKET:
        return None
    p   = str(path_str).replace("\\", "/")
    idx = p.lower().find("/data_store/")
    if idx == -1:
        return None
    return _gcs_join(p[idx + 1:])


def resolve_existing_path(path_str) -> str | None:
    """Devuelve path local si existe, o gs:// si estamos en Cloud Run."""
    if not path_str:
        return None
    p = str(path_str).strip()

    if is_gs_path(p):
        return p
    if Path(p).exists():
        return p

    # Buscar por nombre de archivo en roots locales
    fname = Path(p).name
    for root in DATA_ROOTS:
        rootp = Path(root)
        if not rootp.exists():
            continue
        try:
            found = next(rootp.rglob(fname), None)
            if found and found.exists():
                return str(found)
        except Exception:
            pass

    # Cloud: mapear data_store → gs://
    return map_local_datastore_to_gcs(p)


# ---------- Loaders principales (con cache en memoria) ----------

@lru_cache(maxsize=1)
def load_din_index() -> pd.DataFrame:
    if os.path.exists(INDEX_PARQUET_LOCAL):
        try:
            return pd.read_parquet(INDEX_PARQUET_LOCAL)
        except Exception:
            pass
    if os.path.exists(INDEX_CSV_LOCAL):
        return pd.read_csv(INDEX_CSV_LOCAL, parse_dates=["mtime", "din_datetime"],
                           dayfirst=True, keep_default_na=True)
    if GCS_BUCKET:
        try:
            lp = gcs_download_to_temp(_gcs_join("din_index.parquet"))
            return pd.read_parquet(lp)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


@lru_cache(maxsize=1)
def load_niv_index() -> pd.DataFrame:
    if os.path.exists(NIV_INDEX_LOCAL):
        return pd.read_parquet(NIV_INDEX_LOCAL)
    if GCS_BUCKET:
        try:
            lp = gcs_download_to_temp(_gcs_join("niv_index.parquet"))
            return pd.read_parquet(lp)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


@lru_cache(maxsize=1)
def load_coords_repo() -> pd.DataFrame:
    candidates = [
        COORDS_XLSX_REPO,
        Path.cwd() / "assets" / "Nombres-Pozo_con_coordenadas.xlsx",
        Path("/app/assets/Nombres-Pozo_con_coordenadas.xlsx"),
    ]
    for p in candidates:
        try:
            if Path(p).exists():
                return pd.read_excel(p)
        except Exception:
            pass
    hits = list(BASE_DIR.rglob("Nombres-Pozo_con_coordenadas.xlsx"))
    if hits:
        return pd.read_excel(hits[0])
    return pd.DataFrame()
