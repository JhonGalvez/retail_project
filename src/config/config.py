# =============================================================
# config.py — Configuracion central del proyecto
# El CATALOG se toma de variable de entorno para soportar
# ambientes DEV y PROD sin cambiar el codigo.
# =============================================================
import os

STORAGE_ACCOUNT  = "retaildatalakejp01"
CONTAINER_RAW    = "raw"

# DEV: retail_project | PROD: retail_project_prod
CATALOG          = os.getenv("CATALOG_NAME", "retail_project")

SCHEMAS = {
    "bronze": "bronze",
    "silver": "silver",
    "gold":   "gold",
}

# Rutas ADLS Gen2 (abfss) — Managed Identity, sin keys
def raw_path(filename: str) -> str:
    return (
        f"abfss://{CONTAINER_RAW}@{STORAGE_ACCOUNT}"
        f".dfs.core.windows.net/{filename}"
    )

# Limites de muestreo para archivos grandes (ahorra credito Azure)
SAMPLE_ORDERS = 200_000
SAMPLE_OP     = 500_000
