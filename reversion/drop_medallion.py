# Databricks notebook source
# ===========================================================
# drop_medallion.py  — REVERSION
# Elimina todas las tablas y schemas del catalog.
# USAR CON PRECAUCION — solo para reset completo del ambiente.
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reversion — Drop Medallion
# MAGIC ⚠️ **Precaucion**: Elimina todas las tablas Bronze, Silver y Gold.
# MAGIC Usar solo para reset completo del ambiente DEV.

# COMMAND ----------

import sys, os

DEV_PATH  = "/Workspace/Users/galvezsantosjhon@outlook.com/retail_project/src"
PROD_PATH = "/Workspace/Shared/retail_project_prod/src"
WS_SRC    = DEV_PATH if os.path.exists(DEV_PATH) else PROD_PATH
sys.path.insert(0, WS_SRC)
print(f"[OK] src path: {WS_SRC}")
from config.config import CATALOG

print(f"Catalog a limpiar: {CATALOG}")
print("Iniciando proceso de reversion...")

# COMMAND ----------

# Drop tablas Gold
tablas_gold = [
    "ventas_por_categoria",
    "top_productos",
    "comportamiento_cliente",
    "eficiencia_marketing",
    "actividad_por_horario",
]
for tabla in tablas_gold:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.gold.{tabla}")
    print(f"[DROP] {CATALOG}.gold.{tabla}")

# COMMAND ----------

# Drop tablas Silver
tablas_silver = ["ecommerce_clean", "superstore_orders"]
for tabla in tablas_silver:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.silver.{tabla}")
    print(f"[DROP] {CATALOG}.silver.{tabla}")

# COMMAND ----------

# Drop tablas Bronze
tablas_bronze = [
    "ecommerce_raw",
    "orders_raw",
    "order_products_prior_raw",
    "order_products_train_raw",
    "products_raw",
    "aisles_raw",
    "departments_raw",
]
for tabla in tablas_bronze:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.bronze.{tabla}")
    print(f"[DROP] {CATALOG}.bronze.{tabla}")

# COMMAND ----------

# Drop schemas
for schema in ["gold", "silver", "bronze"]:
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema}")
    print(f"[DROP] Schema: {CATALOG}.{schema}")

# COMMAND ----------

print("\n[OK] Reversion completa.")
print(f"El catalog '{CATALOG}' ha sido limpiado.")
print("Ejecuta 0_preparacion_ambiente.py para reiniciar.")
