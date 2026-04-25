# Databricks notebook source
# ===========================================================
# 1_ingest_ecommerce.py  — BRONZE
# Ingesta del dataset E-Commerce Sales Prediction
# desde ADLS Gen2 usando Managed Identity.
# Config via dbutils.widgets (Job base_parameters).
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Ingesta E-Commerce Sales Prediction

# COMMAND ----------

from pyspark.sql import functions as F

# Parametros del Job (base_parameters) — con defaults para ejecucion manual en DEV
dbutils.widgets.text("storageName", "retaildatalakejp01")
dbutils.widgets.text("container",   "raw")
dbutils.widgets.text("catalogo",    "retail_project")

STORAGE_NAME = dbutils.widgets.get("storageName")
CONTAINER    = dbutils.widgets.get("container")
CATALOG      = dbutils.widgets.get("catalogo")

RAW_PATH     = f"abfss://{CONTAINER}@{STORAGE_NAME}.dfs.core.windows.net/"
SOURCE_FILE  = "Ecommerce_Sales_Prediction_Dataset.csv"

print(f"Storage  : {STORAGE_NAME}")
print(f"Catalog  : {CATALOG}")
print(f"Raw path : {RAW_PATH}")

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
print(f"[OK] Catalog activo: {CATALOG}")

# COMMAND ----------

# Leer CSV desde ADLS Gen2 con Managed Identity
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv(f"{RAW_PATH}{SOURCE_FILE}")
)

print(f"Filas leidas  : {df_raw.count():,}")
print(f"Columnas      : {df_raw.columns}")

# COMMAND ----------

# Agregar columnas de auditoria
df_bronze = (
    df_raw
    .withColumn("_ingestion_date", F.current_timestamp())
    .withColumn("_source_file",    F.lit(SOURCE_FILE))
    .withColumn("_batch_id",       F.lit("batch_001"))
)

display(df_bronze.limit(5))

# COMMAND ----------

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.bronze.ecommerce_raw")

print(f"[OK] Tabla escrita: {CATALOG}.bronze.ecommerce_raw")

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS total FROM {CATALOG}.bronze.ecommerce_raw"))
