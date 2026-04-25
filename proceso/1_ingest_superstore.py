# Databricks notebook source
# ===========================================================
# 1_ingest_superstore.py  — BRONZE
# Ingesta del Supermarket/Superstore Bundle (6 archivos CSV)
# desde ADLS Gen2 usando Managed Identity.
# Config via dbutils.widgets (Job base_parameters).
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Ingesta Superstore Bundle (6 archivos)

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("storageName", "retaildatalakejp01")
dbutils.widgets.text("container",   "raw")
dbutils.widgets.text("catalogo",    "retail_project")

STORAGE_NAME = dbutils.widgets.get("storageName")
CONTAINER    = dbutils.widgets.get("container")
CATALOG      = dbutils.widgets.get("catalogo")
RAW_PATH     = f"abfss://{CONTAINER}@{STORAGE_NAME}.dfs.core.windows.net/"

print(f"Storage  : {STORAGE_NAME}")
print(f"Catalog  : {CATALOG}")

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
print(f"[OK] Catalog activo: {CATALOG}")

# COMMAND ----------

def read_csv(path, sep=",", sample=None):
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("sep", sep)
          .csv(path))
    return df.limit(sample) if sample else df

def write_bronze(df, table, source_file):
    df = (df.withColumn("_ingestion_date", F.current_timestamp())
            .withColumn("_source_file",    F.lit(source_file))
            .withColumn("_batch_id",       F.lit("batch_001")))
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"[OK] {table}")

# COMMAND ----------

# orders.csv — 1M+ filas → muestra 200K para ahorrar credito Azure
write_bronze(
    read_csv(f"{RAW_PATH}orders.csv", sep=";", sample=200000),
    f"{CATALOG}.bronze.orders_raw", "orders.csv"
)

# COMMAND ----------

# order_products__prior.csv — muestra 500K
write_bronze(
    read_csv(f"{RAW_PATH}order_products__prior.csv", sep=";", sample=500000),
    f"{CATALOG}.bronze.order_products_prior_raw", "order_products__prior.csv"
)

# COMMAND ----------

# order_products__train.csv — muestra 500K
write_bronze(
    read_csv(f"{RAW_PATH}order_products__train.csv", sep=";", sample=500000),
    f"{CATALOG}.bronze.order_products_train_raw", "order_products__train.csv"
)

# COMMAND ----------

# products.csv — catalogo completo (49K filas)
write_bronze(
    read_csv(f"{RAW_PATH}products.csv", sep=";"),
    f"{CATALOG}.bronze.products_raw", "products.csv"
)

# COMMAND ----------

# aisles.csv — 134 filas
write_bronze(
    read_csv(f"{RAW_PATH}aisles.csv", sep=";"),
    f"{CATALOG}.bronze.aisles_raw", "aisles.csv"
)

# COMMAND ----------

# departments.csv — 21 filas
write_bronze(
    read_csv(f"{RAW_PATH}departments.csv", sep=";"),
    f"{CATALOG}.bronze.departments_raw", "departments.csv"
)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.bronze"))
