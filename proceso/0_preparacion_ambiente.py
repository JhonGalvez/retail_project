# Databricks notebook source
# ===========================================================
# 0_preparacion_ambiente.py
# Crea el catalog y schemas en Unity Catalog.
# Config via dbutils.widgets (Job base_parameters).
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion de Ambiente — Unity Catalog

# COMMAND ----------

dbutils.widgets.text("storageName", "retaildatalakejp01")
dbutils.widgets.text("catalogo",    "retail_project")
dbutils.widgets.text("container",   "raw")

STORAGE_NAME     = dbutils.widgets.get("storageName")
CATALOG          = dbutils.widgets.get("catalogo")
CONTAINER        = dbutils.widgets.get("container")
MANAGED_LOCATION = f"abfss://{CONTAINER}@{STORAGE_NAME}.dfs.core.windows.net/"

print(f"Ambiente : {'PROD' if 'prod' in CATALOG else 'DEV'}")
print(f"Catalog  : {CATALOG}")
print(f"Location : {MANAGED_LOCATION}")

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG}
    MANAGED LOCATION '{MANAGED_LOCATION}'
""")
spark.sql(f"USE CATALOG {CATALOG}")
print(f"[OK] Catalog: {CATALOG}")

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"[OK] Schema: {CATALOG}.{schema}")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
