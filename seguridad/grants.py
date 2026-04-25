# Databricks notebook source
# ===========================================================
# grants.py  — SEGURIDAD
# Otorga permisos de lectura sobre las tablas Gold
# a usuarios del workspace (rol analitico).
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seguridad — Grants sobre capas Medallion
# MAGIC Otorga permisos de SELECT sobre Gold a usuarios analiticos.
# MAGIC Modifica ANALYST_GROUP con el grupo/usuario real del workspace.

# COMMAND ----------

import sys, os

DEV_PATH  = "/Workspace/Users/galvezsantosjhon@outlook.com/retail_project/src"
PROD_PATH = "/Workspace/Shared/retail_project_prod/src"
WS_SRC    = DEV_PATH if os.path.exists(DEV_PATH) else PROD_PATH
sys.path.insert(0, WS_SRC)
print(f"[OK] src path: {WS_SRC}")
from config.config import CATALOG

# Cambiar por el grupo o usuario al que se quiere dar acceso
ANALYST_GROUP = "account users"

print(f"Catalog : {CATALOG}")
print(f"Grupo   : {ANALYST_GROUP}")

# COMMAND ----------

# MAGIC %md ### Permisos sobre el Catalog

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{ANALYST_GROUP}`")
print(f"[OK] USE CATALOG → {ANALYST_GROUP}")

# COMMAND ----------

# MAGIC %md ### Permisos sobre los Schemas

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `{ANALYST_GROUP}`")
    print(f"[OK] USE SCHEMA {schema} → {ANALYST_GROUP}")

# COMMAND ----------

# MAGIC %md ### Permisos SELECT sobre tablas Gold (capa de consumo)

# COMMAND ----------

tablas_gold = [
    "ventas_por_categoria",
    "top_productos",
    "comportamiento_cliente",
    "eficiencia_marketing",
    "actividad_por_horario",
]
for tabla in tablas_gold:
    spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.gold.{tabla} TO `{ANALYST_GROUP}`")
    print(f"[OK] SELECT {CATALOG}.gold.{tabla} → {ANALYST_GROUP}")

# COMMAND ----------

# Verificar permisos otorgados
display(spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG}"))
