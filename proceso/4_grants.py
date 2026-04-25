# Databricks notebook source
# ===========================================================
# 4_grants.py  — SEGURIDAD
# Otorga permisos de lectura sobre las tablas Gold
# a usuarios del workspace (rol analitico).
# Config via dbutils.widgets (Job base_parameters).
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grants — Permisos sobre capas Medallion

# COMMAND ----------

dbutils.widgets.text("catalogo", "retail_project")
CATALOG = dbutils.widgets.get("catalogo")

# Grupo o usuario al que se otorgan permisos de lectura
ANALYST_GROUP = "account users"

print(f"Catalog : {CATALOG}")
print(f"Grupo   : {ANALYST_GROUP}")

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# Permisos sobre el catalog
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{ANALYST_GROUP}`")
print(f"[OK] USE CATALOG → {ANALYST_GROUP}")

# COMMAND ----------

# Permisos sobre los schemas
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `{ANALYST_GROUP}`")
    print(f"[OK] USE SCHEMA {schema} → {ANALYST_GROUP}")

# COMMAND ----------

# Permisos SELECT sobre tablas Gold (capa de consumo)
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
