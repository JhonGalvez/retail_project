# Databricks notebook source
# ===========================================================
# 2_transform_superstore.py  — SILVER
# Une 5 tablas Bronze en una tabla Silver enriquecida.
# Config via dbutils.widgets (Job base_parameters).
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Superstore (join de 5 tablas)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

dbutils.widgets.text("catalogo", "retail_project")
CATALOG = dbutils.widgets.get("catalogo")

spark.sql(f"USE CATALOG {CATALOG}")
print(f"[OK] Catalog: {CATALOG}")

# COMMAND ----------

orders      = spark.table(f"{CATALOG}.bronze.orders_raw")
op_prior    = spark.table(f"{CATALOG}.bronze.order_products_prior_raw")
op_train    = spark.table(f"{CATALOG}.bronze.order_products_train_raw")
products    = spark.table(f"{CATALOG}.bronze.products_raw")
aisles      = spark.table(f"{CATALOG}.bronze.aisles_raw")
departments = spark.table(f"{CATALOG}.bronze.departments_raw")

# COMMAND ----------

df_op = op_prior.union(op_train).dropDuplicates(["order_id", "product_id"])
print(f"order_products combinados: {df_op.count():,}")

# COMMAND ----------

orders_clean = (
    orders
    .dropDuplicates(["order_id"])
    .filter(F.col("order_id").isNotNull() & F.col("user_id").isNotNull())
    .withColumn("order_id",               F.col("order_id").cast(IntegerType()))
    .withColumn("user_id",                F.col("user_id").cast(IntegerType()))
    .withColumn("order_number",           F.col("order_number").cast(IntegerType()))
    .withColumn("order_dow",              F.col("order_dow").cast(IntegerType()))
    .withColumn("order_hour_of_day",      F.col("order_hour_of_day").cast(IntegerType()))
    .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("double"))
    .withColumn("day_name",
        F.when(F.col("order_dow") == 0, "Sunday")
         .when(F.col("order_dow") == 1, "Monday")
         .when(F.col("order_dow") == 2, "Tuesday")
         .when(F.col("order_dow") == 3, "Wednesday")
         .when(F.col("order_dow") == 4, "Thursday")
         .when(F.col("order_dow") == 5, "Friday")
         .otherwise("Saturday"))
    .withColumn("time_slot",
        F.when(F.col("order_hour_of_day").between(6,  11), "morning")
         .when(F.col("order_hour_of_day").between(12, 17), "afternoon")
         .when(F.col("order_hour_of_day").between(18, 21), "evening")
         .otherwise("night"))
    .drop("_ingestion_date", "_source_file", "_batch_id")
)

# COMMAND ----------

df_silver = (
    df_op
    .join(orders_clean, df_op["order_id"] == orders_clean["order_id"], "inner")
    .drop(orders_clean["order_id"])
    .join(products.select("product_id", "product_name", "aisle_id", "department_id"), on="product_id", how="left")
    .join(aisles.select("aisle_id", "aisle"), on="aisle_id", how="left")
    .join(departments.select("department_id", "department"), on="department_id", how="left")
    .drop("_ingestion_date", "_source_file", "_batch_id")
    .withColumn("is_reorder", F.col("reordered").cast("boolean"))
    .drop("reordered")
)

print(f"Filas Silver Superstore: {df_silver.count():,}")
display(df_silver.limit(5))

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.superstore_orders")
print(f"[OK] {CATALOG}.silver.superstore_orders")

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_lineas,
           COUNT(DISTINCT order_id) AS ordenes_unicas,
           COUNT(DISTINCT user_id)  AS usuarios_unicos,
           COUNT(DISTINCT department) AS departamentos,
           COUNT(DISTINCT product_name) AS productos_unicos
    FROM {CATALOG}.silver.superstore_orders
"""))
