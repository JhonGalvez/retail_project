# Databricks notebook source
# ===========================================================
# 2_transform_ecommerce.py  — SILVER
# Limpieza y transformacion del dataset E-Commerce.
# Config via dbutils.widgets (Job base_parameters).
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — E-Commerce Sales Prediction

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

dbutils.widgets.text("catalogo", "retail_project")
CATALOG = dbutils.widgets.get("catalogo")

spark.sql(f"USE CATALOG {CATALOG}")
print(f"[OK] Catalog: {CATALOG}")

# COMMAND ----------

df = spark.table(f"{CATALOG}.bronze.ecommerce_raw")
print(f"Filas Bronze : {df.count():,}")

# COMMAND ----------

df_silver = (
    df
    .dropDuplicates()
    .filter(
        F.col("Date").isNotNull() &
        F.col("Product_Category").isNotNull() &
        F.col("Units_Sold").isNotNull() &
        (F.col("Units_Sold").cast(IntegerType()) > 0) &
        (F.col("Price").cast(DoubleType()) > 0)
    )
    .select(
        F.to_date(F.col("Date"), "dd-MM-yyyy").alias("order_date"),
        F.lower(F.col("Product_Category")).alias("product_category"),
        F.lower(F.col("Customer_Segment")).alias("customer_segment"),
        F.col("Price").cast(DoubleType()).alias("unit_price"),
        F.col("Discount").cast(DoubleType()).alias("discount_amount"),
        F.col("Marketing_Spend").cast(DoubleType()).alias("marketing_spend"),
        F.col("Units_Sold").cast(IntegerType()).alias("units_sold"),
        F.col("_ingestion_date"),
        F.col("_source_file"),
        F.col("_batch_id"),
    )
    .withColumn("revenue",       F.round(F.col("unit_price") * F.col("units_sold"), 2))
    .withColumn("discount_rate", F.round(F.col("discount_amount") / F.col("unit_price"), 4))
    .withColumn("year_month",    F.date_format("order_date", "yyyy-MM"))
    .withColumn("year",          F.year("order_date"))
    .withColumn("month",         F.month("order_date"))
)

# COMMAND ----------

print(f"Filas Silver : {df_silver.count():,}")
display(df_silver.limit(5))

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.ecommerce_clean")
print(f"[OK] {CATALOG}.silver.ecommerce_clean")

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_filas,
           COUNT(DISTINCT product_category) AS categorias,
           MIN(order_date) AS fecha_min,
           MAX(order_date) AS fecha_max,
           ROUND(SUM(revenue), 2) AS revenue_total
    FROM {CATALOG}.silver.ecommerce_clean
"""))
