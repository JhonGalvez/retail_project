# Databricks notebook source
# ===========================================================
# 3_load.py  — GOLD
# Genera 5 tablas KPI para el dashboard.
# Config via dbutils.widgets (Job base_parameters).
# ===========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — KPIs y Tablas para Dashboard

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "retail_project")
CATALOG = dbutils.widgets.get("catalogo")

spark.sql(f"USE CATALOG {CATALOG}")
print(f"[OK] Catalog: {CATALOG}")

# COMMAND ----------

ecommerce  = spark.table(f"{CATALOG}.silver.ecommerce_clean")
superstore = spark.table(f"{CATALOG}.silver.superstore_orders")

# COMMAND ----------

# MAGIC %md ### KPI 1 — Ventas por categoria y mes

# COMMAND ----------

gold_ventas = (
    ecommerce
    .groupBy("product_category", "year_month", "year", "month")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round(F.sum("revenue") / F.sum("units_sold"), 2).alias("avg_unit_price"),
        F.round(F.avg("discount_rate"), 4).alias("avg_discount_rate"),
        F.sum("units_sold").alias("total_units_sold"),
        F.round(F.sum("marketing_spend"), 2).alias("total_marketing_spend"),
        F.count("*").alias("num_registros"),
    )
    .withColumn("revenue_por_unidad",
                F.round(F.col("total_revenue") / F.col("total_units_sold"), 2))
    .orderBy("year_month", "total_revenue", ascending=[True, False])
)
gold_ventas.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.ventas_por_categoria")
print(f"[OK] {CATALOG}.gold.ventas_por_categoria")
display(gold_ventas.limit(10))

# COMMAND ----------

# MAGIC %md ### KPI 2 — Top 20 productos Superstore

# COMMAND ----------

gold_top = (
    superstore
    .groupBy("product_name", "aisle", "department")
    .agg(
        F.count("*").alias("total_pedidos"),
        F.sum(F.col("is_reorder").cast("int")).alias("total_reordenes"),
        F.countDistinct("user_id").alias("usuarios_unicos"),
        F.countDistinct("order_id").alias("ordenes_unicas"),
    )
    .withColumn("tasa_reorden",
                F.round(F.col("total_reordenes") / F.col("total_pedidos"), 3))
    .orderBy(F.desc("total_pedidos"))
    .limit(20)
)
gold_top.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.top_productos")
print(f"[OK] {CATALOG}.gold.top_productos")

# COMMAND ----------

# MAGIC %md ### KPI 3 — Comportamiento por segmento de cliente

# COMMAND ----------

gold_segmentos = (
    ecommerce
    .groupBy("customer_segment", "product_category")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue_por_registro"),
        F.round(F.avg("discount_rate"), 4).alias("avg_descuento"),
        F.sum("units_sold").alias("total_unidades"),
        F.count("*").alias("num_transacciones"),
    )
    .orderBy("customer_segment", F.desc("total_revenue"))
)
gold_segmentos.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.comportamiento_cliente")
print(f"[OK] {CATALOG}.gold.comportamiento_cliente")

# COMMAND ----------

# MAGIC %md ### KPI 4 — Eficiencia de marketing

# COMMAND ----------

gold_marketing = (
    ecommerce
    .groupBy("product_category")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round(F.sum("marketing_spend"), 2).alias("total_marketing"),
        F.sum("units_sold").alias("total_unidades"),
    )
    .withColumn("roi_marketing",
                F.round(F.col("total_revenue") / F.col("total_marketing"), 3))
    .orderBy(F.desc("roi_marketing"))
)
gold_marketing.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.eficiencia_marketing")
print(f"[OK] {CATALOG}.gold.eficiencia_marketing")

# COMMAND ----------

# MAGIC %md ### KPI 5 — Actividad por horario (Superstore)

# COMMAND ----------

gold_horario = (
    superstore
    .groupBy("order_hour_of_day", "day_name", "time_slot")
    .agg(
        F.countDistinct("order_id").alias("total_ordenes"),
        F.countDistinct("user_id").alias("usuarios_activos"),
        F.count("*").alias("total_items_pedidos"),
    )
    .withColumn("avg_items_por_orden",
                F.round(F.col("total_items_pedidos") / F.col("total_ordenes"), 2))
    .orderBy("order_hour_of_day")
)
gold_horario.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.actividad_por_horario")
print(f"[OK] {CATALOG}.gold.actividad_por_horario")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.gold"))
