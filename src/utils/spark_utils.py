# =============================================================
# spark_utils.py — Funciones reutilizables para el ETL
# =============================================================

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def configure_managed_identity(spark: SparkSession, storage_account: str) -> None:
    """
    Configura la conexión a ADLS Gen2 usando Managed Identity.
    Esto cumple el requisito: 'solo debe ser con Managed Identity'.
    No requiere keys ni secrets — usa la MSI del cluster.
    """
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
    )
    spark.conf.set(
        "fs.azure.account.oauth2.msi.endpoint",
        "http://169.254.169.254/oauth2/token",
    )
    print(f"[OK] Managed Identity configurada para: {storage_account}")


def add_audit_columns(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """Agrega columnas de auditoria estandar a cualquier DataFrame."""
    return (
        df
        .withColumn("_ingestion_date", F.current_timestamp())
        .withColumn("_source_file",    F.lit(source_file))
        .withColumn("_batch_id",       F.lit(batch_id))
    )


def read_csv_adls(spark: SparkSession, path: str, sep: str = ",", sample: int = None) -> DataFrame:
    """
    Lee un CSV desde ADLS Gen2.
    - sep: ',' para ecommerce, ';' para superstore
    - sample: limite de filas para ahorrar computo en archivos de 1M+
    """
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", sep)
        .option("encoding", "UTF-8")
        .csv(path)
    )
    if sample:
        df = df.limit(sample)
    return df


def write_delta(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """Escribe un DataFrame como Delta Table en Unity Catalog."""
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    print(f"[OK] {table} escrita correctamente")


def null_report(df: DataFrame) -> None:
    """Imprime reporte de nulos por columna (util en Silver)."""
    total = df.count()
    print(f"\n--- Reporte de nulos ({total:,} filas) ---")
    for col in df.columns:
        nulls = df.filter(F.col(col).isNull()).count()
        pct   = round(nulls / total * 100, 1) if total > 0 else 0
        print(f"  {col}: {nulls:,} nulos ({pct}%)")
