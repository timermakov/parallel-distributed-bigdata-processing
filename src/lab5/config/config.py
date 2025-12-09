"""Spark session configuration with Apache Iceberg support."""

from pyspark.sql import SparkSession

APP_NAME = "NBAPlayerSalariesIceberg"
CATALOG_NAME = "local"
DATABASE_NAME = "nba"
TABLE_NAME = "player_salaries"

# path for SQL queries
FULL_TABLE_NAME = f"{CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}"


def create_spark_session(warehouse_path: str = "/app/warehouse") -> SparkSession:
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse_path)
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        .getOrCreate()
    )

