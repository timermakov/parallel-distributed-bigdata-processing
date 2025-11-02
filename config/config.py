from typing import Optional

from pyspark.sql import SparkSession

APP_NAME: str = "FibonacciApp"


def create_spark_session(master: Optional[str] = None, app_name: Optional[str] = None) -> SparkSession:
    """Create or reuse a SparkSession with optional master and app name overrides."""

    builder = SparkSession.builder.appName(app_name or APP_NAME)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()
