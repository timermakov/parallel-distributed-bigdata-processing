from pyspark.sql import SparkSession

APP_NAME: str = "FibonacciApp"


def create_spark_session(master: str | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(APP_NAME)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()
