from pyspark.sql import SparkSession

APP_NAME: str = "NBAPlayerEfficiencyApp"


def create_spark_session(master: str) -> SparkSession:
    builder = SparkSession.builder.appName(APP_NAME)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()

