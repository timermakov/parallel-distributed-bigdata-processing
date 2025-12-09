from typing import List
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType


SCHEMA = StructType([
    StructField("player_name", StringType(), False),
    StructField("salary", LongType(), False),
    StructField("season_start", IntegerType(), False),
    StructField("season_end", IntegerType(), False),
    StructField("team", StringType(), True),
    StructField("team_full_name", StringType(), True),
])


def load_salaries_from_excel(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path)
    df = df.rename(columns={
        "Player Name": "player_name",
        "Salary in $": "salary",
        "Season Start": "season_start",
        "Season End": "season_end",
        "Team": "team",
        "Full Team Name": "team_full_name",
    })
    if "Register Value" in df.columns:
        df = df.drop(columns=["Register Value"])
    return df


def get_available_years(df: pd.DataFrame) -> List[int]:
    return sorted(df["season_end"].unique().tolist())


def get_data_for_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    return df[df["season_end"] == year].copy()


def pandas_to_spark(spark: SparkSession, pdf: pd.DataFrame) -> DataFrame:
    """pandas DataFrame to Spark DataFrame with proper schema"""
    return spark.createDataFrame(pdf, schema=SCHEMA)

