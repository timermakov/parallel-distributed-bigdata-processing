"""Data loading and validation for NBA datasets."""

from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def load_players(spark: SparkSession, path: str) -> DataFrame:
    """Load players dataset with player ID and full name."""
    # Read with inferred schema
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    # Select only _id and name columns, rename them
    # _id: player ID (e.g., "abdelal01")
    # name: full player name (e.g., "Alaa Abdelnaby")
    df = df.select(
        F.col("_id").alias("player_id"),
        F.col("name").alias("player_name")
    )
    
    return df


def load_salaries(spark: SparkSession, path: str) -> DataFrame:
    """
    Load salaries dataset with player salaries by season.
    
    Actual structure:
    - player_id: player ID (e.g., "abdelal01")
    - salary: salary amount
    - season_end: ending year of season (e.g., 1991 for season 1990-91)
    - team: team name
    """
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    print(f"\n=== Salaries CSV columns: {df.columns}")
    
    # Exact mapping based on actual data structure:
    # player_id -> player_id (will join with players._id to get name)
    # salary -> Salary
    # season_end -> season_year (READY TO USE!)
    # team -> Team
    
    df = df.select(
        F.col("player_id").alias("player_id"),
        F.col("salary").cast(DoubleType()).alias("Salary"),
        F.col("season_end").cast(IntegerType()).alias("season_year"),
        F.col("team").alias("Team")
    )
    
    return df


def load_seasons_stats(spark: SparkSession, path: str) -> DataFrame:
    """
    Load seasons statistics dataset with player performance metrics.
    
    Actual structure:
    - Year: season year
    - Player: FULL PLAYER NAME
    - Tm: team abbreviation
    - PTS, TRB, AST: statistics
    """
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    print(f"\n=== Seasons Stats CSV columns (first 10): {df.columns[:10]}")
    print(f"=== Total columns: {len(df.columns)}")
    
    # Select and rename columns
    # Year -> season_year
    # Player -> player_name (full name to match with players.name)
    # PTS, TRB, AST -> keep as is
    
    df = df.select(
        F.col("Year").cast(IntegerType()).alias("season_year"),
        F.col("Player").alias("player_name"),
        F.col("PTS").cast(DoubleType()).alias("PTS"),
        F.col("TRB").cast(DoubleType()).alias("TRB"),
        F.col("AST").cast(DoubleType()).alias("AST")
    )
    
    return df


def load_all_datasets(
    spark: SparkSession,
    players_path: str,
    salaries_path: str,
    seasons_stats_path: str
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Load all three datasets and return them as a tuple."""
    players_df = load_players(spark, players_path)
    salaries_df = load_salaries(spark, salaries_path)
    seasons_stats_df = load_seasons_stats(spark, seasons_stats_path)
    
    return players_df, salaries_df, seasons_stats_df


def load_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Load processed data from Parquet format."""
    return spark.read.parquet(path)

