"""Apache Iceberg table operations for NBA player salaries."""

from pyspark.sql import SparkSession, DataFrame

from config.config import CATALOG_NAME, DATABASE_NAME, TABLE_NAME, FULL_TABLE_NAME


def create_database_if_not_exists(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}")


def create_table_if_not_exists(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
            player_name STRING,
            salary BIGINT,
            season_start INT,
            season_end INT,
            team STRING,
            team_full_name STRING
        )
        USING iceberg
        PARTITIONED BY (season_end)
    """)


def merge_year_data(spark: SparkSession, df: DataFrame, year: int) -> int:
    temp_view = f"year_data_{year}"
    df.createOrReplaceTempView(temp_view)
    
    # MERGE INTO: update if player+season_end exists, otherwise insert
    spark.sql(f"""
        MERGE INTO {FULL_TABLE_NAME} AS target
        USING {temp_view} AS source
        ON target.player_name = source.player_name 
           AND target.season_end = source.season_end
           AND target.team = source.team
        WHEN MATCHED THEN
            UPDATE SET
                salary = source.salary,
                season_start = source.season_start,
                team_full_name = source.team_full_name
        WHEN NOT MATCHED THEN
            INSERT (player_name, salary, season_start, season_end, team, team_full_name)
            VALUES (source.player_name, source.salary, source.season_start, 
                    source.season_end, source.team, source.team_full_name)
    """)
    
    return df.count()


def get_table_snapshot_at_time(spark: SparkSession, timestamp: str) -> DataFrame:
    return spark.sql(f"""
        SELECT * FROM {FULL_TABLE_NAME} TIMESTAMP AS OF '{timestamp}'
    """)


def get_data_for_year(spark: SparkSession, year: int) -> DataFrame:
    return spark.sql(f"""
        SELECT * FROM {FULL_TABLE_NAME}
        WHERE season_end = {year}
        ORDER BY salary DESC
    """)


def get_top_paid_players(spark: SparkSession, year: int, limit: int = 10) -> DataFrame:
    return spark.sql(f"""
        SELECT player_name, salary, team, team_full_name, season_end
        FROM {FULL_TABLE_NAME}
        WHERE season_end = {year}
        ORDER BY salary DESC
        LIMIT {limit}
    """)


def get_table_history(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}.history")


def get_table_snapshots(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}.snapshots")


def get_total_record_count(spark: SparkSession) -> int:
    return spark.sql(f"SELECT COUNT(*) as cnt FROM {FULL_TABLE_NAME}").collect()[0]["cnt"]

