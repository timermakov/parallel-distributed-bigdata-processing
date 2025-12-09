"""Data processing logic for NBA player efficiency analysis."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_efficiency(df: DataFrame) -> DataFrame:
    """Calculate player efficiency as PTS + TRB + AST."""
    df = df.withColumn(
        "efficiency",
        F.col("PTS") + F.col("TRB") + F.col("AST")
    )
    return df


def calculate_cost_per_efficiency(df: DataFrame) -> DataFrame:
    """Calculate cost per efficiency unit as Salary / Efficiency."""
    df = df.withColumn(
        "cost_per_efficiency",
        F.col("Salary") / F.col("efficiency")
    )
    return df


def process_player_efficiency(
    players_df: DataFrame,
    salaries_df: DataFrame,
    seasons_stats_df: DataFrame,
    output_path: str
) -> None:
    """
    Process NBA data to calculate cost per efficiency and save to partitioned Parquet.
    
    Strategy:
    1. salaries has player_id + season_year + Salary + Team
    2. players has player_id + player_name (bridge table)
    3. seasons_stats has player_name + season_year + PTS/TRB/AST
    
    Join flow:
    1. Join salaries with players on player_id to get player_name
    2. Join result with seasons_stats on player_name + season_year
    3. Calculate efficiency and cost per efficiency
    """
    
    # Debug: Show sample data
    print("\n=== Debug: Players Data ===")
    print(f"Players count: {players_df.count()}")
    players_df.show(5, truncate=False)
    
    print("\n=== Debug: Salaries Data ===")
    print(f"Salaries count: {salaries_df.count()}")
    salaries_df.show(5, truncate=False)
    
    print("\n=== Debug: Season Stats Data ===")
    print(f"Season stats count: {seasons_stats_df.count()}")
    seasons_stats_df.show(5, truncate=False)
    
    # Step 1: Join salaries with players to get player names
    # salaries.player_id = players.player_id
    salaries_with_names = salaries_df.join(
        players_df,
        on="player_id",
        how="inner"
    )
    
    print("\n=== Debug: After joining salaries with players ===")
    print(f"Count: {salaries_with_names.count()}")
    salaries_with_names.select("player_id", "player_name", "season_year", "Salary", "Team").show(5, truncate=False)
    
    # Step 2: Join with seasons_stats on player_name + season_year
    # salaries_with_names.player_name = seasons_stats.player_name
    # salaries_with_names.season_year = seasons_stats.season_year
    joined_df = salaries_with_names.join(
        seasons_stats_df,
        on=["player_name", "season_year"],
        how="inner"
    )
    
    print(f"\n=== Debug: After joining with season stats ===")
    print(f"Joined count: {joined_df.count()}")
    if joined_df.count() > 0:
        joined_df.select("player_name", "season_year", "Salary", "PTS", "TRB", "AST").show(10, truncate=False)
    else:
        print("❌ WARNING: No records after join! Check player name matching.")
    
    # Calculate efficiency
    joined_df = calculate_efficiency(joined_df)
    
    # Filter out records with null values in key columns
    joined_df = joined_df.filter(
        F.col("Salary").isNotNull() &
        F.col("PTS").isNotNull() &
        F.col("TRB").isNotNull() &
        F.col("AST").isNotNull() &
        F.col("efficiency").isNotNull()
    )
    
    # Filter out records where efficiency is 0 or negative (avoid division by zero)
    joined_df = joined_df.filter(F.col("efficiency") > 0)
    
    # Calculate cost per efficiency
    joined_df = calculate_cost_per_efficiency(joined_df)
    
    # Filter out null or invalid cost per efficiency values
    joined_df = joined_df.filter(
        F.col("cost_per_efficiency").isNotNull() &
        (F.col("cost_per_efficiency") > 0)
    )
    
    # Select final columns
    final_df = joined_df.select(
        F.col("player_name").alias("Player"),
        "season_year",
        "Team",
        "Salary",
        "PTS",
        "TRB",
        "AST",
        "efficiency",
        "cost_per_efficiency"
    )
    
    # Save to Parquet partitioned by season_year
    final_df.write.partitionBy("season_year").mode("overwrite").parquet(output_path)
    
    print(f"\nProcessed data saved to {output_path}")
    print(f"Records processed: {final_df.count()}")
    print(f"Seasons covered: {final_df.select('season_year').distinct().count()}")

