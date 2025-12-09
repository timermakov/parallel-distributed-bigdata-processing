"""Analytics module for finding top cost-effective NBA players."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def get_top_players_per_season(df: DataFrame, top_n: int = 5) -> DataFrame:
    # window partitioned by season_year, ordered by cost_per_efficiency
    window_spec = Window.partitionBy("season_year").orderBy(F.col("cost_per_efficiency").asc())
    
    df_ranked = df.withColumn("rank", F.row_number().over(window_spec))

    top_players = df_ranked.filter(F.col("rank") <= top_n)
    
    return top_players


def display_top_players(df: DataFrame) -> None:
    df_sorted = df.orderBy("season_year", "rank")
    
    print("\n" + "="*100)
    print("TOP 5 MOST COST-EFFECTIVE NBA PLAYERS PER SEASON")
    print("="*100)
    
    # Get unique seasons
    seasons = df_sorted.select("season_year").distinct().orderBy("season_year").collect()
    
    for season_row in seasons:
        season = season_row["season_year"]
        print(f"\n{'─'*100}")
        print(f"Season: {season}")
        print(f"{'─'*100}")
        
        # Get players for this season
        season_players = df_sorted.filter(F.col("season_year") == season)
        
        # Format display
        print(f"{'Rank':<6} {'Player':<25} {'Team':<8} {'Salary':>12} {'PTS':>6} {'TRB':>6} {'AST':>6} {'Efficiency':>10} {'Cost/Eff':>12}")
        print(f"{'-'*6} {'-'*25} {'-'*8} {'-'*12} {'-'*6} {'-'*6} {'-'*6} {'-'*10} {'-'*12}")
        
        for row in season_players.collect():
            rank = row["rank"]
            player = row["Player"][:25]  # Truncate long names
            team = str(row["Team"])[:8] if row["Team"] else "N/A"
            salary = f"${row['Salary']:,.0f}"
            pts = f"{row['PTS']:.1f}"
            trb = f"{row['TRB']:.1f}"
            ast = f"{row['AST']:.1f}"
            efficiency = f"{row['efficiency']:.1f}"
            cost_per_eff = f"${row['cost_per_efficiency']:,.0f}"
            
            print(f"{rank:<6} {player:<25} {team:<8} {salary:>12} {pts:>6} {trb:>6} {ast:>6} {efficiency:>10} {cost_per_eff:>12}")
    
    print(f"\n{'='*100}\n")


def save_top_players(df: DataFrame, output_path: str) -> None:
    df.write.mode("overwrite").parquet(output_path)
    print(f"Top players analysis saved to {output_path}")


def run_analytics(data_path: str, top_n: int = 5, output_path: str = None) -> None:
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Loading processed data from: {data_path}")
    df = spark.read.parquet(data_path)
    
    print(f"Finding top {top_n} most cost-effective players per season...")
    top_players = get_top_players_per_season(df, top_n)
    
    # Display results
    display_top_players(top_players)
    
    # Save if output path provided
    if output_path:
        save_top_players(top_players, output_path)
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"  Total seasons analyzed: {df.select('season_year').distinct().count()}")
    print(f"  Total player-season records: {df.count()}")
    print(f"  Top players selected: {top_players.count()}")

