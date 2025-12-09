"""
Main entry point for Lab4: NBA Player Efficiency Analytics with PySpark.
Provides CLI interface for data processing and analytics tasks.
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config.config import create_spark_session


def run_process(
    spark: SparkSession,
    players_path: str,
    salaries_path: str,
    seasons_stats_path: str,
    output_path: str
) -> None:
    """Process raw CSV data and save to partitioned Parquet."""
    import data_loader
    import processor
    
    load_all_datasets = data_loader.load_all_datasets
    process_player_efficiency = processor.process_player_efficiency
    
    print("\n" + "="*100)
    print("PROCESSING NBA PLAYER EFFICIENCY DATA")
    print("="*100 + "\n")
    
    print("Loading datasets...")
    print(f"  - Players: {players_path}")
    print(f"  - Salaries: {salaries_path}")
    print(f"  - Season Stats: {seasons_stats_path}")
    
    players_df, salaries_df, seasons_stats_df = load_all_datasets(
        spark,
        players_path,
        salaries_path,
        seasons_stats_path
    )
    
    print(f"\nPlayers loaded: {players_df.count()} records")
    print(f"Salaries loaded: {salaries_df.count()} records")
    print(f"Season stats loaded: {seasons_stats_df.count()} records")
    
    print(f"\nProcessing data and calculating efficiency metrics...")
    process_player_efficiency(players_df, salaries_df, seasons_stats_df, output_path)
    
    print("\nProcessing complete!")


def run_analytics_task(
    spark: SparkSession,
    data_path: str,
    top_n: int,
    output_path: str = None
) -> None:
    """Run analytics to find top cost-effective players."""
    import analytics
    
    run_analytics = analytics.run_analytics
    
    print("\n" + "="*100)
    print("RUNNING NBA PLAYER COST-EFFECTIVENESS ANALYTICS")
    print("="*100 + "\n")
    
    if not Path(data_path).exists():
        print(f"Error: Processed data not found at {data_path}")
        print("Please run with --task process first")
        sys.exit(1)
    
    run_analytics(data_path, top_n, output_path)


def run_all(
    spark: SparkSession,
    players_path: str,
    salaries_path: str,
    seasons_stats_path: str,
    processed_path: str,
    top_n: int,
    output_path: str = None
) -> None:
    """Run complete pipeline: process data and run analytics."""
    # Process data
    run_process(spark, players_path, salaries_path, seasons_stats_path, processed_path)
    
    # Run analytics
    run_analytics_task(spark, processed_path, top_n, output_path)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="NBA Player Efficiency Analytics with PySpark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process raw CSV data into partitioned Parquet
  python -m main --task process \\
    --players data/lab4/players.csv \\
    --salaries data/lab4/salaries_1985to2018.csv \\
    --seasons-stats data/lab4/Seasons_Stats.csv \\
    --processed-path data/lab4/processed.parquet
  
  # Run analytics on processed data
  python -m main --task analytics \\
    --processed-path data/lab4/processed.parquet \\
    --top-n 5
  
  # Run complete pipeline
  python -m main --task all \\
    --players data/lab4/players.csv \\
    --salaries data/lab4/salaries_1985to2018.csv \\
    --seasons-stats data/lab4/Seasons_Stats.csv \\
    --processed-path data/lab4/processed.parquet \\
    --top-n 5
        """
    )
    
    parser.add_argument(
        "--task",
        type=str,
        required=True,
        choices=["process", "analytics", "all"],
        help="Task to run: process (process raw data), analytics (find top players), all (complete pipeline)"
    )
    
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Spark master URL (e.g., local[*] or spark://spark-master:7077). Default: local[*]"
    )
    
    parser.add_argument(
        "--players",
        type=str,
        default="data/lab4/players.csv",
        help="Path to players CSV file (default: data/lab4/players.csv)"
    )
    
    parser.add_argument(
        "--salaries",
        type=str,
        default="data/lab4/salaries_1985to2018.csv",
        help="Path to salaries CSV file (default: data/lab4/salaries_1985to2018.csv)"
    )
    
    parser.add_argument(
        "--seasons-stats",
        type=str,
        default="data/lab4/Seasons_Stats.csv",
        help="Path to seasons stats CSV file (default: data/lab4/Seasons_Stats.csv)"
    )
    
    parser.add_argument(
        "--processed-path",
        type=str,
        default="data/lab4/processed.parquet",
        help="Path to processed Parquet data (default: data/lab4/processed.parquet)"
    )
    
    parser.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Number of top players to show per season (default: 5)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional path to save analytics results"
    )
    
    args = parser.parse_args()
    
    # Validate required paths for process task
    if args.task in ["process", "all"]:
        for path_arg, path_value in [
            ("players", args.players),
            ("salaries", args.salaries),
            ("seasons-stats", args.seasons_stats),
        ]:
            if not Path(path_value).exists():
                print(f"Error: {path_arg} file not found at {path_value}")
                sys.exit(1)
    
    # Create Spark session
    master = args.master if args.master else "local[*]"
    spark = create_spark_session(master)
    
    try:
        if args.task == "process":
            run_process(
                spark,
                args.players,
                args.salaries,
                args.seasons_stats,
                args.processed_path
            )
        
        elif args.task == "analytics":
            run_analytics_task(
                spark,
                args.processed_path,
                args.top_n,
                args.output
            )
        
        elif args.task == "all":
            run_all(
                spark,
                args.players,
                args.salaries,
                args.seasons_stats,
                args.processed_path,
                args.top_n,
                args.output
            )
        
        print("\nAll tasks completed successfully!")
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

