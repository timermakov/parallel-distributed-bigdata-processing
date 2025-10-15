"""
Main entry point for Lab2: E-Commerce Data Analytics with PySpark.
Provides CLI interface for data loading, EDA, and analytics tasks.
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from config.config import create_spark_session


def run_download_and_process(spark: SparkSession, output_path: str) -> None:
    """Download dataset from Kaggle and process to Parquet."""
    from data_loader import load_and_process_dataset
    
    print("\n" + "="*80)
    print("DOWNLOADING AND PROCESSING DATASET")
    print("="*80 + "\n")
    
    load_and_process_dataset(spark, output_path=output_path)
    print("\nDataset processing complete!")


def run_eda(spark: SparkSession, data_path: str) -> None:
    """Run exploratory data analysis."""
    from data_loader import load_parquet
    from EDA import run_full_eda
    
    print("\n" + "="*80)
    print("RUNNING EXPLORATORY DATA ANALYSIS")
    print("="*80 + "\n")
    
    print(f"Loading data from: {data_path}")
    df = load_parquet(spark, data_path)
    run_full_eda(df)


def run_analytics_task(
    spark: SparkSession,
    data_path: str,
    top_n: int,
    output_dir: str
) -> None:
    """Run analytics tasks."""
    from data_loader import load_parquet
    from analytics import run_analytics
    
    print("\n" + "="*80)
    print("RUNNING ANALYTICS TASKS")
    print("="*80 + "\n")
    
    print(f"Loading data from: {data_path}")
    df = load_parquet(spark, data_path)
    run_analytics(df, top_n=top_n, output_dir=output_dir)


def run_all(
    spark: SparkSession,
    data_path: str,
    top_n: int,
    output_dir: str
) -> None:
    """Run complete pipeline: download, EDA, and analytics."""
    # Download and process
    run_download_and_process(spark, data_path)
    
    # Run EDA
    run_eda(spark, data_path)
    
    # Run analytics
    run_analytics_task(spark, data_path, top_n, output_dir)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="E-Commerce Data Analytics with PySpark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download and process dataset
  python -m main --task download --data-path data/ecommerce.parquet
  
  # Run exploratory data analysis
  python -m main --task eda --data-path data/ecommerce.parquet
  
  # Run analytics (top products and customer stats)
  python -m main --task analytics --data-path data/ecommerce.parquet
  
  # Run complete pipeline
  python -m main --task all --data-path data/ecommerce.parquet
        """
    )
    
    parser.add_argument(
        "--task",
        type=str,
        required=True,
        choices=["download", "eda", "analytics", "all"],
        help="Task to run: download (download & process data), eda (exploratory analysis), analytics (business analytics), all (complete pipeline)"
    )
    
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Spark master URL (e.g., local[*] or spark://spark-master:7077). Default: local[*]"
    )
    
    parser.add_argument(
        "--data-path",
        type=str,
        default="data/ecommerce.parquet",
        help="Path to Parquet data file (default: data/ecommerce.parquet)"
    )
    
    parser.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Number of top products to analyze (default: 5)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results",
        help="Directory to save analytics results (default: results)"
    )
    
    args = parser.parse_args()
    
    # Create Spark session
    master = args.master if args.master else "local[*]"
    spark = create_spark_session(master)
    
    try:
        if args.task == "download":
            run_download_and_process(spark, args.data_path)
        
        elif args.task == "eda":
            if not Path(args.data_path).exists():
                print(f"Error: Data file not found at {args.data_path}")
                print("Please run with --task download first")
                sys.exit(1)
            run_eda(spark, args.data_path)
        
        elif args.task == "analytics":
            if not Path(args.data_path).exists():
                print(f"Error: Data file not found at {args.data_path}")
                print("Please run with --task download first")
                sys.exit(1)
            run_analytics_task(spark, args.data_path, args.top_n, args.output_dir)
        
        elif args.task == "all":
            run_all(spark, args.data_path, args.top_n, args.output_dir)
        
        print("\n✓ Task completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
