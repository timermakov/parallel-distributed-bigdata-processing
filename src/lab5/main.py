import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.config import create_spark_session, FULL_TABLE_NAME
import data_loader
import iceberg_ops


def load_all_years(
    spark,
    excel_path: str,
    show_progress: bool = True
) -> None:
    print("\n" + "=" * 80)
    print("LOADING NBA PLAYER SALARIES INTO APACHE ICEBERG")
    print("=" * 80)
    
    # Load Excel data
    print(f"\nLoading data from: {excel_path}")
    pdf = data_loader.load_salaries_from_excel(excel_path)
    years = data_loader.get_available_years(pdf)
    print(f"Found {len(pdf)} records across {len(years)} seasons")
    print(f"Years: {years[0]} - {years[-1]}")
    
    print("\nInitializing Iceberg catalog...")
    iceberg_ops.create_database_if_not_exists(spark)
    iceberg_ops.create_table_if_not_exists(spark)
    print(f"Table created: {FULL_TABLE_NAME}")

    print("\n" + "-" * 80)
    print("Loading data year by year (MERGE INTO)...")
    print("-" * 80)
    
    total_processed = 0
    for year in years:
        year_pdf = data_loader.get_data_for_year(pdf, year)
        year_df = data_loader.pandas_to_spark(spark, year_pdf)
        rows = iceberg_ops.merge_year_data(spark, year_df, year)
        total_processed += rows
        if show_progress:
            print(f"  Year {year}: {rows} records merged")
    
    print(f"\nTotal records processed: {total_processed}")
    final_count = iceberg_ops.get_total_record_count(spark)
    print(f"Records in table: {final_count}")


def show_year_data(spark, year: int) -> None:
    """Display data for specific year."""
    print(f"\n" + "=" * 80)
    print(f"DATA FOR YEAR {year}")
    print("=" * 80)
    
    df = iceberg_ops.get_data_for_year(spark, year)
    count = df.count()
    print(f"\nTotal players in {year}: {count}")
    print("\nAll players (sorted by salary DESC):")
    df.show(count, truncate=False)


def show_top_players(spark, year: int, limit: int = 10) -> None:
    print(f"\n" + "=" * 80)
    print(f"TOP {limit} HIGHEST PAID PLAYERS IN {year}")
    print("=" * 80)
    
    df = iceberg_ops.get_top_paid_players(spark, year, limit)
    df.show(truncate=False)


def show_table_metadata(spark) -> None:
    print("\n" + "=" * 80)
    print("ICEBERG TABLE METADATA")
    print("=" * 80)
    
    print("\n--- Table History ---")
    try:
        history = iceberg_ops.get_table_history(spark)
        history.show(truncate=False)
    except Exception as e:
        print(f"Could not retrieve history: {e}")
    
    print("\n--- Table Snapshots ---")
    try:
        snapshots = iceberg_ops.get_table_snapshots(spark)
        snapshots.show(truncate=False)
    except Exception as e:
        print(f"Could not retrieve snapshots: {e}")


def main() -> None:
    excel_path = "/app/data/lab5/Player - Salaries per Year (1990 - 2017).xlsx"
    warehouse_path = "/app/warehouse"
    
    print("Creating Spark session with Iceberg support...")
    spark = create_spark_session(warehouse_path)
    
    try:
        load_all_years(spark, excel_path)
        
        show_year_data(spark, 2016)
        
        show_top_players(spark, 2016, 10)
        
        show_table_metadata(spark)
        
        print("\n" + "=" * 80)
        print("ALL TASKS COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\nCheck the warehouse directory for Iceberg metadata:")
        print(f"  {warehouse_path}/nba/player_salaries/metadata/")
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

