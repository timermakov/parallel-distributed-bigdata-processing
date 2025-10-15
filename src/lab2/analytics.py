"""
Analytics module for E-Commerce dataset.
Implements core business analytics tasks.
"""

from pathlib import Path
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as spark_sum, countDistinct, avg
from pyspark.sql.window import Window


def get_top_products_by_quantity(df: DataFrame, top_n: int = 5) -> DataFrame:
    """
    Get top N products by total quantity sold.
    
    Args:
        df: Input DataFrame with e-commerce data
        top_n: Number of top products to return (default: 5)
        
    Returns:
        DataFrame with top products containing:
        - StockCode
        - Description
        - TotalQuantitySold
    """
    top_products = df.groupBy("StockCode", "Description") \
        .agg(spark_sum("Quantity").alias("TotalQuantitySold")) \
        .orderBy(col("TotalQuantitySold").desc()) \
        .limit(top_n)
    
    return top_products


def get_customer_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate statistics for each customer:
    - Total number of orders
    - Total amount spent
    - Average order value
    
    Args:
        df: Input DataFrame with e-commerce data
        
    Returns:
        DataFrame with customer statistics containing:
        - CustomerID
        - TotalOrders
        - TotalSpent
        - AverageOrderValue
    """
    # total price per invoice (order)
    order_totals = df.groupBy("CustomerID", "InvoiceNo") \
        .agg(spark_sum("TotalPrice").alias("OrderTotal"))
    
    # customer-level statistics
    customer_stats = order_totals.groupBy("CustomerID") \
        .agg(
            countDistinct("InvoiceNo").alias("TotalOrders"),
            spark_sum("OrderTotal").alias("TotalSpent"),
            avg("OrderTotal").alias("AverageOrderValue")
        ) \
        .orderBy(col("TotalSpent").desc())
    
    return customer_stats


def save_analytics_results(
    top_products: DataFrame,
    customer_stats: DataFrame,
    output_dir: str = "results"
) -> None:
    """
    Save analytics results to Parquet files.
    
    Args:
        top_products: DataFrame with top products
        customer_stats: DataFrame with customer statistics
        output_dir: Directory to save results (default: 'results')
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # save top products
    top_products_path = f"{output_dir}/top_products.parquet"
    print(f"\nSaving top products to: {top_products_path}")
    top_products.write.mode("overwrite").parquet(top_products_path)
    
    top_products_csv = f"{output_dir}/top_products.csv"
    top_products.coalesce(1).write.mode("overwrite") \
        .option("header", "true").csv(top_products_csv)
    
    # save customer statistics
    customer_stats_path = f"{output_dir}/customer_statistics.parquet"
    print(f"Saving customer statistics to: {customer_stats_path}")
    customer_stats.write.mode("overwrite").parquet(customer_stats_path)
    
    customer_stats_csv = f"{output_dir}/customer_statistics.csv"
    customer_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true").csv(customer_stats_csv)
    
    print(f"\nResults saved successfully to '{output_dir}/' directory")


def print_analytics_summary_debug(
    top_products: DataFrame,
    customer_stats: DataFrame
) -> None:
    """
    Print summary of analytics results.
    Not use in prod for resource saving.
    
    Args:
        top_products: DataFrame with top products
        customer_stats: DataFrame with customer statistics
    """
    print("\n" + "="*80)
    print("ANALYTICS RESULTS")
    print("="*80)
    
    print("\nTOP 5 PRODUCTS BY QUANTITY SOLD:")
    print("-" * 80)
    top_products.show(truncate=False)
    
    print("\nCUSTOMER STATISTICS (Top 20 by spending):")
    print("-" * 80)
    customer_stats.show(20)
    
    print("\nOVERALL CUSTOMER METRICS:")
    print("-" * 80)
    overall_stats = customer_stats.select([
        avg("TotalOrders").alias("Avg_Orders_Per_Customer"),
        avg("TotalSpent").alias("Avg_Total_Spent"),
        avg("AverageOrderValue").alias("Avg_Order_Value")
    ])
    overall_stats.show()
    
    print("="*80 + "\n")


def run_analytics(
    df: DataFrame,
    top_n: int = 5,
    output_dir: str = "results",
    save_results: bool = True
) -> 'Tuple[DataFrame, DataFrame]':
    """
    Run complete analytics pipeline.
    
    Args:
        df: Input DataFrame with e-commerce data
        top_n: Number of top products to analyze (default: 5)
        output_dir: Directory to save results (default: 'results')
        save_results: Whether to save results to disk (default: True)
        
    Returns:
        Tuple of (top_products DataFrame, customer_stats DataFrame)
    """
    print("\nRunning analytics...")
    
    print(f"\nCalculating top {top_n} products by quantity sold...")
    top_products = get_top_products_by_quantity(df, top_n)
    
    print("Calculating customer statistics...")
    customer_stats = get_customer_statistics(df)
    
    #debug only
    #print_analytics_summary_debug(top_products, customer_stats)
    
    if save_results:
        save_analytics_results(top_products, customer_stats, output_dir)
    
    return top_products, customer_stats


def main() -> None:
    """Main entry point for analytics module."""
    from config.config import create_spark_session
    from data_loader import load_parquet
    
    spark = create_spark_session("local[*]")
    
    try:
        print("Loading dataset...")
        df = load_parquet(spark)
        
        run_analytics(df)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
