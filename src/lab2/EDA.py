"""
Exploratory Data Analysis (EDA) module for E-Commerce dataset.
Provides functions to analyze and understand the dataset.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max, countDistinct


def print_dataset_overview(df: DataFrame) -> None:
    """Print basic dataset statistics and structure."""
    print("\n" + "="*80)
    print("DATASET OVERVIEW")
    print("="*80)
    
    print(f"\nTotal records: {df.count():,}")
    print(f"\nSchema:")
    df.printSchema()
    
    print("\nFirst 10 rows:")
    df.show(10, truncate=False)


def analyze_basic_statistics(df: DataFrame) -> None:
    """Analyze basic statistical measures of the dataset."""
    print("\n" + "="*80)
    print("BASIC STATISTICS")
    print("="*80)
    
    # Unique counts
    print("\nUnique value counts:")
    unique_stats = df.select([
        countDistinct("InvoiceNo").alias("Unique Invoices"),
        countDistinct("StockCode").alias("Unique Products"),
        countDistinct("CustomerID").alias("Unique Customers"),
        countDistinct("Country").alias("Unique Countries")
    ])
    unique_stats.show()
    
    print("\nNumerical column statistics:")
    df.select(["Quantity", "UnitPrice", "TotalPrice"]).describe().show()
    
    print("\nMissing values per column:")
    missing_counts = df.select([
        (count("*") - count(c)).alias(c) for c in df.columns
    ])
    missing_counts.show()


def analyze_top_countries(df: DataFrame, top_n: int = 10) -> None:
    """Analyze sales by country."""
    print("\n" + "="*80)
    print(f"TOP {top_n} COUNTRIES BY SALES")
    print("="*80)
    
    country_stats = df.groupBy("Country").agg(
        count("InvoiceNo").alias("Total_Orders"),
        countDistinct("CustomerID").alias("Unique_Customers"),
        spark_sum("TotalPrice").alias("Total_Revenue")
    ).orderBy(col("Total_Revenue").desc())
    
    print(f"\nTop {top_n} countries:")
    country_stats.show(top_n)


def analyze_product_categories(df: DataFrame, top_n: int = 20) -> None:
    """Analyze top products by quantity and revenue."""
    print("\n" + "="*80)
    print(f"TOP {top_n} PRODUCTS ANALYSIS")
    print("="*80)
    
    product_stats = df.groupBy("StockCode", "Description").agg(
        spark_sum("Quantity").alias("Total_Quantity"),
        spark_sum("TotalPrice").alias("Total_Revenue"),
        count("InvoiceNo").alias("Order_Count")
    ).orderBy(col("Total_Quantity").desc())
    
    print(f"\nTop {top_n} products by quantity sold:")
    product_stats.show(top_n, truncate=False)


def analyze_customer_behavior(df: DataFrame, top_n: int = 10) -> None:
    """Analyze customer purchasing patterns."""
    print("\n" + "="*80)
    print(f"TOP {top_n} CUSTOMERS ANALYSIS")
    print("="*80)
    
    customer_stats = df.groupBy("CustomerID").agg(
        countDistinct("InvoiceNo").alias("Total_Orders"),
        spark_sum("TotalPrice").alias("Total_Spent"),
        avg("TotalPrice").alias("Avg_Order_Value"),
        spark_sum("Quantity").alias("Total_Items")
    ).orderBy(col("Total_Spent").desc())
    
    print(f"\nTop {top_n} customers by spending:")
    customer_stats.show(top_n)
    
    print("\nOverall customer statistics:")
    customer_stats.select([
        avg("Total_Orders").alias("Avg_Orders_Per_Customer"),
        avg("Total_Spent").alias("Avg_Spent_Per_Customer"),
        avg("Avg_Order_Value").alias("Overall_Avg_Order_Value")
    ]).show()


def analyze_time_patterns(df: DataFrame) -> None:
    """Analyze temporal patterns in the data."""
    print("\n" + "="*80)
    print("TIME PATTERNS ANALYSIS")
    print("="*80)
    
    from pyspark.sql.functions import year, month, dayofweek, hour
    
    df_time = df.withColumn("Year", year("InvoiceDate")) \
                .withColumn("Month", month("InvoiceDate")) \
                .withColumn("DayOfWeek", dayofweek("InvoiceDate")) \
                .withColumn("Hour", hour("InvoiceDate"))
    
    print("\nSales by Month:")
    monthly_sales = df_time.groupBy("Year", "Month").agg(
        spark_sum("TotalPrice").alias("Revenue"),
        count("InvoiceNo").alias("Orders")
    ).orderBy("Year", "Month")
    monthly_sales.show()
    
    print("\nSales by Day of Week (1=Sunday, 7=Saturday):")
    dow_sales = df_time.groupBy("DayOfWeek").agg(
        spark_sum("TotalPrice").alias("Revenue"),
        count("InvoiceNo").alias("Orders")
    ).orderBy("DayOfWeek")
    dow_sales.show()
    
    print("\nSales by Hour of Day:")
    hour_sales = df_time.groupBy("Hour").agg(
        spark_sum("TotalPrice").alias("Revenue"),
        count("InvoiceNo").alias("Orders")
    ).orderBy("Hour")
    hour_sales.show(24)


def run_full_eda(df: DataFrame) -> None:
    """Run complete exploratory data analysis."""
    print_dataset_overview(df)
    analyze_basic_statistics(df)
    analyze_top_countries(df)
    analyze_product_categories(df)
    analyze_customer_behavior(df)
    analyze_time_patterns(df)
    print("\n" + "="*80)
    print("EDA COMPLETE")
    print("="*80 + "\n")


def main() -> None:
    """Main entry point for EDA module."""
    from config.config import create_spark_session
    from data_loader import load_parquet
    
    spark = create_spark_session("local[*]")
    
    try:
        # Load dataset
        print("Loading dataset...")
        df = load_parquet(spark)
        
        # Run EDA
        run_full_eda(df)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
