"""
Data loader module for E-Commerce dataset from Kaggle.
Downloads dataset using kagglehub, applies schema casting, and saves as Parquet.
"""

import os
from pathlib import Path
from typing import Optional

import kagglehub
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


def get_ecommerce_schema() -> StructType:
    """
    Define the schema for E-Commerce dataset.
    
    Dataset columns from https://www.kaggle.com/datasets/carrie1/ecommerce-data/data:
    - InvoiceNo: Invoice number
    - StockCode: Product (item) code  
    - Description: Product (item) name
    - Quantity: The quantities of each product (item) per transaction
    - InvoiceDate: Invoice Date and time
    - UnitPrice: Unit price (Product price per unit)
    - CustomerID: Customer number
    - Country: Country name
    """
    return StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),  # Will be converted to TimestampType
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),  # Can be null, keep as string for now
        StructField("Country", StringType(), True),
    ])


def download_dataset() -> str:
    """
    Download E-Commerce dataset from Kaggle using kagglehub.
    
    Returns:
        Path to downloaded dataset directory
    """
    print("Downloading E-Commerce dataset from Kaggle...")
    path = kagglehub.dataset_download("carrie1/ecommerce-data")
    print(f"Dataset downloaded to: {path}")
    return path


def load_and_process_dataset(
    spark: SparkSession,
    dataset_path: Optional[str] = None,
    output_path: str = "data/ecommerce.parquet"
) -> DataFrame:
    """
    Load CSV dataset, apply schema casting, and save as Parquet.
    
    Args:
        spark: Active SparkSession
        dataset_path: Path to dataset directory (if None, will download from Kaggle)
        output_path: Path where to save Parquet file
        
    Returns:
        Processed DataFrame
    """
    # Download dataset if path not provided
    if dataset_path is None:
        dataset_path = download_dataset()
    
    # Find CSV file in downloaded directory
    csv_files = list(Path(dataset_path).glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {dataset_path}")
    
    csv_path = str(csv_files[0])
    print(f"Loading dataset from: {csv_path}")
    
    schema = get_ecommerce_schema()
    df = spark.read.csv(
        csv_path,
        header=True,
        schema=schema,
        encoding="ISO-8859-1"
    )
    
    from pyspark.sql.functions import to_timestamp
    df = df.withColumn("InvoiceDate", to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
    
    # TotalPrice = Quantity * UnitPrice
    from pyspark.sql.functions import col
    df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))
    
    # Clean data: filter out cancelled orders (InvoiceNo starting with 'C') and nulls
    df_clean = df.filter(
        ~col("InvoiceNo").startswith("C") &
        col("Quantity").isNotNull() &
        (col("Quantity") > 0) &
        col("UnitPrice").isNotNull() &
        (col("UnitPrice") > 0) &
        col("CustomerID").isNotNull()
    )
    
    print(f"Saving processed dataset to: {output_path}")
    df_clean.write.mode("overwrite").parquet(output_path)
    print(f"Dataset saved successfully. Total records: {df_clean.count()}")
    
    return df_clean


def load_parquet(spark: SparkSession, path: str = "data/ecommerce.parquet") -> DataFrame:
    """
    Load processed dataset from Parquet file.
    
    Args:
        spark: Active SparkSession
        path: Path to Parquet file
        
    Returns:
        DataFrame loaded from Parquet
    """
    return spark.read.parquet(path)
