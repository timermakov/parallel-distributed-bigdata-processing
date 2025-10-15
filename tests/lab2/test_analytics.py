import sys
import os
import pytest
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from analytics import get_top_products_by_quantity, get_customer_statistics


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("TestAnalytics") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark):
    """Create sample e-commerce data for testing."""
    schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("TotalPrice", DoubleType(), True),
    ])
    
    data = [
        ("INV001", "PROD1", "Product 1", 10, 5.0, "CUST1", 50.0),
        ("INV001", "PROD2", "Product 2", 5, 10.0, "CUST1", 50.0),
        ("INV002", "PROD1", "Product 1", 20, 5.0, "CUST2", 100.0),
        ("INV003", "PROD3", "Product 3", 15, 8.0, "CUST1", 120.0),
        ("INV004", "PROD2", "Product 2", 8, 10.0, "CUST3", 80.0),
        ("INV005", "PROD1", "Product 1", 25, 5.0, "CUST2", 125.0),
    ]
    
    return spark.createDataFrame(data, schema)


def test_get_top_products_by_quantity(sample_data):
    """Test top products calculation."""
    result = get_top_products_by_quantity(sample_data, top_n=3)
    result_list = result.collect()
    
    # Should return 3 products
    assert len(result_list) == 3
    
    # First product should be PROD1 with quantity 55 (10+20+25)
    assert result_list[0]["StockCode"] == "PROD1"
    assert result_list[0]["TotalQuantitySold"] == 55
    
    # Second should be PROD3 with quantity 15
    assert result_list[1]["StockCode"] == "PROD3"
    assert result_list[1]["TotalQuantitySold"] == 15


def test_get_customer_statistics(sample_data):
    """Test customer statistics calculation."""
    result = get_customer_statistics(sample_data)
    result_list = result.collect()
    
    # Should have 3 customers
    assert len(result_list) == 3
    
    # Find CUST1 stats
    cust1 = [r for r in result_list if r["CustomerID"] == "CUST1"][0]
    assert cust1["TotalOrders"] == 2  # INV001 and INV003
    assert cust1["TotalSpent"] == 220.0  # 100 + 120
    assert cust1["AverageOrderValue"] == 110.0  # 220 / 2


def test_top_products_ordering(sample_data):
    """Test that products are ordered by quantity descending."""
    result = get_top_products_by_quantity(sample_data, top_n=5)
    quantities = [row["TotalQuantitySold"] for row in result.collect()]
    
    # Check descending order
    assert quantities == sorted(quantities, reverse=True)


def test_customer_stats_ordering(sample_data):
    """Test that customers are ordered by total spent descending."""
    result = get_customer_statistics(sample_data)
    totals = [row["TotalSpent"] for row in result.collect()]
    
    # Check descending order
    assert totals == sorted(totals, reverse=True)
