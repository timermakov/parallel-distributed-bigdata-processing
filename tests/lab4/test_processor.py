"""Tests for NBA data processing functionality."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Import sys and add src to path for imports
import sys
from pathlib import Path
src_path = Path(__file__).parent.parent.parent / "src" / "lab4"
sys.path.insert(0, str(src_path))

from processor import (
    calculate_cost_per_efficiency,
    calculate_efficiency,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test_nba_processor") \
        .getOrCreate()
    yield spark
    spark.stop()



def test_calculate_efficiency(spark):
    """Test efficiency calculation (PTS + TRB + AST)."""
    schema = StructType([
        StructField("Player", StringType(), True),
        StructField("PTS", DoubleType(), True),
        StructField("TRB", DoubleType(), True),
        StructField("AST", DoubleType(), True),
    ])
    
    data = [
        ("Player A", 20.5, 10.2, 5.3),
        ("Player B", 15.0, 8.0, 7.0),
        ("Player C", 25.0, 12.0, 3.0),
    ]
    
    df = spark.createDataFrame(data, schema)
    result_df = calculate_efficiency(df)
    
    results = result_df.select("Player", "efficiency").collect()
    
    assert results[0]["efficiency"] == pytest.approx(36.0, rel=0.01)
    assert results[1]["efficiency"] == pytest.approx(30.0, rel=0.01)
    assert results[2]["efficiency"] == pytest.approx(40.0, rel=0.01)


def test_calculate_cost_per_efficiency(spark):
    """Test cost per efficiency calculation."""
    schema = StructType([
        StructField("Player", StringType(), True),
        StructField("Salary", DoubleType(), True),
        StructField("efficiency", DoubleType(), True),
    ])
    
    data = [
        ("Player A", 1000000.0, 40.0),
        ("Player B", 2000000.0, 50.0),
        ("Player C", 500000.0, 25.0),
    ]
    
    df = spark.createDataFrame(data, schema)
    result_df = calculate_cost_per_efficiency(df)
    
    results = result_df.select("Player", "cost_per_efficiency").collect()
    
    assert results[0]["cost_per_efficiency"] == pytest.approx(25000.0, rel=0.01)
    assert results[1]["cost_per_efficiency"] == pytest.approx(40000.0, rel=0.01)
    assert results[2]["cost_per_efficiency"] == pytest.approx(20000.0, rel=0.01)


def test_efficiency_with_zeros(spark):
    """Test efficiency calculation with zero values."""
    schema = StructType([
        StructField("Player", StringType(), True),
        StructField("PTS", DoubleType(), True),
        StructField("TRB", DoubleType(), True),
        StructField("AST", DoubleType(), True),
    ])
    
    data = [
        ("Player A", 0.0, 0.0, 0.0),
        ("Player B", 10.0, 0.0, 0.0),
    ]
    
    df = spark.createDataFrame(data, schema)
    result_df = calculate_efficiency(df)
    
    results = result_df.select("Player", "efficiency").collect()
    
    assert results[0]["efficiency"] == 0.0
    assert results[1]["efficiency"] == 10.0

