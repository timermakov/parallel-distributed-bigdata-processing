import sys
import os
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from data_loader import get_ecommerce_schema


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("TestDataLoader") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_get_ecommerce_schema():
    """Test that schema is properly defined."""
    schema = get_ecommerce_schema()
    
    # Check it's a StructType
    assert isinstance(schema, StructType)
    
    # Check all expected fields are present
    field_names = [field.name for field in schema.fields]
    expected_fields = [
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    ]
    
    assert field_names == expected_fields
    
    # Check field count
    assert len(schema.fields) == 8
