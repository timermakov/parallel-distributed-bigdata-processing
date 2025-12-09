"""Tests for NBA analytics functionality."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

import sys
from pathlib import Path
src_path = Path(__file__).parent.parent.parent / "src" / "lab4"
sys.path.insert(0, str(src_path))

from analytics import get_top_players_per_season


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test_nba_analytics") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_get_top_players_per_season(spark):
    """Test getting top N most cost-effective players per season."""
    schema = StructType([
        StructField("Player", StringType(), True),
        StructField("season_year", IntegerType(), True),
        StructField("Team", StringType(), True),
        StructField("Salary", DoubleType(), True),
        StructField("efficiency", DoubleType(), True),
        StructField("cost_per_efficiency", DoubleType(), True),
    ])
    
    # Create test data with multiple seasons
    data = [
        # Season 1990 - 5 players
        ("Player A", 1990, "LAL", 1000000.0, 40.0, 25000.0),
        ("Player B", 1990, "CHI", 2000000.0, 50.0, 40000.0),
        ("Player C", 1990, "BOS", 500000.0, 25.0, 20000.0),  # Best
        ("Player D", 1990, "MIA", 800000.0, 30.0, 26667.0),
        ("Player E", 1990, "NYK", 1500000.0, 45.0, 33333.0),
        
        # Season 1991 - 5 players
        ("Player F", 1991, "LAL", 1200000.0, 48.0, 25000.0),
        ("Player G", 1991, "CHI", 900000.0, 30.0, 30000.0),
        ("Player H", 1991, "BOS", 600000.0, 35.0, 17143.0),  # Best
        ("Player I", 1991, "MIA", 1800000.0, 60.0, 30000.0),
        ("Player J", 1991, "NYK", 2500000.0, 70.0, 35714.0),
    ]
    
    df = spark.createDataFrame(data, schema)
    result_df = get_top_players_per_season(df, top_n=3)
    
    # Should have 3 players per season = 6 total
    assert result_df.count() == 6
    
    # Check 1990 season - top 3
    season_1990 = result_df.filter(result_df.season_year == 1990).orderBy("rank").collect()
    assert len(season_1990) == 3
    assert season_1990[0]["Player"] == "Player C"  # Rank 1
    assert season_1990[0]["rank"] == 1
    assert season_1990[1]["Player"] == "Player A"  # Rank 2
    assert season_1990[1]["rank"] == 2
    
    # Check 1991 season - top 3
    season_1991 = result_df.filter(result_df.season_year == 1991).orderBy("rank").collect()
    assert len(season_1991) == 3
    assert season_1991[0]["Player"] == "Player H"  # Rank 1
    assert season_1991[0]["rank"] == 1


def test_get_top_players_single_season(spark):
    """Test getting top players with only one season."""
    schema = StructType([
        StructField("Player", StringType(), True),
        StructField("season_year", IntegerType(), True),
        StructField("cost_per_efficiency", DoubleType(), True),
    ])
    
    data = [
        ("Player A", 2000, 30000.0),
        ("Player B", 2000, 20000.0),  # Best
        ("Player C", 2000, 40000.0),
    ]
    
    df = spark.createDataFrame(data, schema)
    result_df = get_top_players_per_season(df, top_n=2)
    
    assert result_df.count() == 2
    results = result_df.orderBy("rank").collect()
    assert results[0]["Player"] == "Player B"
    assert results[0]["rank"] == 1


def test_get_top_players_fewer_than_n(spark):
    """Test when there are fewer players than requested top N."""
    schema = StructType([
        StructField("Player", StringType(), True),
        StructField("season_year", IntegerType(), True),
        StructField("cost_per_efficiency", DoubleType(), True),
    ])
    
    data = [
        ("Player A", 2005, 30000.0),
        ("Player B", 2005, 20000.0),
    ]
    
    df = spark.createDataFrame(data, schema)
    result_df = get_top_players_per_season(df, top_n=5)
    
    # Should return only 2 players even though we asked for 5
    assert result_df.count() == 2

