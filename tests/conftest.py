"""
Pytest configuration for CFDB ML Pipeline tests
"""
import pytest
import sys
from pathlib import Path

# Ensure src directory is in Python path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Disable bytecode writing to avoid permission issues in CI
sys.dont_write_bytecode = True


@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing.
    Only use this for integration tests that require actual Spark.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("cfdb_ml_tests") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        yield spark
        spark.stop()
    except ImportError:
        pytest.skip("PySpark not available for testing")


@pytest.fixture
def catalog_name():
    """Default catalog name for tests"""
    return "cfdb_test"


@pytest.fixture
def sample_team_data():
    """Sample team data for testing"""
    return [
        {"id": 1, "school": "Nebraska", "conference": "Big Ten", "classification": "fbs"},
        {"id": 2, "school": "Iowa", "conference": "Big Ten", "classification": "fbs"},
        {"id": 3, "school": "Michigan", "conference": "Big Ten", "classification": "fbs"}
    ]


@pytest.fixture
def sample_game_data():
    """Sample game data for testing"""
    return [
        {
            "id": 1,
            "season": 2024,
            "week": 1,
            "home_team": "Nebraska",
            "away_team": "Iowa",
            "home_points": 28,
            "away_points": 21
        }
    ]