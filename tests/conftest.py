"""
Pytest Configuration and Fixtures for Local PySpark Testing.

Provides:
- A session-scoped local SparkSession fixture optimized for fast unit tests.
- Reusable test fixtures with small, deterministic DataFrames.
"""


import pytest

from src.config.settings import SparkConfig
from src.utils.spark import (
    detect_and_set_environment,
    get_spark_session,
    stop_spark_session,
)


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for unit and integration tests."""
    detect_and_set_environment()
    test_config = SparkConfig(
        app_name="PyTest_AzureLakehouse_Module1",
        master="local[2]",
        shuffle_partitions=2,
        driver_memory="1g",
        timezone="UTC",
        log_level="ERROR",
    )
    spark_session = get_spark_session(test_config)
    yield spark_session
    stop_spark_session(spark_session)
