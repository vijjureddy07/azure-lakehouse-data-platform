"""
PySpark Session Factory and Spark Helper Utilities.

Provides a clean, reusable SparkSession with:
- Local multi-threaded execution ('local[*]')
- Sensible shuffle partitions for development (avoids creating 200 tiny partitions)
- Strict UTC timezone enforcement for deterministic timestamp operations
- Automatic JAVA_HOME detection on macOS/Linux
- Clean shutdown logic
"""

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src.config.settings import SparkConfig

logger = logging.getLogger(__name__)


def detect_and_set_environment() -> None:
    """Set PYSPARK_PYTHON and detect Java home directory if not explicitly set."""
    # Ensure worker and driver Python versions strictly match active interpreter
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if os.environ.get("JAVA_HOME"):
        return

    candidate_paths = [
        "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
        "/opt/homebrew/opt/openjdk@17",
        "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home",
        "/opt/homebrew/opt/openjdk@11",
        "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home",
        "/opt/homebrew/opt/openjdk",
        "/usr/local/opt/openjdk@17",
        "/usr/local/opt/openjdk@11",
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-11-openjdk-amd64",
    ]

    for path_str in candidate_paths:
        p = Path(path_str)
        if p.exists():
            os.environ["JAVA_HOME"] = str(p)
            logger.info("Auto-detected and configured JAVA_HOME: %s", str(p))
            return


def get_spark_session(config: SparkConfig | None = None) -> SparkSession:
    """
    Build and return a deterministic, local-optimized SparkSession.

    Configurations applied and why:
    - master('local[*]'): Utilizes all available CPU cores on local machine for parallel tasks.
    - spark.sql.shuffle.partitions (default 4): Default 200 partitions creates severe file & memory
      overhead on small/medium local datasets. 4 partitions matches typical laptop core counts.
    - spark.sql.session.timeZone ('UTC'): Guarantees uniform timestamp serialization regardless
      of local machine timezone (e.g. IST, EST, UTC).
    - spark.driver.memory (e.g. '2g'): Prevents JVM Heap OutOfMemoryErrors during standard dataset batch runs.
    - spark.sql.adaptive.enabled ('true'): Enables Adaptive Query Execution (AQE) for optimal runtime joins.
    """
    detect_and_set_environment()
    cfg = config or SparkConfig()

    builder = (
        SparkSession.builder.appName(cfg.app_name)
        .master(cfg.master)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.session.timeZone", cfg.timezone)
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone={cfg.timezone}")
        .config("spark.executor.extraJavaOptions", f"-Duser.timezone={cfg.timezone}")
        .config("spark.ui.enabled", "false")  # Disable web UI in local test/batch runs to avoid port bind conflicts
    )

    try:
        import delta

        spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    except (ImportError, Exception):  # noqa: BLE001
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel(cfg.log_level)
    return spark


def stop_spark_session(spark: SparkSession | None) -> None:
    """Stop the active SparkSession cleanly."""
    if spark is not None:
        try:
            spark.stop()
            logger.info("SparkSession stopped cleanly.")
        except Exception as e:  # noqa: BLE001
            logger.warning("Error stopping SparkSession: %s", e)
