"""
Configuration and settings for Azure Lakehouse Data Platform (Module 1).
"""

from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
SAMPLE_DATA_DIR = DATA_DIR / "sample"
OUTPUT_DIR = PROJECT_ROOT / "output"
CLEANED_DIR = OUTPUT_DIR / "cleaned"
QUARANTINE_DIR = OUTPUT_DIR / "quarantine"
CURATED_DIR = OUTPUT_DIR / "curated"
METRICS_DIR = OUTPUT_DIR / "metrics"
SQL_DIR = PROJECT_ROOT / "sql"


@dataclass
class ScaleConfig:
    """Dataset scale configuration for data generation and pipeline execution."""
    name: str
    num_customers: int
    num_products: int
    num_stores: int
    num_employees: int
    num_orders: int
    max_items_per_order: int
    return_rate: float
    seed: int = 42
    # Defect injection rates (as fractions between 0.0 and 1.0)
    defect_rates: dict[str, float] = field(default_factory=lambda: {
        "duplicate_rows": 0.015,
        "duplicate_pks": 0.010,
        "null_mandatory": 0.020,
        "whitespace_casing": 0.050,
        "malformed_dates": 0.015,
        "negative_quantities": 0.015,
        "negative_prices": 0.010,
        "orphan_foreign_keys": 0.020,
        "invalid_emails": 0.020,
        "payment_unreconciled": 0.025,
        "invalid_statuses": 0.015,
    })


SCALE_PRESETS: dict[str, ScaleConfig] = {
    "sample": ScaleConfig(
        name="sample",
        num_customers=25,
        num_products=10,
        num_stores=3,
        num_employees=5,
        num_orders=50,
        max_items_per_order=3,
        return_rate=0.10,
        seed=42,
    ),
    "small": ScaleConfig(
        name="small",
        num_customers=500,
        num_products=50,
        num_stores=10,
        num_employees=50,
        num_orders=2_000,
        max_items_per_order=4,
        return_rate=0.08,
        seed=42,
    ),
    "standard": ScaleConfig(
        name="standard",
        num_customers=50_000,
        num_products=2_000,
        num_stores=100,
        num_employees=1_000,
        num_orders=200_000,
        max_items_per_order=5,
        return_rate=0.08,
        seed=42,
    ),
}


@dataclass
class SparkConfig:
    """Local Spark configuration."""
    app_name: str = "AzureLakehouse_LocalModule1"
    master: str = "local[*]"
    shuffle_partitions: int = 4
    driver_memory: str = "2g"
    timezone: str = "UTC"
    log_level: str = "WARN"


def ensure_directories():
    """Ensure all required input/output directories exist."""
    for path in [
        DATA_DIR,
        RAW_DATA_DIR,
        SAMPLE_DATA_DIR,
        OUTPUT_DIR,
        CLEANED_DIR,
        QUARANTINE_DIR,
        CURATED_DIR,
        METRICS_DIR,
        SQL_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)
