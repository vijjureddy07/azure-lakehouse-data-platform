"""
Unit tests for deterministic synthetic data generation.
"""

import tempfile
from pathlib import Path

from src.config.settings import ScaleConfig
from src.data_generation.generate_retail_data import RetailDataGenerator


def test_deterministic_data_generation():
    """Verify that same seed produces identical dataset sizes and files."""
    config = ScaleConfig(
        name="test_scale",
        num_customers=20,
        num_products=10,
        num_stores=2,
        num_employees=4,
        num_orders=30,
        max_items_per_order=2,
        return_rate=0.1,
        seed=123,
    )

    with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
        gen1 = RetailDataGenerator(config, output_dir=Path(tmpdir1))
        counts1 = gen1.generate_all()

        gen2 = RetailDataGenerator(config, output_dir=Path(tmpdir2))
        counts2 = gen2.generate_all()

        assert counts1 == counts2
        assert (Path(tmpdir1) / "customers.csv").exists()
        assert (Path(tmpdir1) / "products.csv").exists()
        assert (Path(tmpdir1) / "payments.json").exists()

        # Check content match
        with open(Path(tmpdir1) / "customers.csv") as f1, open(Path(tmpdir2) / "customers.csv") as f2:
            assert f1.read() == f2.read()
