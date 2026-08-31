"""
Deterministic Synthetic Retail Data Generator with Injected Real-World Defects.

Generates 8 core retail domain datasets for Module 1:
1. customers.csv
2. products.csv
3. stores.csv
4. employees.csv
5. orders.csv
6. order_items.csv
7. payments.json (JSON Lines)
8. returns.csv

Intentional defect injection ensures students/recruiters can verify genuine
Data Engineering transformation, cleaning, quarantine, and reconciliation logic.
"""

import csv
import json
import logging
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.config.settings import (
    RAW_DATA_DIR,
    SCALE_PRESETS,
    ScaleConfig,
    ensure_directories,
)

logger = logging.getLogger(__name__)


class RetailDataGenerator:
    """Generates synthetic retail datasets with realistic defects using a fixed seed."""

    def __init__(self, config: ScaleConfig, output_dir: Path | None = None):
        self.config = config
        self.output_dir = output_dir or RAW_DATA_DIR
        self.rng = random.Random(config.seed)
        self.defect_rates = config.defect_rates

        # Static reference pools
        self.categories = {
            "Electronics": ["Laptops", "Smartphones", "Audio", "Accessories", "Wearables"],
            "Home & Living": ["Furniture", "Cookware", "Bedding", "Decor", "Lighting"],
            "Apparel": ["Men's Clothing", "Women's Clothing", "Footwear", "Outerwear"],
            "Beauty": ["Skincare", "Haircare", "Fragrance", "Cosmetics"],
            "Sports": ["Fitness Equipment", "Outdoor Gear", "Athletic Apparel"],
        }
        self.states = ["CA", "NY", "TX", "FL", "IL", "WA", "MA", "CO", "NC", "GA"]
        self.regions = {
            "CA": "West",
            "WA": "West",
            "CO": "West",
            "TX": "South",
            "FL": "South",
            "GA": "South",
            "NC": "South",
            "NY": "East",
            "MA": "East",
            "IL": "Central",
        }
        self.order_channels = ["IN_STORE", "WEB", "MOBILE_APP"]
        self.order_statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]
        self.payment_methods = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "APPLE_PAY", "CASH"]
        self.return_reasons = ["DEFECTIVE", "WRONG_SIZE", "UNSATISFIED", "NOT_AS_DESCRIBED", "ORDERED_BY_MISTAKE"]

    def _random_date(self, start_year: int = 2023, end_year: int = 2024) -> datetime:
        start_date = datetime(start_year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(end_year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        delta = end_date - start_date
        random_seconds = self.rng.randint(0, int(delta.total_seconds()))
        return start_date + timedelta(seconds=random_seconds)

    def generate_all(self) -> dict[str, int]:
        """Generate all datasets and write to disk. Returns row counts."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Starting deterministic data generation (Scale: %s, Seed: %d)...", self.config.name, self.config.seed)

        # 1. Stores
        stores = self._generate_stores()
        self._write_csv(self.output_dir / "stores.csv", stores)

        # 2. Employees
        employees = self._generate_employees(stores)
        self._write_csv(self.output_dir / "employees.csv", employees)

        # 3. Customers
        customers = self._generate_customers()
        self._write_csv(self.output_dir / "customers.csv", customers)

        # 4. Products
        products = self._generate_products()
        self._write_csv(self.output_dir / "products.csv", products)

        # 5 & 6. Orders and Order Items
        orders, order_items = self._generate_orders_and_items(customers, stores, employees, products)
        self._write_csv(self.output_dir / "orders.csv", orders)
        self._write_csv(self.output_dir / "order_items.csv", order_items)

        # 7. Payments (JSON)
        payments = self._generate_payments(orders)
        self._write_json(self.output_dir / "payments.json", payments)

        # 8. Returns
        returns = self._generate_returns(order_items)
        self._write_csv(self.output_dir / "returns.csv", returns)

        counts = {
            "stores": len(stores),
            "employees": len(employees),
            "customers": len(customers),
            "products": len(products),
            "orders": len(orders),
            "order_items": len(order_items),
            "payments": len(payments),
            "returns": len(returns),
        }
        logger.info("Data generation complete! Created files in %s: %s", self.output_dir, counts)
        return counts

    def _generate_stores(self) -> list[dict[str, Any]]:
        stores = []
        for i in range(1, self.config.num_stores + 1):
            store_id = f"STR-{i:04d}"
            state = self.rng.choice(self.states)
            region = self.regions[state]
            store_type = "Online" if i == 1 else self.rng.choice(["Flagship", "Mall", "Outlet"])
            opened = self._random_date(2015, 2022).strftime("%Y-%m-%d")
            store_name = f"Store {store_id} ({state})"

            row = {
                "store_id": store_id,
                "store_name": store_name,
                "store_type": store_type,
                "region": region,
                "state": state,
                "country": "US",
                "opened_date": opened,
            }
            stores.append(row)
        return stores

    def _generate_employees(self, stores: list[dict[str, Any]]) -> list[dict[str, Any]]:
        employees = []
        roles = ["Store Associate", "Store Associate", "Cashier", "Shift Supervisor", "Store Manager"]
        first_names = ["Alex", "Jordan", "Taylor", "Morgan", "Sam", "Chris", "Pat", "Riley", "Casey", "Dakota"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

        for i in range(1, self.config.num_employees + 1):
            emp_id = f"EMP-{i:05d}"
            store_id = self.rng.choice(stores)["store_id"]
            fn = self.rng.choice(first_names)
            ln = self.rng.choice(last_names)
            email = f"{fn.lower()}.{ln.lower()}{i}@retailplatform.com"
            role = self.rng.choice(roles)
            hire_date = self._random_date(2018, 2023).strftime("%Y-%m-%d")

            row = {
                "employee_id": emp_id,
                "store_id": store_id,
                "first_name": fn,
                "last_name": ln,
                "email": email,
                "role": role,
                "hire_date": hire_date,
                "is_active": "True",
            }
            employees.append(row)
        return employees

    def _generate_customers(self) -> list[dict[str, Any]]:
        customers = []
        first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Wilson", "Anderson", "Taylor"]
        tiers = ["STANDARD", "SILVER", "GOLD", "PLATINUM"]

        for i in range(1, self.config.num_customers + 1):
            cust_id = f"CUST-{i:06d}"
            fn = self.rng.choice(first_names)
            ln = self.rng.choice(last_names)
            state = self.rng.choice(self.states)
            email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
            signup = self._random_date(2021, 2024).strftime("%Y-%m-%d")
            tier = self.rng.choice(tiers)

            # Injected defects
            # 1. Whitespace & mixed casing
            if self.rng.random() < self.defect_rates["whitespace_casing"]:
                fn = f"  {fn.upper()}  "
                ln = f" {ln.lower()} "
                state = state.lower() if self.rng.random() < 0.5 else f" {state} "

            # 2. Invalid email format
            if self.rng.random() < self.defect_rates["invalid_emails"]:
                email = f"{fn.strip().lower()}_at_example_no_domain"

            # 3. Malformed date
            if self.rng.random() < self.defect_rates["malformed_dates"]:
                signup = "2023-99-99" if self.rng.random() < 0.5 else "INVALID_DATE"

            # 4. Null mandatory field
            if self.rng.random() < self.defect_rates["null_mandatory"]:
                if self.rng.random() < 0.5:
                    email = ""
                else:
                    cust_id = ""

            row = {
                "customer_id": cust_id,
                "first_name": fn,
                "last_name": ln,
                "email": email,
                "phone": f"+1-555-{self.rng.randint(100,999):03d}-{self.rng.randint(1000,9999):04d}",
                "address": f"{self.rng.randint(100, 9999)} Main Street",
                "city": "Metropolis",
                "state": state,
                "country": "US",
                "postal_code": f"{self.rng.randint(10000, 99999):05d}",
                "signup_date": signup,
                "loyalty_tier": tier,
            }
            customers.append(row)

            # 5. Duplicate Row defect
            if self.rng.random() < self.defect_rates["duplicate_rows"]:
                customers.append(dict(row))

            # 6. Duplicate PK defect (different person, reused ID)
            if cust_id and self.rng.random() < self.defect_rates["duplicate_pks"]:
                dup_row = dict(row)
                dup_row["first_name"] = "DuplicateUser"
                dup_row["email"] = f"dup.{i}@example.com"
                customers.append(dup_row)

        return customers

    def _generate_products(self) -> list[dict[str, Any]]:
        products = []
        product_counter = 1

        for cat, subcats in self.categories.items():
            for subcat in subcats:
                items_in_sub = max(1, self.config.num_products // (len(self.categories) * len(subcats)))
                for _ in range(items_in_sub):
                    prod_id = f"PROD-{product_counter:05d}"
                    sku = f"SKU-{cat[:3].upper()}-{subcat[:3].upper()}-{product_counter:05d}"
                    name = f"{subcat} Model {product_counter}"
                    price_val = round(self.rng.uniform(10.0, 800.0), 2)
                    cost_val = round(price_val * self.rng.uniform(0.4, 0.7), 2)
                    is_active = "True"
                    description = f"High quality {name} in {cat} category."

                    # Injected defects
                    # 1. Negative price
                    if self.rng.random() < self.defect_rates["negative_prices"]:
                        price_val = -1 * abs(price_val)

                    # 2. Whitespace / Casing in category
                    if self.rng.random() < self.defect_rates["whitespace_casing"]:
                        cat = f"  {cat.upper()} "
                        subcat = f" {subcat.lower()}  "

                    # 3. Null mandatory field
                    if self.rng.random() < self.defect_rates["null_mandatory"]:
                        prod_id = ""

                    row = {
                        "product_id": prod_id,
                        "product_sku": sku,
                        "product_name": name,
                        "category": cat.strip().title(),
                        "subcategory": subcat.strip().title(),
                        "unit_price": f"{price_val:.2f}",
                        "cost_price": f"{cost_val:.2f}",
                        "is_active": is_active,
                        "description": description,
                    }
                    products.append(row)
                    product_counter += 1

                    # 4. Duplicate PK defect
                    if prod_id and self.rng.random() < self.defect_rates["duplicate_pks"]:
                        dup_prod = dict(row)
                        dup_prod["product_name"] = f"Duplicate {name}"
                        products.append(dup_prod)

        return products

    def _generate_orders_and_items(
        self,
        customers: list[dict[str, Any]],
        stores: list[dict[str, Any]],
        employees: list[dict[str, Any]],
        products: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        orders = []
        order_items = []
        valid_cust_ids = [c["customer_id"] for c in customers if c["customer_id"]]
        valid_store_ids = [s["store_id"] for s in stores if s["store_id"]]
        valid_emp_ids = [e["employee_id"] for e in employees if e["employee_id"]]
        valid_prods = [p for p in products if p["product_id"] and float(p["unit_price"]) > 0]

        item_id_counter = 1

        for i in range(1, self.config.num_orders + 1):
            order_id = f"ORD-{i:07d}"
            cust_id = self.rng.choice(valid_cust_ids)
            store_id = self.rng.choice(valid_store_ids)
            emp_id = self.rng.choice(valid_emp_ids)
            order_dt = self._random_date(2023, 2024)
            order_ts = order_dt.strftime("%Y-%m-%d %H:%M:%S")
            status = self.rng.choice(self.order_statuses)
            channel = self.rng.choice(self.order_channels)

            # Injected defects on Orders:
            # 1. Orphan Customer FK
            if self.rng.random() < self.defect_rates["orphan_foreign_keys"]:
                cust_id = "CUST-999999"

            # 2. Orphan Store FK
            if self.rng.random() < self.defect_rates["orphan_foreign_keys"]:
                store_id = "STR-9999"

            # 3. Invalid status
            if self.rng.random() < self.defect_rates["invalid_statuses"]:
                status = "UNKNOWN_STATUS_INVALID"

            # 4. Malformed date
            if self.rng.random() < self.defect_rates["malformed_dates"]:
                order_ts = "2024-02-31 25:61:99"

            # Generate items for this order
            num_items = self.rng.randint(1, self.config.max_items_per_order)
            subtotal = Decimal("0.00")

            for _ in range(num_items):
                item_id = f"ITEM-{item_id_counter:08d}"
                item_id_counter += 1
                prod = self.rng.choice(valid_prods)
                prod_id = prod["product_id"]
                unit_price = Decimal(prod["unit_price"])
                quantity = self.rng.randint(1, 5)
                discount_pct = Decimal(str(self.rng.choice([0.0, 0.0, 0.05, 0.10, 0.15, 0.20])))

                # Injected defects on Order Items:
                # Negative or Zero Quantity
                if self.rng.random() < self.defect_rates["negative_quantities"]:
                    quantity = -1 if self.rng.random() < 0.5 else 0

                # Orphan Product FK
                if self.rng.random() < self.defect_rates["orphan_foreign_keys"]:
                    prod_id = "PROD-99999"

                line_gross = Decimal(quantity) * unit_price
                line_discount = line_gross * discount_pct
                line_net = line_gross - line_discount
                if quantity > 0:
                    subtotal += line_net

                order_items.append({
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": prod_id,
                    "quantity": str(quantity),
                    "unit_price": f"{unit_price:.2f}",
                    "discount_percent": f"{discount_pct:.2f}",
                })

            shipping = Decimal("0.00") if channel == "IN_STORE" else Decimal("9.99")
            tax = (subtotal * Decimal("0.08")).quantize(Decimal("0.01"))
            total = subtotal + shipping + tax

            orders.append({
                "order_id": order_id,
                "customer_id": cust_id,
                "store_id": store_id,
                "employee_id": emp_id,
                "order_timestamp": order_ts,
                "order_status": status,
                "channel": channel,
                "shipping_cost": f"{shipping:.2f}",
                "tax_amount": f"{tax:.2f}",
                "order_subtotal": f"{subtotal:.2f}",
                "total_amount": f"{total:.2f}",
            })

            # Duplicate Order ID defect
            if self.rng.random() < self.defect_rates["duplicate_pks"]:
                dup_order = dict(orders[-1])
                dup_order["total_amount"] = f"{(total + Decimal('100.00')):.2f}"
                orders.append(dup_order)

        return orders, order_items

    def _generate_payments(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payments = []
        payment_id_counter = 1

        for order in orders:
            # Skip if order_id is missing
            if not order["order_id"]:
                continue

            pay_id = f"PAY-{payment_id_counter:07d}"
            payment_id_counter += 1
            order_id = order["order_id"]
            amount = Decimal(order["total_amount"])
            method = self.rng.choice(self.payment_methods)
            status = "SUCCESS" if order["order_status"] in ["COMPLETED", "REFUNDED"] else "FAILED"
            ts = order["order_timestamp"]

            # Injected defects on Payments:
            # 1. Unreconciled payment amount
            if self.rng.random() < self.defect_rates["payment_unreconciled"]:
                amount += Decimal("25.00")

            # 2. Orphan Order FK
            if self.rng.random() < self.defect_rates["orphan_foreign_keys"]:
                order_id = "ORD-9999999"

            # 3. Invalid payment status
            if self.rng.random() < self.defect_rates["invalid_statuses"]:
                status = "STATUS_UNKNOWN_INVALID"

            payments.append({
                "payment_id": pay_id,
                "order_id": order_id,
                "payment_timestamp": ts,
                "payment_method": method,
                "payment_status": status,
                "payment_amount": f"{amount:.2f}",
                "transaction_reference": f"TXN-{self.rng.randint(1000000, 9999999)}",
            })

        return payments

    def _generate_returns(self, order_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        returns = []
        return_id_counter = 1

        for item in order_items:
            if self.rng.random() < self.config.return_rate:
                ret_id = f"RET-{return_id_counter:07d}"
                return_id_counter += 1
                item_id = item["order_item_id"]
                reason = self.rng.choice(self.return_reasons)
                qty = int(item["quantity"]) if item["quantity"].isdigit() else 1
                price = Decimal(item["unit_price"]) if Decimal(item["unit_price"]) > 0 else Decimal("10.00")
                refund = (Decimal(qty) * price).quantize(Decimal("0.01"))
                ret_ts = self._random_date(2023, 2024).strftime("%Y-%m-%d %H:%M:%S")

                # Injected defect: Orphan Item FK
                if self.rng.random() < self.defect_rates["orphan_foreign_keys"]:
                    item_id = "ITEM-99999999"

                returns.append({
                    "return_id": ret_id,
                    "order_item_id": item_id,
                    "return_timestamp": ret_ts,
                    "return_reason": reason,
                    "refund_amount": f"{refund:.2f}",
                    "return_status": "APPROVED",
                })

        return returns

    def _write_csv(self, file_path: Path, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        fieldnames = list(rows[0].keys())
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _write_json(self, file_path: Path, rows: list[dict[str, Any]]) -> None:
        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(json.dumps(row) + "\n" for row in rows)


def generate_all_datasets(config: ScaleConfig, output_dir: Path | None = None) -> dict[str, int]:
    """Generate all datasets for a given ScaleConfig instance."""
    ensure_directories()
    generator = RetailDataGenerator(config, output_dir=output_dir)
    return generator.generate_all()


def generate_retail_dataset(scale: str = "small", output_dir: Path | None = None) -> dict[str, int]:
    """Entrypoint function to generate dataset for a given scale."""
    config = SCALE_PRESETS.get(scale.lower())
    if not config:
        raise ValueError(f"Unknown scale '{scale}'. Available options: {list(SCALE_PRESETS.keys())}")

    return generate_all_datasets(config, output_dir=output_dir)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic retail dataset")
    parser.add_argument("--scale", choices=["sample", "small", "standard"], default="small", help="Scale preset (sample, small, or standard)")
    parser.add_argument("--output-dir", type=str, default=None, help="Optional custom output directory path")
    args = parser.parse_args()
    out_path = Path(args.output_dir) if args.output_dir else None
    generate_retail_dataset(scale=args.scale, output_dir=out_path)
