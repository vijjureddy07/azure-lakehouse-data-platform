"""
Unit tests for order, employee, order_item, and payment transformations and referential integrity.
"""

from src.schemas.retail_schemas import (
    RAW_CUSTOMERS_SCHEMA,
    RAW_EMPLOYEES_SCHEMA,
    RAW_ORDERS_SCHEMA,
    RAW_PAYMENTS_SCHEMA,
    RAW_STORES_SCHEMA,
)
from src.transformations.customers import transform_customers
from src.transformations.orders import (
    transform_employees,
    transform_orders,
    transform_payments,
    transform_stores,
)


def test_order_referential_integrity_and_orphan_rejection(spark):
    """Verify orders with non-existent customers or stores are quarantined as orphans."""
    # 1. Stores
    store_data = [("STR-0001", "Main Store", "Flagship", "West", "CA", "US", "2020-01-01")]
    stores_raw = spark.createDataFrame(store_data, schema=RAW_STORES_SCHEMA)
    clean_stores, _, _ = transform_stores(stores_raw)

    # 2. Employees
    emp_data = [("EMP-00001", "STR-0001", "Alex", "Smith", "alex@store.com", "Associate", "2021-01-01", "True")]
    emp_raw = spark.createDataFrame(emp_data, schema=RAW_EMPLOYEES_SCHEMA)
    clean_emp, _, _ = transform_employees(emp_raw, clean_stores)

    # 3. Customers
    cust_data = [("CUST-000001", "Jane", "Doe", "jane.doe@example.com", "555-0100", "123 St", "City", "CA", "US", "90210", "2023-01-01", "GOLD")]
    cust_raw = spark.createDataFrame(cust_data, schema=RAW_CUSTOMERS_SCHEMA)
    clean_cust, _, _ = transform_customers(cust_raw)

    # 4. Orders: 1 valid, 1 orphan customer, 1 orphan store
    order_data = [
        ("ORD-001", "CUST-000001", "STR-0001", "EMP-00001", "2023-08-01 14:00:00", "COMPLETED", "WEB", "0.00", "8.00", "100.00", "108.00"),
        ("ORD-002", "CUST-999999", "STR-0001", "EMP-00001", "2023-08-01 15:00:00", "COMPLETED", "WEB", "0.00", "8.00", "100.00", "108.00"),  # Orphan customer
        ("ORD-003", "CUST-000001", "STR-9999", "EMP-00001", "2023-08-01 16:00:00", "COMPLETED", "WEB", "0.00", "8.00", "100.00", "108.00"),  # Orphan store
    ]
    orders_raw = spark.createDataFrame(order_data, schema=RAW_ORDERS_SCHEMA)
    clean_orders, quar_orders, m_orders = transform_orders(orders_raw, clean_cust, clean_stores, clean_emp)

    assert clean_orders.count() == 1
    assert quar_orders.count() == 2

    quar_reasons = {r["rejection_reason"] for r in quar_orders.collect()}
    assert "ORPHAN_CUSTOMER_FK" in quar_reasons
    assert "ORPHAN_STORE_FK" in quar_reasons
    assert m_orders.referential_orphan_count == 2


def test_payment_reconciliation_check(spark):
    """Verify unreconciled payments are quarantined."""
    # Setup clean order
    store_raw = spark.createDataFrame([("STR-0001", "Store", "Mall", "West", "CA", "US", "2020-01-01")], schema=RAW_STORES_SCHEMA)
    clean_store, _, _ = transform_stores(store_raw)
    emp_raw = spark.createDataFrame([("EMP-00001", "STR-0001", "Alex", "Smith", "alex@store.com", "Associate", "2021-01-01", "True")], schema=RAW_EMPLOYEES_SCHEMA)
    clean_emp, _, _ = transform_employees(emp_raw, clean_store)
    cust_raw = spark.createDataFrame([("CUST-000001", "Jane", "Doe", "jane.doe@example.com", "555-0100", "123 St", "City", "CA", "US", "90210", "2023-01-01", "GOLD")], schema=RAW_CUSTOMERS_SCHEMA)
    clean_cust, _, _ = transform_customers(cust_raw)

    order_raw = spark.createDataFrame([
        ("ORD-001", "CUST-000001", "STR-0001", "EMP-00001", "2023-08-01 14:00:00", "COMPLETED", "WEB", "0.00", "8.00", "100.00", "108.00"),
    ], schema=RAW_ORDERS_SCHEMA)
    clean_orders, _, _ = transform_orders(order_raw, clean_cust, clean_store, clean_emp)

    payment_data = [
        ("PAY-001", "ORD-001", "2023-08-01 14:05:00", "CREDIT_CARD", "SUCCESS", "108.00", "TXN-101"),  # Reconciled
        ("PAY-002", "ORD-001", "2023-08-01 14:05:00", "CREDIT_CARD", "SUCCESS", "150.00", "TXN-102"),  # Unreconciled
    ]
    payments_raw = spark.createDataFrame(payment_data, schema=RAW_PAYMENTS_SCHEMA)
    clean_payments, quar_payments, _m_payments = transform_payments(payments_raw, clean_orders)

    assert clean_payments.count() == 1
    assert quar_payments.count() == 1
    assert quar_payments.collect()[0]["rejection_reason"] == "PAYMENT_AMOUNT_UNRECONCILED"
