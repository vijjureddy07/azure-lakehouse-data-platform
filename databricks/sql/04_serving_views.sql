-- ==============================================================================
-- 04_serving_views.sql
-- Module 6: Governed SQL Serving Views (Unity Catalog)
--
-- Schema: <catalog>.serving
-- Target Compute: Serverless Databricks SQL Warehouse
--
-- Exposes governed analytical views over:
-- 1. Curated Gold Business KPIs (6 views)
-- 2. Kimball Dimensional Model (2 enriched analytical views)
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS serving
COMMENT 'Governed analytical serving layer for SQL analysts, BI tools, and reporting dashboards';

USE SCHEMA serving;

-- ==============================================================================
-- 1. GOLD SERVING VIEWS (Business Aggregates & KPIs)
-- ==============================================================================

-- 1.1 Daily Sales Performance
CREATE OR REPLACE VIEW daily_sales_performance AS
SELECT
    order_date,
    total_orders,
    total_units_sold,
    gross_revenue,
    total_discounts,
    net_sales,
    total_cogs,
    gross_profit,
    returns_count,
    total_refunded_amount
FROM gold.gold_daily_sales_performance;

-- 1.2 Monthly Revenue Trajectory
CREATE OR REPLACE VIEW monthly_revenue AS
SELECT
    year,
    month,
    total_orders,
    total_units_sold,
    total_net_revenue,
    total_refunded_amount,
    ROUND(total_net_revenue - total_refunded_amount, 2) AS net_retained_revenue
FROM gold.gold_monthly_revenue;

-- 1.3 Store Regional Performance
CREATE OR REPLACE VIEW store_region_revenue AS
SELECT
    store_id,
    store_name,
    store_type,
    city,
    state,
    country,
    total_orders,
    total_net_revenue,
    avg_order_value
FROM gold.gold_revenue_by_store_region;

-- 1.4 Product Category Profitability
CREATE OR REPLACE VIEW category_revenue_performance AS
SELECT
    category,
    sub_category,
    units_sold,
    gross_revenue,
    net_revenue,
    units_returned,
    return_rate_pct,
    total_refunded_amount
FROM gold.gold_category_revenue_performance;

-- 1.5 Customer Spending Summary
CREATE OR REPLACE VIEW customer_spending_summary AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    loyalty_tier,
    total_orders,
    lifetime_spend,
    avg_order_value,
    first_order_date,
    latest_order_date
FROM gold.gold_customer_spending_summary;

-- 1.6 Return and Refund Performance
CREATE OR REPLACE VIEW return_refund_performance AS
SELECT
    return_reason,
    return_count,
    total_refund_amount,
    avg_refund_amount
FROM gold.gold_return_refund_performance;

-- ==============================================================================
-- 2. WAREHOUSE SERVING VIEWS (Star Schema Enriched Facts)
-- ==============================================================================

-- 2.1 Enriched Sales Detail (Point-in-Time SCD2 Customer Resolution)
-- Joins fact_sales directly to dim_customer via customer_key (the PIT resolved SCD2 surrogate key),
-- preserving the exact customer loyalty tier and address that was valid when the purchase occurred.
CREATE OR REPLACE VIEW sales_detail AS
SELECT
    -- Fact Grain & Line Items
    s.sales_key,
    s.order_item_id,
    s.order_id,
    s.order_timestamp,
    s.quantity,
    s.unit_price,
    s.gross_amount,
    s.discount_amount,
    s.net_amount,
    s.cost_amount,
    s.profit_amount,
    s.order_status,
    s.payment_method,

    -- Temporal Dimension
    d.full_date AS order_date,
    d.year AS order_year,
    d.quarter_name AS order_quarter,
    d.month_name AS order_month,
    d.day_name AS order_day_of_week,
    d.is_weekend AS is_weekend_order,

    -- SCD2 Customer Dimension (Point-in-Time Attributes)
    c.customer_key,
    c.customer_id,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.email AS customer_email,
    c.loyalty_tier AS customer_historical_loyalty_tier,
    c.city AS customer_historical_city,
    c.state AS customer_historical_state,
    c.postal_code AS customer_historical_postal_code,

    -- SCD1 Product Dimension (Current Attributes)
    p.product_key,
    p.product_id,
    p.product_name,
    p.category AS product_category,
    p.subcategory AS product_subcategory,
    p.current_retail_price,

    -- Store Dimension
    st.store_key,
    st.store_id,
    st.store_name,
    st.store_type,
    st.city AS store_city,
    st.state AS store_state,
    st.region AS store_region,

    -- Employee Dimension
    e.employee_key,
    e.employee_id,
    e.first_name AS employee_first_name,
    e.last_name AS employee_last_name,
    e.role AS employee_role

FROM warehouse.fact_sales s
JOIN warehouse.dim_date d ON s.order_date_key = d.date_key
JOIN warehouse.dim_customer c ON s.customer_key = c.customer_key
JOIN warehouse.dim_product p ON s.product_key = p.product_key
JOIN warehouse.dim_store st ON s.store_key = st.store_key
JOIN warehouse.dim_employee e ON s.employee_key = e.employee_key;

-- 2.2 Enriched Returns Detail
CREATE OR REPLACE VIEW returns_detail AS
SELECT
    r.return_key,
    r.return_id,
    r.order_item_id,
    r.order_id,
    r.return_timestamp,
    r.return_quantity,
    r.refund_amount,
    r.return_reason,

    -- Temporal Dimension
    d.full_date AS return_date,
    d.year AS return_year,
    d.month_name AS return_month,

    -- Customer Dimension (Historical at Return Time)
    c.customer_id,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.loyalty_tier AS customer_loyalty_tier,

    -- Product Dimension
    p.product_id,
    p.product_name,
    p.category AS product_category,

    -- Store Dimension
    st.store_id,
    st.store_name,
    st.region AS store_region

FROM warehouse.fact_returns r
JOIN warehouse.dim_date d ON r.return_date_key = d.date_key
JOIN warehouse.dim_customer c ON r.customer_key = c.customer_key
JOIN warehouse.dim_product p ON r.product_key = p.product_key
JOIN warehouse.dim_store st ON r.store_key = st.store_key;
