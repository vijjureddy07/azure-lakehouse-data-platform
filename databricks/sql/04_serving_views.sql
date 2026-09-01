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

USE CATALOG IDENTIFIER(:catalog_name);

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
    region,
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
    subcategory,
    units_sold,
    gross_revenue,
    total_discounts,
    net_revenue,
    units_returned,
    total_refunded_amount,
    return_rate_pct
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
    first_order_date,
    latest_order_date,
    avg_order_value
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
-- Grain: EXACTLY ONE ROW PER FACT_SALES ROW (1 row per valid Silver order item).
-- Joins fact_sales directly to dim_customer via customer_key (the PIT resolved SCD2 surrogate key),
-- preserving the exact customer loyalty tier and address that was valid when the purchase occurred.
-- Note: Does NOT join dim_employee to prevent 1-to-many fanout and preserve fact grain.
CREATE OR REPLACE VIEW sales_detail AS
SELECT
    -- Fact Grain & Line Items
    s.order_item_id,
    s.order_id,
    s.order_timestamp,
    s.order_status,
    s.channel,
    s.quantity,
    s.unit_price,
    s.gross_amount,
    s.discount_amount,
    s.net_amount,
    s.cost_amount,
    s.profit_amount,

    -- Temporal Dimension
    d.date_key AS order_date_key,
    d.full_date AS order_date,
    d.year AS order_year,
    d.quarter_name AS order_quarter,
    d.month_name AS order_month,
    d.day_name AS order_day_of_week,
    d.is_weekend AS is_weekend_order,

    -- SCD2 Customer Dimension (Point-in-Time Attributes at Sale Time)
    c.customer_key,
    c.customer_id,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.email AS customer_email,
    c.loyalty_tier AS customer_historical_loyalty_tier,
    c.city AS customer_historical_city,
    c.state AS customer_historical_state,
    c.postal_code AS customer_historical_postal_code,

    -- SCD1 Product Dimension (Current Type-1 Attributes)
    p.product_key,
    p.product_id,
    p.product_sku,
    p.product_name,
    p.category AS product_category,
    p.subcategory AS product_subcategory,
    p.unit_price AS product_current_unit_price,

    -- Store Dimension
    st.store_key,
    st.store_id,
    st.store_name,
    st.store_type,
    st.region AS store_region,
    st.state AS store_state,
    st.country AS store_country

FROM warehouse.fact_sales s
JOIN warehouse.dim_date d ON s.order_date_key = d.date_key
JOIN warehouse.dim_customer c ON s.customer_key = c.customer_key
JOIN warehouse.dim_product p ON s.product_key = p.product_key
JOIN warehouse.dim_store st ON s.store_key = st.store_key;

-- 2.2 Enriched Returns Detail
-- Grain: EXACTLY ONE ROW PER FACT_RETURNS ROW (1 row per valid return event).
-- Note: Customer surrogate key (customer_key) is inherited from the associated original sale.
CREATE OR REPLACE VIEW returns_detail AS
SELECT
    -- Fact Grain
    r.return_id,
    r.order_item_id,
    r.order_id,
    r.return_timestamp,
    r.return_reason,
    r.return_status,
    r.refund_amount,

    -- Temporal Dimension
    d.date_key AS return_date_key,
    d.full_date AS return_date,
    d.year AS return_year,
    d.month_name AS return_month,

    -- Customer Dimension (Historical attributes inherited from the original sale)
    c.customer_key,
    c.customer_id,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.loyalty_tier AS customer_historical_loyalty_tier,

    -- Product Dimension
    p.product_key,
    p.product_id,
    p.product_sku,
    p.product_name,
    p.category AS product_category,
    p.subcategory AS product_subcategory,

    -- Store Dimension
    st.store_key,
    st.store_id,
    st.store_name,
    st.store_type,
    st.region AS store_region,
    st.state AS store_state,
    st.country AS store_country

FROM warehouse.fact_returns r
JOIN warehouse.dim_date d ON r.return_date_key = d.date_key
JOIN warehouse.dim_customer c ON r.customer_key = c.customer_key
JOIN warehouse.dim_product p ON r.product_key = p.product_key
JOIN warehouse.dim_store st ON r.store_key = st.store_key;
