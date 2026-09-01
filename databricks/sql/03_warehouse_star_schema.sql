-- ==============================================================================
-- 03_warehouse_star_schema.sql
-- Module 4: Enterprise Dimensional Modeling & Star Schema Analytics
--
-- Schema: retail_lakehouse.warehouse
-- ==============================================================================

USE CATALOG retail_lakehouse;
CREATE SCHEMA IF NOT EXISTS warehouse COMMENT 'Kimball Star Schema Dimensional Warehouse';
USE SCHEMA warehouse;

-- ==============================================================================
-- 1. ANALYTICAL QUERY: POINT-IN-TIME SALES BY HISTORICAL CUSTOMER LOYALTY TIER
-- Demonstrates SCD Type 2 point-in-time surrogate key resolution: sales are
-- aggregated by the loyalty tier the customer held at the exact time of purchase.
-- ==============================================================================
SELECT
    c.loyalty_tier,
    d.year,
    d.quarter_name,
    COUNT(DISTINCT s.order_id) AS total_orders,
    SUM(s.quantity) AS total_units_sold,
    SUM(s.gross_amount) AS total_gross_sales,
    SUM(s.discount_amount) AS total_discounts,
    SUM(s.net_amount) AS total_net_sales,
    SUM(s.cost_amount) AS total_cogs,
    SUM(s.profit_amount) AS total_profit,
    ROUND(SUM(s.profit_amount) / NULLIF(SUM(s.net_amount), 0) * 100, 2) AS profit_margin_pct
FROM retail_lakehouse.warehouse.fact_sales s
JOIN retail_lakehouse.warehouse.dim_customer c ON s.customer_key = c.customer_key
JOIN retail_lakehouse.warehouse.dim_date d ON s.order_date_key = d.date_key
GROUP BY c.loyalty_tier, d.year, d.quarter_name
ORDER BY d.year DESC, d.quarter_name DESC, total_net_sales DESC;

-- ==============================================================================
-- 2. ANALYTICAL QUERY: PRODUCT CATEGORY PROFITABILITY & DISCOUNT IMPACT
-- ==============================================================================
SELECT
    p.category,
    p.subcategory,
    COUNT(DISTINCT s.order_item_id) AS items_sold_count,
    SUM(s.quantity) AS total_quantity,
    SUM(s.gross_amount) AS gross_revenue,
    SUM(s.discount_amount) AS total_discounts_given,
    SUM(s.net_amount) AS net_sales_revenue,
    SUM(s.profit_amount) AS total_category_profit,
    ROUND(AVG(s.discount_amount / NULLIF(s.gross_amount, 0)) * 100, 2) AS avg_discount_pct
FROM retail_lakehouse.warehouse.fact_sales s
JOIN retail_lakehouse.warehouse.dim_product p ON s.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY net_sales_revenue DESC;

-- ==============================================================================
-- 3. ANALYTICAL QUERY: STORE REGIONAL PERFORMANCE WITH TEMPORAL DRILL-DOWN
-- ==============================================================================
SELECT
    st.region,
    st.store_name,
    d.year,
    d.month_name,
    COUNT(DISTINCT s.order_id) AS order_volume,
    SUM(s.net_amount) AS monthly_net_sales,
    SUM(s.profit_amount) AS monthly_store_profit,
    ROUND(SUM(s.net_amount) / NULLIF(COUNT(DISTINCT s.order_id), 0), 2) AS avg_order_value
FROM retail_lakehouse.warehouse.fact_sales s
JOIN retail_lakehouse.warehouse.dim_store st ON s.store_key = st.store_key
JOIN retail_lakehouse.warehouse.dim_date d ON s.order_date_key = d.date_key
GROUP BY st.region, st.store_name, d.year, d.month_name
ORDER BY st.region, monthly_net_sales DESC;

-- ==============================================================================
-- 4. ANALYTICAL QUERY: RETURN RATES & REFUND IMPACT BY PRODUCT CATEGORY
-- ==============================================================================
SELECT
    p.category,
    COUNT(DISTINCT s.order_item_id) AS total_sold_items,
    COUNT(DISTINCT r.return_id) AS returned_items_count,
    ROUND(COUNT(DISTINCT r.return_id) / NULLIF(COUNT(DISTINCT s.order_item_id), 0) * 100, 2) AS return_rate_pct,
    COALESCE(SUM(r.refund_amount), 0.00) AS total_refunds_issued
FROM retail_lakehouse.warehouse.fact_sales s
JOIN retail_lakehouse.warehouse.dim_product p ON s.product_key = p.product_key
LEFT JOIN retail_lakehouse.warehouse.fact_returns r ON s.order_item_id = r.order_item_id
GROUP BY p.category
ORDER BY return_rate_pct DESC;
