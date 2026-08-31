-- ==============================================================================
-- 02_gold_kpi_queries.sql
-- Module 3: Azure Databricks + Delta Lake + Medallion Lakehouse
--
-- Analytical SQL Queries against Gold Delta Tables in Unity Catalog
-- ==============================================================================

USE CATALOG retail_lakehouse;
USE SCHEMA gold;

-- 1. Daily Executive Summary: Sales, Margin & Return Rates
SELECT 
    order_date,
    total_orders,
    total_units_sold,
    gross_revenue,
    total_discounts,
    net_sales,
    total_cogs,
    gross_profit,
    ROUND((gross_profit / NULLIF(net_sales, 0)) * 100, 2) AS gross_margin_pct,
    returns_count,
    total_refunded_amount
FROM gold_daily_sales_performance
ORDER BY order_date DESC
LIMIT 30;

-- 2. Monthly Revenue Trajectory
SELECT 
    year,
    month,
    total_orders,
    total_units_sold,
    total_net_revenue,
    total_refunded_amount,
    ROUND(total_net_revenue - total_refunded_amount, 2) AS net_retained_revenue
FROM gold_monthly_revenue
ORDER BY year DESC, month DESC;

-- 3. Top Store Regional Contribution & Average Order Value (AOV)
SELECT 
    store_name,
    city,
    state,
    country,
    total_orders,
    total_net_revenue,
    avg_order_value,
    DENSE_RANK() OVER (ORDER BY total_net_revenue DESC) AS revenue_rank
FROM gold_revenue_by_store_region
ORDER BY total_net_revenue DESC
LIMIT 20;

-- 4. Product Category Profitability and Return Risk Breakdown
SELECT 
    category,
    sub_category,
    units_sold,
    gross_revenue,
    net_revenue,
    units_returned,
    return_rate_pct,
    total_refunded_amount
FROM gold_category_revenue_performance
ORDER BY net_revenue DESC;

-- 5. VIP Customer Cohort Spending
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
FROM gold_customer_spending_summary
WHERE loyalty_tier IN ('PLATINUM', 'GOLD')
ORDER BY lifetime_spend DESC
LIMIT 50;

-- 6. Return Reason Root Cause Analysis
SELECT 
    return_reason,
    return_count,
    total_refund_amount,
    avg_refund_amount,
    ROUND((return_count * 100.0) / SUM(return_count) OVER (), 2) AS pct_of_total_returns
FROM gold_return_refund_performance
ORDER BY return_count DESC;
