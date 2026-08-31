-- Daily and Monthly Revenue Aggregation
-- Analyzes sales trends, distinct active customers, and average transaction sizes.

SELECT
    order_year,
    order_month,
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    CAST(SUM(gross_sales) AS DECIMAL(14, 2)) AS total_gross_revenue,
    CAST(SUM(discount_amount) AS DECIMAL(14, 2)) AS total_discounts,
    CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS total_net_revenue,
    CAST(AVG(net_sales) AS DECIMAL(10, 2)) AS avg_line_item_value
FROM
    v_curated_sales
GROUP BY
    order_year,
    order_month,
    order_date
ORDER BY
    order_date ASC;
