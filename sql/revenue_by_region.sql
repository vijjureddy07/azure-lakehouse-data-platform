-- Revenue Performance by Store Region and Channel
-- Analyzes sales distribution across geographic territories and retail channels.

SELECT
    store_region,
    channel,
    COUNT(DISTINCT order_id) AS total_orders,
    CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS regional_net_revenue,
    CAST(SUM(gross_profit) AS DECIMAL(14, 2)) AS regional_gross_profit,
    CAST(
        SUM(net_sales) / COUNT(DISTINCT order_id) AS DECIMAL(10, 2)
    ) AS avg_order_value
FROM
    v_curated_sales
GROUP BY
    store_region,
    channel
ORDER BY
    regional_net_revenue DESC;
