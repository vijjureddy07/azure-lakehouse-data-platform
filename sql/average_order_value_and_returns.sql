-- Category Return Rate & Profitability Impact
-- Joins curated sales lines with returns to evaluate product return behavior per category.

WITH CategorySales AS (
    SELECT
        category,
        COUNT(order_item_id) AS total_items_sold,
        CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS category_net_revenue
    FROM
        v_curated_sales
    GROUP BY
        category
),
CategoryReturns AS (
    SELECT
        s.category,
        COUNT(r.return_id) AS total_returns,
        CAST(SUM(r.refund_amount) AS DECIMAL(14, 2)) AS total_refunded
    FROM
        v_curated_sales s
    INNER JOIN
        v_returns r ON s.order_item_id = r.order_item_id
    WHERE
        r.return_status = 'APPROVED'
    GROUP BY
        s.category
)
SELECT
    cs.category,
    cs.total_items_sold,
    COALESCE(cr.total_returns, 0) AS total_returned_items,
    ROUND(
        (COALESCE(cr.total_returns, 0) * 100.0) / cs.total_items_sold, 2
    ) AS return_rate_percent,
    cs.category_net_revenue,
    COALESCE(cr.total_refunded, CAST(0.00 AS DECIMAL(14, 2))) AS total_refunded_amount,
    CAST(
        cs.category_net_revenue - COALESCE(cr.total_refunded, 0.0) AS DECIMAL(14, 2)
    ) AS final_net_realized_revenue
FROM
    CategorySales cs
LEFT JOIN
    CategoryReturns cr ON cs.category = cr.category
ORDER BY
    return_rate_percent DESC;
