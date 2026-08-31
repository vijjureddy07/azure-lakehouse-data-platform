-- Top Products By Revenue per Category
-- Leverages Window Functions in Spark SQL to rank top revenue generators per department.

WITH ProductSummary AS (
    SELECT
        category,
        product_id,
        product_name,
        SUM(quantity) AS units_sold,
        CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS total_net_revenue,
        CAST(SUM(gross_profit) AS DECIMAL(14, 2)) AS total_profit
    FROM
        v_curated_sales
    GROUP BY
        category,
        product_id,
        product_name
),
RankedProducts AS (
    SELECT
        category,
        product_id,
        product_name,
        units_sold,
        total_net_revenue,
        total_profit,
        DENSE_RANK() OVER (
            PARTITION BY category
            ORDER BY total_net_revenue DESC
        ) AS category_revenue_rank
    FROM
        ProductSummary
)
SELECT
    category,
    category_revenue_rank,
    product_id,
    product_name,
    units_sold,
    total_net_revenue,
    total_profit
FROM
    RankedProducts
WHERE
    category_revenue_rank <= 5
ORDER BY
    category ASC,
    category_revenue_rank ASC;
