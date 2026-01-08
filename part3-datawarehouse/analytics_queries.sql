
-- analytics_queries.sql

-- Query 1: Monthly Sales Drill-Down
-- Business Scenario: The CEO wants to see sales performance broken down by time periods.
-- Demonstrates: Drill-down from Year → Quarter → Month for 2024

SELECT
    d.year,
    d.quarter,
    d.month_name,
    SUM(fs.total_amount)  AS total_sales,
    SUM(fs.quantity_sold) AS total_quantity
FROM fact_sales fs
JOIN dim_date d
  ON fs.date_key = d.date_key
WHERE d.year = 2024
GROUP BY d.year, d.quarter, d.month_name, d.month
ORDER BY d.year, d.quarter, d.month;


-- Query 2: Top 10 Products by Revenue
-- Business Scenario: Identify top-performing products and their revenue contribution

WITH product_revenue AS (
    SELECT
        p.product_key,
        p.product_name,
        p.category,
        SUM(fs.quantity_sold) AS units_sold,
        SUM(fs.total_amount)  AS revenue
    FROM fact_sales fs
    JOIN dim_product p
      ON fs.product_key = p.product_key
    GROUP BY p.product_key, p.product_name, p.category
),

revenue_total AS (
    SELECT SUM(revenue) AS overall_revenue
    FROM product_revenue
)

SELECT
    pr.product_name,
    pr.category,
    pr.units_sold,
    pr.revenue,
    ROUND((pr.revenue / rt.overall_revenue) * 100, 2) AS revenue_percentage
FROM product_revenue pr
CROSS JOIN revenue_total rt
ORDER BY pr.revenue DESC
FETCH FIRST 10 ROWS ONLY;


-- Query 3: Customer Segmentation
-- Business Scenario: Segment customers into High / Medium / Low value based on total spend

WITH customer_spend AS (
    SELECT
        c.customer_key,
        SUM(fs.total_amount) AS total_revenue
    FROM fact_sales fs
    JOIN dim_customer c
      ON fs.customer_key = c.customer_key
    GROUP BY c.customer_key
),

segmented_customers AS (
    SELECT
        customer_key,
        total_revenue,
        CASE
            WHEN total_revenue > 50000 THEN 'High Value'
            WHEN total_revenue BETWEEN 20000 AND 50000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment
    FROM customer_spend
)

SELECT
    customer_segment,
    COUNT(customer_key)      AS customer_count,
    SUM(total_revenue)       AS total_revenue,
    ROUND(AVG(total_revenue), 2) AS avg_revenue_per_customer
FROM segmented_customers
GROUP BY customer_segment
ORDER BY
    CASE customer_segment
        WHEN 'High Value' THEN 1
        WHEN 'Medium Value' THEN 2
        ELSE 3
    END;
