## Task 1.3: Business Query Implementation 

# Query 1: Customer Purchase History
# Business Question: "Generate a detailed report showing each customer's name, email, total number of orders placed, and total amount spent. 
-- Include only customers who have placed at least 2 orders and spent more than ₹5,000. Order by total amount spent in descending order."


SELECT
    (c.first_name || ' ' || c.last_name) AS customer_name,
    c.email AS email,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity * oi.unit_price) AS total_spent
FROM customers AS c
JOIN orders AS o
  ON o.customer_id = c.customer_id
JOIN order_items AS oi
  ON oi.order_id = o.order_id
GROUP BY
    c.first_name, c.last_name, c.email
HAVING
    COUNT(DISTINCT o.order_id) >= 2
    AND SUM(oi.quantity * oi.unit_price) > 5000
ORDER BY
    total_spent DESC;

# Query 2: Product Sales Analysis (5 marks)
# Business Question: "For each product category, show the category name, number of different products sold, total quantity sold, and total revenue generated. 
## Only include categories that have generated more than ₹10,000 in revenue. Order by total revenue descending

SELECT
    p.category AS category,
    COUNT(DISTINCT p.product_id) AS num_products,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM products AS p
JOIN order_items AS oi
  ON oi.product_id = p.product_id
GROUP BY
    p.category
HAVING
    SUM(oi.quantity * oi.unit_price) > 10000
ORDER BY
    total_revenue DESC;


# Query 3: Monthly Sales Trend (5 marks)
Business Question: "Show monthly sales trends for the year 2024. For each month, display the month name, total number of orders, total revenue, 
and the running total of revenue (cumulative revenue from January to that month)."


WITH monthly AS (
    SELECT
        date_trunc('month', o.order_date)::date AS month,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.quantity * oi.unit_price) AS monthly_revenue
    FROM orders AS o
    JOIN order_items AS oi
      ON oi.order_id = o.order_id
    WHERE o.order_date >= DATE '2024-01-01'
      AND o.order_date <  DATE '2025-01-01'
    GROUP BY date_trunc('month', o.order_date)
)
SELECT
    TO_CHAR(month, 'FMMonth') AS month_name,               -- e.g., January, February...
    total_orders,
    monthly_revenue,
    SUM(monthly_revenue) OVER (ORDER BY month
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cumulative_revenue
FROM monthly
ORDER BY month;

