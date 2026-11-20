-- Query 1: Running total of order amounts over time
SELECT orderid, orderdate, amount,
       SUM(amount) OVER (ORDER BY orderdate) AS running_total
FROM warehouse.fact_orders
WHERE amount IS NOT NULL;

-- Query 2: Compare each order's amount to the previous order
SELECT orderid, orderdate, amount,
       LAG(amount) OVER (ORDER BY orderdate) AS previous_order_amount
FROM warehouse.fact_orders
WHERE amount IS NOT NULL;

-- Query 3: Compare each order's amount to the next order
SELECT orderid, orderdate, amount,
       LEAD(amount) OVER (ORDER BY orderdate) AS next_order_amount
FROM warehouse.fact_orders
WHERE amount IS NOT NULL;

-- Query 4: Total spend per customer (CTE), using warehouse fact + dimension tables
WITH customer_totals AS (
    SELECT customerid, SUM(amount) AS total_spent
    FROM warehouse.fact_orders
    WHERE amount IS NOT NULL
    GROUP BY customerid
)
SELECT c.name, ct.total_spent
FROM customer_totals ct
JOIN warehouse.dim_customers c ON ct.customerid = c.customerid
ORDER BY ct.total_spent DESC;

