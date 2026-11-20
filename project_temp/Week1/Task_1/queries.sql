-- Query 1: Orders with customer name only
SELECT o.orderid, c.name, o.status
FROM orders o
JOIN customers c ON o.customerid = c.customerid;
-- Query 2: Orders with customer name and product title
SELECT o.orderid, c.name, p.title, o.status
FROM orders o
JOIN customers c ON o.customerid = c.customerid
JOIN products p ON o.productid = p.productid;
-- Query 3: Orders that have no matching payment
SELECT o.orderid, o.status, p.paymentid
FROM orders o
LEFT JOIN payments p ON o.orderid = p.orderid;
-- Query 4: Total spent per customer, with names (using a CTE)
WITH customer_spend AS (
    SELECT o.customerid, SUM(pay.amount) AS total_spent
    FROM orders o
    JOIN payments pay ON o.orderid = pay.orderid
    GROUP BY o.customerid
)
SELECT c.name, cs.total_spent
FROM customer_spend cs
JOIN customers c ON cs.customerid = c.customerid;
-- Query 5: Rank products by price within each category
SELECT title, category, price,
       RANK() OVER (PARTITION BY category ORDER BY price DESC) AS price_rank
FROM products;
-- Query 6: Row number for products within each category
SELECT title, category, price,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS row_num
FROM products;
-- Query 7: Compare each payment to the previous payment (ordered by date)
SELECT paymentid, paymentdate, amount,
       LAG(amount) OVER (ORDER BY paymentdate) AS previous_amount
FROM payments;
-- Query 8: Compare each payment to the next payment (ordered by date)
SELECT paymentid, paymentdate, amount,
       LEAD(amount) OVER (ORDER BY paymentdate) AS next_amount
FROM payments;
-- Query 9: Label each product by price tier
SELECT title, price,
    CASE
        WHEN price >= 500 THEN 'Expensive'
        WHEN price >= 50 THEN 'Mid-range'
        ELSE 'Cheap'
    END AS price_tier
FROM products;
-- Query 10: Running total of payment amounts, ordered by date
SELECT paymentid, paymentdate, amount,SUM(amount) OVER (ORDER BY paymentdate) AS running_total
FROM payments;