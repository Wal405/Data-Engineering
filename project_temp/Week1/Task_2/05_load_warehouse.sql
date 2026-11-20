-- Load dim_customers from staging.customers
INSERT INTO warehouse.dim_customers (customerid, name, email, city, country)
SELECT customerid, name, email, city, country
FROM staging.customers
ON CONFLICT (customerid) DO NOTHING;

-- Load dim_products from staging.products
INSERT INTO warehouse.dim_products (productid, title, category, price, rating)
SELECT productid, title, category, price, rating
FROM staging.products
ON CONFLICT (productid) DO NOTHING;

-- Load fact_orders by combining staging.orders with staging.payments
INSERT INTO warehouse.fact_orders (
    orderid, customerid, productid, quantity, orderdate, status,
    paymentmethod, amount, paymentstatus
)
SELECT 
    o.orderid,
    o.customerid,
    o.productid,
    o.quantity,
    o.orderdate,
    o.status,
    p.paymentmethod,
    p.amount,
    p.paymentstatus
FROM staging.orders o
LEFT JOIN staging.payments p ON o.orderid = p.orderid
ON CONFLICT (orderid) DO NOTHING;