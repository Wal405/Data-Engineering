-- Load staging.customers from raw.customers, with validation
INSERT INTO staging.customers (customerid, name, email, city, country)
SELECT 
    customerid::INT,
    TRIM(name),
    LOWER(TRIM(email)),
    TRIM(city),
    TRIM(country)
FROM raw.customers
WHERE customerid IS NOT NULL
  AND customerid ~ '^\d+$'
  AND name IS NOT NULL
ON CONFLICT (customerid) DO NOTHING;

-- Load staging.products from raw.products, with validation
INSERT INTO staging.products (productid, title, category, price, rating)
SELECT 
    productid::INT,
    TRIM(title),
    TRIM(category),
    price::NUMERIC,
    rating::NUMERIC
FROM raw.products
WHERE productid IS NOT NULL
  AND productid ~ '^\d+$'
  AND price ~ '^\d+(\.\d+)?$'
  AND title IS NOT NULL
ON CONFLICT (productid) DO NOTHING;

-- Load staging.orders from raw.orders, with validation
INSERT INTO staging.orders (orderid, customerid, productid, quantity, orderdate, status)
SELECT 
    orderid::INT,
    customerid::INT,
    productid::INT,
    quantity::INT,
    orderdate::DATE,
    TRIM(status)
FROM raw.orders
WHERE orderid ~ '^\d+$'
  AND customerid ~ '^\d+$'
  AND productid ~ '^\d+$'
  AND quantity ~ '^\d+$'
ON CONFLICT (orderid) DO NOTHING;

-- Load staging.payments from raw.payments, with validation
INSERT INTO staging.payments (paymentid, orderid, paymentmethod, amount, paymentdate, paymentstatus)
SELECT 
    paymentid::INT,
    orderid::INT,
    TRIM(paymentmethod),
    amount::NUMERIC,
    paymentdate::DATE,
    TRIM(paymentstatus)
FROM raw.payments
WHERE paymentid ~ '^\d+$'
  AND orderid ~ '^\d+$'
  AND amount ~ '^\d+(\.\d+)?$'
ON CONFLICT (paymentid) DO NOTHING;