-- STAGING LAYER: cleaned, validated, properly typed
CREATE TABLE staging.customers (
    customerid INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE staging.products (
    productid INT PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT,
    price NUMERIC(10,2),
    rating NUMERIC(2,1)
);

CREATE TABLE staging.orders (
    orderid INT PRIMARY KEY,
    customerid INT,
    productid INT,
    quantity INT,
    orderdate DATE,
    status TEXT
);

CREATE TABLE staging.payments (
    paymentid INT PRIMARY KEY,
    orderid INT,
    paymentmethod TEXT,
    amount NUMERIC(10,2),
    paymentdate DATE,
    paymentstatus TEXT
);