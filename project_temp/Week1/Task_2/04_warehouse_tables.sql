
-- Dimension: customers
CREATE TABLE warehouse.dim_customers (
    customerid INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    city TEXT,
    country TEXT
);

-- Dimension: products
CREATE TABLE warehouse.dim_products (
    productid INT PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT,
    price NUMERIC(10,2),
    rating NUMERIC(2,1)
);

-- Fact: orders (with payment info combined in)
CREATE TABLE warehouse.fact_orders (
    orderid INT PRIMARY KEY,
    customerid INT REFERENCES warehouse.dim_customers(customerid),
    productid INT REFERENCES warehouse.dim_products(productid),
    quantity INT,
    orderdate DATE,
    status TEXT,
    paymentmethod TEXT,
    amount NUMERIC(10,2),
    paymentstatus TEXT
);