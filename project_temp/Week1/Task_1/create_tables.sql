CREATE TABLE customers (
    customerid INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    city TEXT,
    country TEXT
);

CREATE TABLE products (
    productid INT PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT,
    price NUMERIC(10,2),
    rating NUMERIC(2,1)
);

CREATE TABLE orders (
    orderid INT PRIMARY KEY,
    customerid INT REFERENCES customers(customerid),
    productid INT REFERENCES products(productid),
    quantity INT,
    orderdate DATE,
    status TEXT
);

CREATE TABLE payments (
    paymentid INT PRIMARY KEY,
    orderid INT REFERENCES orders(orderid),
    paymentmethod TEXT,
    amount NUMERIC(10,2),
    paymentdate DATE,
    paymentstatus TEXT
);