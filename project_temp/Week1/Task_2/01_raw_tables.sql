-- RAW LAYER: mirrors source CSVs closely, loose typing on purpose
CREATE TABLE raw.customers (
    customerid TEXT,
    name TEXT,
    email TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE raw.products (
    productid TEXT,
    title TEXT,
    category TEXT,
    price TEXT,
    rating TEXT
);

CREATE TABLE raw.orders (
    orderid TEXT,
    customerid TEXT,
    productid TEXT,
    quantity TEXT,
    orderdate TEXT,
    status TEXT
);

CREATE TABLE raw.payments (
    paymentid TEXT,
    orderid TEXT,
    paymentmethod TEXT,
    amount TEXT,
    paymentdate TEXT,
    paymentstatus TEXT
);