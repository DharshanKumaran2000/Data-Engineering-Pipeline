CREATE TABLE users (
    user_id INT PRIMARY KEY,
    name TEXT,
    location TEXT
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name TEXT,
    category TEXT,
    price FLOAT
);

CREATE TABLE transactions (
    txn_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    product_id INT REFERENCES products(product_id),
    amount FLOAT,
    timestamp TIMESTAMP
);  