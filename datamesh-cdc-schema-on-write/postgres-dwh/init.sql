CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS raw_bronze;
CREATE SCHEMA IF NOT EXISTS raw_silver;
CREATE SCHEMA IF NOT EXISTS raw_gold;

-- Source tables (seed data for dbt)
CREATE TABLE raw.orders (
    id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    customer_email VARCHAR,
    status VARCHAR,
    total_amount NUMERIC(19,4),
    order_date DATE
);

CREATE TABLE raw.customers (
    id VARCHAR PRIMARY KEY,
    email VARCHAR,
    segment VARCHAR,
    registration_date DATE
);

INSERT INTO raw.orders VALUES
('1', '101', 'alice@example.com', 'confirmed', 150.00, '2024-01-15'),
('2', '102', 'bob@example.com', 'shipped', 230.50, '2024-01-16'),
('3', '103', 'charlie@example.com', 'pending', 89.99, '2024-01-17');

INSERT INTO raw.customers VALUES
('101', 'alice@example.com', 'premium', '2023-06-01'),
('102', 'bob@example.com', 'standard', '2023-08-15'),
('103', 'charlie@example.com', 'basic', '2024-01-01');