CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL DEFAULT 'US',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO customers (email, full_name, country) VALUES
('alice@example.com', 'Alice Johnson', 'US'),
('bob@example.com', 'Bob Smith', 'UK');
SELECT pg_create_logical_replication_slot('debezium', 'pgoutput');
CREATE PUBLICATION dbz_publication FOR TABLE customers;
