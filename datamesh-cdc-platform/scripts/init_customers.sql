-- Customers Domain Schema (v1.0)
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL DEFAULT 'US',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_country ON customers(country);

-- Insert seed data
INSERT INTO customers (email, full_name, country, created_at, updated_at) VALUES
('alice@example.com', 'Alice Johnson', 'US', '2024-01-10 08:00:00', '2024-01-10 08:00:00'),
('bob@example.com', 'Bob Smith', 'UK', '2024-01-11 09:00:00', '2024-01-11 09:00:00'),
('charlie@example.com', 'Charlie Brown', 'US', '2024-01-12 10:00:00', '2024-01-12 10:00:00'),
('diana@example.com', 'Diana Prince', 'DE', '2024-01-13 11:00:00', '2024-01-13 11:00:00');

-- Enable logical replication
SELECT pg_create_logical_replication_slot('debezium', 'pgoutput');

-- Publication for CDC
CREATE PUBLICATION dbz_publication FOR TABLE customers;
