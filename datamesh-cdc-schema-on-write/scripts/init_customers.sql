CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(100) DEFAULT 'US',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_country ON customers(country);

INSERT INTO customers (full_name, email, country) VALUES
('Alice Johnson', 'alice@example.com', 'US'),
('Bob Smith', 'bob@example.com', 'GB'),
('Charlie Brown', 'charlie@example.com', 'US');

-- Idempotent logical replication slot
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_customers'
    ) THEN
        PERFORM pg_create_logical_replication_slot('debezium_customers', 'pgoutput');
    END IF;
END $$;

-- Idempotent publication (PostgreSQL 15 has no IF NOT EXISTS for publications)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'
    ) THEN
        CREATE PUBLICATION dbz_publication FOR TABLE customers;
    END IF;
END $$;
