CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);

INSERT INTO orders (customer_id, total_amount, status) VALUES
(1, 150.00, 'completed'),
(2, 299.99, 'pending'),
(3, 45.50, 'completed');

-- Idempotent logical replication slot
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_orders'
    ) THEN
        PERFORM pg_create_logical_replication_slot('debezium_orders', 'pgoutput');
    END IF;
END $$;

-- Idempotent publication (PostgreSQL 15 has no IF NOT EXISTS for publications)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'
    ) THEN
        CREATE PUBLICATION dbz_publication FOR TABLE orders;
    END IF;
END $$;
