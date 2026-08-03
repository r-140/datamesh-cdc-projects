CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_orders_customer ON orders(customer_id);
INSERT INTO orders (customer_id, total_amount, status) VALUES
(1, 150.00, 'completed'), (2, 299.99, 'pending'), (3, 45.50, 'completed');
SELECT pg_create_logical_replication_slot('debezium', 'pgoutput');
CREATE PUBLICATION dbz_publication FOR TABLE orders;
