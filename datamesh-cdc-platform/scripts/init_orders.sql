-- Orders Domain Schema (v1.0)
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);

-- Insert seed data
INSERT INTO orders (customer_id, total_amount, status, created_at, updated_at) VALUES
(1, 150.00, 'completed', '2024-01-15 10:00:00', '2024-01-15 10:05:00'),
(2, 299.99, 'pending', '2024-01-15 11:00:00', '2024-01-15 11:00:00'),
(3, 45.50, 'completed', '2024-01-15 12:00:00', '2024-01-15 12:30:00'),
(1, 89.99, 'shipped', '2024-01-16 09:00:00', '2024-01-16 14:00:00'),
(4, 1200.00, 'pending', '2024-01-16 10:00:00', '2024-01-16 10:00:00');

-- Enable logical replication
SELECT pg_create_logical_replication_slot('debezium', 'pgoutput');

-- Publication for CDC
CREATE PUBLICATION dbz_publication FOR TABLE orders;
