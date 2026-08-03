-- Demo: Schema Evolution in PostgreSQL
-- Run these commands to simulate schema changes

-- ========== ORDERS DOMAIN ==========\n\n-- 1. Add optional field (compatible)
ALTER TABLE orders ADD COLUMN promo_code VARCHAR(50) DEFAULT NULL;

-- 2. Add required field (forward incompatible without default)
-- ALTER TABLE orders ADD COLUMN priority INT NOT NULL; -- This would fail BACKWARD check

-- 3. Safe: add with default
ALTER TABLE orders ADD COLUMN discount_pct DECIMAL(5,2) DEFAULT 0.0;

-- 4. Breaking change: remove consumed field
-- ALTER TABLE orders DROP COLUMN total_amount; -- This pauses opt-out pipelines

-- 5. Type widening (int -> bigint)
-- ALTER TABLE orders ALTER COLUMN customer_id TYPE BIGINT;

-- ========== CUSTOMERS DOMAIN ==========\n\n-- 1. Add field
ALTER TABLE customers ADD COLUMN phone VARCHAR(20) DEFAULT NULL;

-- 2. Add field with default
ALTER TABLE customers ADD COLUMN is_vip BOOLEAN DEFAULT FALSE;

-- Check current schema
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'orders';
