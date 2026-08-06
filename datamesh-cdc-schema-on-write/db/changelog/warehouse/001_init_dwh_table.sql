--liquibase formatted sql
--changeset dev-team:001_init_schema

CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    external_id     VARCHAR(64) NOT NULL,
    customer_id     BIGINT NOT NULL,
    status          VARCHAR(32) NOT NULL DEFAULT 'CREATED',
    amount          NUMERIC(19,4),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ
);

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status) WHERE status != 'ARCHIVED';

--rollback DROP TABLE orders;