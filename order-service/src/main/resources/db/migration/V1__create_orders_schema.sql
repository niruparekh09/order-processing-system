-- ============================================================
-- V1: Initial schema for Order Service
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── orders ───────────────────────────────────────────────────────────────────
CREATE TABLE orders (
                        id                  UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
                        customer_id         UUID            NOT NULL,
                        idempotency_key     VARCHAR(255)    NOT NULL,
                        status              VARCHAR(50)     NOT NULL DEFAULT 'PENDING',
                        total_amount        NUMERIC(19, 4)  NOT NULL,
                        currency            CHAR(3)         NOT NULL DEFAULT 'INR',
                        created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                        updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                        version             BIGINT          NOT NULL DEFAULT 0,

                        CONSTRAINT chk_orders_status CHECK (
                            status IN (
                                       'PENDING',
                                       'INVENTORY_RESERVED',
                                       'INVENTORY_FAILED',
                                       'COMPENSATING',
                                       'PAYMENT_FAILED',
                                       'COMPLETED',
                                       'CANCELLED'
                                )
                            ),
                        CONSTRAINT chk_orders_amount   CHECK (total_amount > 0),
                        CONSTRAINT uq_orders_idempotency_key UNIQUE (idempotency_key)
);

-- ── order_items ───────────────────────────────────────────────────────────────
CREATE TABLE order_items (
                             id              UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
                             order_id        UUID            NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                             product_id      UUID            NOT NULL,
                             product_name    VARCHAR(500)    NOT NULL,
                             quantity        INT             NOT NULL,
                             unit_price      NUMERIC(19, 4)  NOT NULL,
                             total_price     NUMERIC(19, 4)  NOT NULL,

                             CONSTRAINT chk_order_items_quantity     CHECK (quantity > 0),
                             CONSTRAINT chk_order_items_unit_price   CHECK (unit_price > 0),
                             CONSTRAINT chk_order_items_total_price  CHECK (total_price > 0)
);

-- ── outbox_events ─────────────────────────────────────────────────────────────
CREATE TABLE outbox_events (
                               id              UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
                               aggregate_id    UUID            NOT NULL,
                               aggregate_type  VARCHAR(100)    NOT NULL,
                               event_type      VARCHAR(100)    NOT NULL,
                               event_version   VARCHAR(10)     NOT NULL DEFAULT 'v1',
                               payload         JSONB           NOT NULL,
                               status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
                               kafka_topic     VARCHAR(255)    NOT NULL,
                               kafka_key       VARCHAR(255),
                               created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                               published_at    TIMESTAMPTZ,
                               retry_count     INT             NOT NULL DEFAULT 0,
                               last_error      TEXT,

                               CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
);

-- ── indexes ───────────────────────────────────────────────────────────────────

-- Outbox poller hot path - only scans PENDING rows
CREATE INDEX idx_outbox_pending
    ON outbox_events (created_at ASC)
    WHERE status = 'PENDING';

-- Order lookups by customer
CREATE INDEX idx_orders_customer_id
    ON orders (customer_id);

-- Order lookups by status (admin/support tooling)
CREATE INDEX idx_orders_status
    ON orders (status);

-- Outbox lookup by aggregate (debugging)
CREATE INDEX idx_outbox_aggregate_id
    ON outbox_events (aggregate_id);

-- ── auto-update updated_at trigger ───────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION fn_update_updated_at();