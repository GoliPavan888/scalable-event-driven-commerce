CREATE TABLE IF NOT EXISTS product_sales_view (
  product_id INTEGER PRIMARY KEY,
  total_quantity_sold BIGINT NOT NULL DEFAULT 0,
  total_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0,
  order_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS category_metrics_view (
  category_name VARCHAR(255) PRIMARY KEY,
  total_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0,
  total_orders BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS customer_ltv_view (
  customer_id INTEGER PRIMARY KEY,
  total_spent NUMERIC(14, 2) NOT NULL DEFAULT 0,
  order_count BIGINT NOT NULL DEFAULT 0,
  last_order_date TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS hourly_sales_view (
  hour_timestamp TIMESTAMPTZ PRIMARY KEY,
  total_orders BIGINT NOT NULL DEFAULT 0,
  total_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS product_catalog (
  product_id INTEGER PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(255) NOT NULL,
  current_price NUMERIC(12, 2) NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_events (
  event_id VARCHAR(128) PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS projection_state (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
  last_processed_event_timestamp TIMESTAMPTZ NULL
);

INSERT INTO projection_state (singleton, last_processed_event_timestamp)
VALUES (TRUE, NULL)
ON CONFLICT (singleton) DO NOTHING;
