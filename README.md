# Scalable Event-Driven Commerce Analytics (CQRS + Outbox)

This project implements a high-throughput e-commerce analytics backend using:

- CQRS (separate write and read models)
- Event-driven architecture with RabbitMQ
- Transactional outbox pattern for reliable event publishing
- Idempotent consumers for at-least-once delivery
- Materialized analytics views for fast query APIs

## Architecture

### Services

- command-service (port 8080): Handles write operations.
- consumer-service: Consumes events and builds analytics projections in the read database.
- query-service (port 8081): Exposes read-only analytics endpoints.
- db: Primary write database (PostgreSQL).
- read-db: Read/projection database (PostgreSQL).
- broker: RabbitMQ message broker.

### Data Flow

1. Client calls command-service endpoint.
2. command-service writes business data and outbox event in a single DB transaction.
3. Background outbox publisher reads unpublished outbox rows and publishes to RabbitMQ.
4. consumer-service processes events idempotently and updates read-model tables.
5. query-service reads from projection tables for low-latency analytics.

See docs/data-flow.mmd for a diagram source.

## Project Structure

- command-service/: write API + outbox publisher
- consumer-service/: event consumers + projection updater
- query-service/: read-only analytics API
- db/write-init/: write-model schema bootstrap SQL
- db/read-init/: read-model schema bootstrap SQL
- docker-compose.yml: full environment orchestration
- .env.example: environment variables documentation
- submission.json: evaluation endpoints

## Write Model Schema (Primary DB)

- products (id, name, category, price, stock)
- orders (id, customer_id, total, status, created_at)
- order_items (id, order_id, product_id, quantity, price)
- outbox (id, topic, payload, created_at, published_at)

Also includes customers table for extensibility.

## Read Model Schema (Read DB)

- product_sales_view (product_id, total_quantity_sold, total_revenue, order_count)
- category_metrics_view (category_name, total_revenue, total_orders)
- customer_ltv_view (customer_id, total_spent, order_count, last_order_date)
- hourly_sales_view (hour_timestamp, total_orders, total_revenue)

Supporting tables:

- processed_events: idempotency deduplication
- projection_state: last processed event timestamp (for sync-status)
- product_catalog: product metadata cache in read model

## API Endpoints

### Command Service

- GET /health
- POST /api/products
- PUT /api/products/:id/price
- POST /api/orders

#### Create Product

POST /api/products

Request:

{
  "name": "T-Shirt",
  "category": "apparel",
  "price": 25.99,
  "stock": 100
}

Response (201):

{
  "productId": 1
}

#### Create Order

POST /api/orders

Request:

{
  "customerId": 42,
  "items": [
    {
      "productId": 1,
      "quantity": 2,
      "price": 25.99
    }
  ]
}

Response (201):

{
  "orderId": 1
}

### Query Service

- GET /health
- GET /api/analytics/products/:productId/sales
- GET /api/analytics/categories/:category/revenue
- GET /api/analytics/customers/:customerId/lifetime-value
- GET /api/analytics/sync-status

## Outbox and Event Reliability

The command service writes domain changes and event records to outbox inside the same transaction. This avoids dual-write inconsistencies.

Outbox publisher behavior:

1. Poll unpublished rows from outbox.
2. Publish each event to RabbitMQ exchange using topic routing key.
3. Mark row as published (published_at set).

## Idempotent Consumer Strategy

Before applying a projection update, consumer-service inserts event_id into processed_events using ON CONFLICT DO NOTHING.

- If insert succeeds: process event and update projections.
- If insert conflicts: event was already processed; skip safely.

## Setup and Run

### Prerequisites

- Docker
- Docker Compose

### Start all services

1. Copy environment template:

   cp .env.example .env

2. Start the stack:

   docker-compose up --build

3. Check health:

   - Command service: http://localhost:8080/health
   - Query service: http://localhost:8081/health
   - RabbitMQ UI: http://localhost:15672 (guest/guest)

## Verification Walkthrough

1. Create product with POST /api/products.
2. Create one or more orders with POST /api/orders.
3. Wait a few seconds for async event processing.
4. Query analytics endpoints in query-service.
5. Check /api/analytics/sync-status for projection progress.

## Notes on Eventual Consistency

Read models are eventually consistent by design. Immediately after a write, query projections may lag briefly until the event is processed.

## Testing

A test guide is available in tests/TESTING.md with step-by-step API verification focused on eventual consistency behavior.
