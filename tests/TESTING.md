# Manual Test Plan

## 1. Start services

docker-compose up --build

## 2. Create a product

curl -X POST http://localhost:8080/api/products \
  -H "Content-Type: application/json" \
  -d '{"name":"Keyboard","category":"electronics","price":50,"stock":100}'

Expected: 201 with productId.

## 3. Create orders

curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":101,"items":[{"productId":1,"quantity":2,"price":50}]}'

curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":101,"items":[{"productId":1,"quantity":1,"price":50}]}'

Expected: 201 with orderId.

## 4. Wait for projections

sleep 10

## 5. Verify product analytics

curl http://localhost:8081/api/analytics/products/1/sales

Expected values:
- totalQuantitySold: 3
- totalRevenue: 150
- orderCount: 2

## 6. Verify category analytics

curl http://localhost:8081/api/analytics/categories/electronics/revenue

Expected values:
- totalRevenue: 150
- totalOrders: 2

## 7. Verify customer LTV

curl http://localhost:8081/api/analytics/customers/101/lifetime-value

Expected values:
- totalSpent: 150
- orderCount: 2
- lastOrderDate: non-null ISO string

## 8. Verify sync status

curl http://localhost:8081/api/analytics/sync-status

Expected keys:
- lastProcessedEventTimestamp
- lagSeconds
