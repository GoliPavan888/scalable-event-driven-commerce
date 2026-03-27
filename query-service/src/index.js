const express = require('express');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 8081);
const READ_DATABASE_URL = process.env.READ_DATABASE_URL;

if (!READ_DATABASE_URL) {
  throw new Error('READ_DATABASE_URL is required');
}

const pool = new Pool({ connectionString: READ_DATABASE_URL });

const app = express();
app.use(express.json());

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.status(200).json({ status: 'ok' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/api/analytics/products/:productId/sales', async (req, res) => {
  const productId = Number(req.params.productId);

  if (!Number.isInteger(productId) || productId <= 0) {
    return res.status(400).json({ error: 'Invalid productId' });
  }

  try {
    const result = await pool.query(
      `SELECT product_id, total_quantity_sold, total_revenue, order_count
       FROM product_sales_view
       WHERE product_id = $1`,
      [productId]
    );

    if (!result.rowCount) {
      return res.status(200).json({
        productId,
        totalQuantitySold: 0,
        totalRevenue: 0,
        orderCount: 0
      });
    }

    const row = result.rows[0];
    return res.status(200).json({
      productId: row.product_id,
      totalQuantitySold: Number(row.total_quantity_sold),
      totalRevenue: Number(row.total_revenue),
      orderCount: Number(row.order_count)
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/analytics/categories/:category/revenue', async (req, res) => {
  const category = req.params.category;

  try {
    const result = await pool.query(
      `SELECT category_name, total_revenue, total_orders
       FROM category_metrics_view
       WHERE category_name = $1`,
      [category]
    );

    if (!result.rowCount) {
      return res.status(200).json({ category, totalRevenue: 0, totalOrders: 0 });
    }

    const row = result.rows[0];
    return res.status(200).json({
      category: row.category_name,
      totalRevenue: Number(row.total_revenue),
      totalOrders: Number(row.total_orders)
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/analytics/customers/:customerId/lifetime-value', async (req, res) => {
  const customerId = Number(req.params.customerId);

  if (!Number.isInteger(customerId) || customerId <= 0) {
    return res.status(400).json({ error: 'Invalid customerId' });
  }

  try {
    const result = await pool.query(
      `SELECT customer_id, total_spent, order_count, last_order_date
       FROM customer_ltv_view
       WHERE customer_id = $1`,
      [customerId]
    );

    if (!result.rowCount) {
      return res.status(200).json({
        customerId,
        totalSpent: 0,
        orderCount: 0,
        lastOrderDate: null
      });
    }

    const row = result.rows[0];
    return res.status(200).json({
      customerId: row.customer_id,
      totalSpent: Number(row.total_spent),
      orderCount: Number(row.order_count),
      lastOrderDate: row.last_order_date ? new Date(row.last_order_date).toISOString() : null
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/analytics/sync-status', async (_req, res) => {
  try {
    const result = await pool.query(
      `SELECT last_processed_event_timestamp
       FROM projection_state
       WHERE singleton = TRUE`
    );

    const lastProcessedEventTimestamp = result.rowCount
      ? result.rows[0].last_processed_event_timestamp
      : null;

    const nowMs = Date.now();
    const lagSeconds = lastProcessedEventTimestamp
      ? Math.max(0, Math.floor((nowMs - new Date(lastProcessedEventTimestamp).getTime()) / 1000))
      : 0;

    return res.status(200).json({
      lastProcessedEventTimestamp: lastProcessedEventTimestamp
        ? new Date(lastProcessedEventTimestamp).toISOString()
        : null,
      lagSeconds
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Query service listening on ${PORT}`);
});
