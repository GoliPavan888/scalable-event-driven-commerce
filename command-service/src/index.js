const express = require('express');
const { randomUUID } = require('crypto');
const amqp = require('amqplib');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 8080);
const DATABASE_URL = process.env.DATABASE_URL;
const BROKER_URL = process.env.BROKER_URL;
const BROKER_EXCHANGE = process.env.BROKER_EXCHANGE || 'commerce.events';
const OUTBOX_POLL_INTERVAL_MS = Number(process.env.OUTBOX_POLL_INTERVAL_MS || 2000);

if (!DATABASE_URL || !BROKER_URL) {
  throw new Error('DATABASE_URL and BROKER_URL are required');
}

const pool = new Pool({ connectionString: DATABASE_URL });

let brokerConnection = null;
let brokerChannel = null;
let publisherTimer = null;

async function ensureBrokerChannel() {
  if (brokerChannel) {
    return brokerChannel;
  }

  if (!brokerConnection) {
    brokerConnection = await amqp.connect(BROKER_URL);
    brokerConnection.on('error', () => {
      brokerConnection = null;
      brokerChannel = null;
    });
    brokerConnection.on('close', () => {
      brokerConnection = null;
      brokerChannel = null;
    });
  }

  brokerChannel = await brokerConnection.createChannel();
  await brokerChannel.assertExchange(BROKER_EXCHANGE, 'topic', { durable: true });
  return brokerChannel;
}

async function publishOutboxBatch() {
  let client;
  try {
    const channel = await ensureBrokerChannel();
    client = await pool.connect();

    const pending = await client.query(
      `SELECT id, topic, payload
       FROM outbox
       WHERE published_at IS NULL
       ORDER BY id
       LIMIT 100`
    );

    for (const row of pending.rows) {
      const payloadString = JSON.stringify(row.payload);
      channel.publish(BROKER_EXCHANGE, row.topic, Buffer.from(payloadString), {
        persistent: true,
        contentType: 'application/json',
        messageId: String(row.id)
      });

      await client.query(
        'UPDATE outbox SET published_at = NOW() WHERE id = $1 AND published_at IS NULL',
        [row.id]
      );
    }
  } catch (error) {
    console.error('Outbox publish error:', error.message);
  } finally {
    if (client) {
      client.release();
    }
  }
}

function buildEvent(eventType, data, occurredAt) {
  return {
    eventId: randomUUID(),
    eventType,
    occurredAt: occurredAt || new Date().toISOString(),
    data
  };
}

async function insertOutboxEvent(client, topic, payload) {
  await client.query(
    `INSERT INTO outbox (topic, payload)
     VALUES ($1, $2::jsonb)`,
    [topic, JSON.stringify(payload)]
  );
}

async function withTransaction(work) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

const app = express();
app.use(express.json());

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    const brokerHealthy = Boolean(brokerConnection && brokerChannel);
    res.status(200).json({ status: 'ok', brokerConnected: brokerHealthy });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/api/products', async (req, res) => {
  const { name, category, price, stock } = req.body || {};

  if (!name || !category || typeof price !== 'number' || !Number.isInteger(stock) || stock < 0 || price < 0) {
    return res.status(400).json({ error: 'Invalid product payload' });
  }

  try {
    const response = await withTransaction(async (client) => {
      const inserted = await client.query(
        `INSERT INTO products (name, category, price, stock)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
        [name, category, price, stock]
      );

      const productId = inserted.rows[0].id;

      const event = buildEvent('ProductCreated', {
        productId,
        name,
        category,
        price,
        stock
      });

      await insertOutboxEvent(client, 'product-events', event);

      return { productId };
    });

    return res.status(201).json(response);
  } catch (error) {
    console.error('Create product failed:', error.message);
    return res.status(500).json({ error: 'Failed to create product' });
  }
});

app.put('/api/products/:id/price', async (req, res) => {
  const productId = Number(req.params.id);
  const { price } = req.body || {};

  if (!Number.isInteger(productId) || productId <= 0 || typeof price !== 'number' || price < 0) {
    return res.status(400).json({ error: 'Invalid request payload' });
  }

  try {
    const payload = await withTransaction(async (client) => {
      const updated = await client.query(
        `UPDATE products
         SET price = $1, updated_at = NOW()
         WHERE id = $2
         RETURNING id, name, category, price`,
        [price, productId]
      );

      if (!updated.rowCount) {
        return null;
      }

      const product = updated.rows[0];

      const event = buildEvent('PriceChanged', {
        productId: product.id,
        name: product.name,
        category: product.category,
        newPrice: Number(product.price)
      });

      await insertOutboxEvent(client, 'product-events', event);
      return { productId: product.id, price: Number(product.price) };
    });

    if (!payload) {
      return res.status(404).json({ error: 'Product not found' });
    }

    return res.status(200).json(payload);
  } catch (error) {
    console.error('Price update failed:', error.message);
    return res.status(500).json({ error: 'Failed to update product price' });
  }
});

app.post('/api/orders', async (req, res) => {
  const { customerId, items } = req.body || {};

  if (!Number.isInteger(customerId) || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'Invalid order payload' });
  }

  for (const item of items) {
    if (!Number.isInteger(item.productId) || !Number.isInteger(item.quantity) || item.quantity <= 0 || typeof item.price !== 'number' || item.price < 0) {
      return res.status(400).json({ error: 'Invalid order item payload' });
    }
  }

  try {
    const result = await withTransaction(async (client) => {
      const productIds = [...new Set(items.map((item) => item.productId))];
      const productsResult = await client.query(
        `SELECT id, name, category, stock
         FROM products
         WHERE id = ANY($1::int[])
         FOR UPDATE`,
        [productIds]
      );

      if (productsResult.rowCount !== productIds.length) {
        throw new Error('One or more products not found');
      }

      const productsById = new Map(productsResult.rows.map((p) => [p.id, p]));
      const requestedStock = new Map();
      let total = 0;

      for (const item of items) {
        const product = productsById.get(item.productId);
        const alreadyRequested = requestedStock.get(item.productId) || 0;
        const nextRequested = alreadyRequested + item.quantity;

        if (nextRequested > product.stock) {
          throw new Error(`Insufficient stock for product ${item.productId}`);
        }

        requestedStock.set(item.productId, nextRequested);
        total += item.quantity * item.price;
      }

      const orderInsert = await client.query(
        `INSERT INTO orders (customer_id, total, status)
         VALUES ($1, $2, 'CREATED')
         RETURNING id, created_at`,
        [customerId, total]
      );

      const orderId = orderInsert.rows[0].id;
      const createdAt = orderInsert.rows[0].created_at.toISOString();

      for (const item of items) {
        await client.query(
          `INSERT INTO order_items (order_id, product_id, quantity, price)
           VALUES ($1, $2, $3, $4)`,
          [orderId, item.productId, item.quantity, item.price]
        );
      }

      for (const [productId, quantity] of requestedStock.entries()) {
        await client.query(
          `UPDATE products
           SET stock = stock - $1, updated_at = NOW()
           WHERE id = $2`,
          [quantity, productId]
        );
      }

      const eventItems = items.map((item) => {
        const product = productsById.get(item.productId);
        return {
          productId: item.productId,
          productName: product.name,
          category: product.category,
          quantity: item.quantity,
          price: item.price,
          lineTotal: item.quantity * item.price
        };
      });

      const event = buildEvent('OrderCreated', {
        orderId,
        customerId,
        items: eventItems,
        total,
        status: 'CREATED',
        timestamp: createdAt
      }, createdAt);

      await insertOutboxEvent(client, 'order-events', event);

      return { orderId };
    });

    return res.status(201).json(result);
  } catch (error) {
    console.error('Create order failed:', error.message);
    if (error.message.startsWith('Insufficient stock') || error.message.includes('not found')) {
      return res.status(400).json({ error: error.message });
    }
    return res.status(500).json({ error: 'Failed to create order' });
  }
});

function startOutboxPublisher() {
  publisherTimer = setInterval(() => {
    publishOutboxBatch().catch((error) => {
      console.error('Outbox loop error:', error.message);
    });
  }, OUTBOX_POLL_INTERVAL_MS);

  publishOutboxBatch().catch((error) => {
    console.error('Initial outbox publish failed:', error.message);
  });
}

async function bootstrap() {
  app.listen(PORT, () => {
    console.log(`Command service listening on ${PORT}`);
  });

  startOutboxPublisher();
}

async function shutdown() {
  if (publisherTimer) {
    clearInterval(publisherTimer);
  }
  if (brokerChannel) {
    await brokerChannel.close().catch(() => {});
  }
  if (brokerConnection) {
    await brokerConnection.close().catch(() => {});
  }
  await pool.end().catch(() => {});
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

bootstrap().catch((error) => {
  console.error('Fatal bootstrap error:', error);
  process.exit(1);
});
