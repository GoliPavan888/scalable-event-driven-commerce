const amqp = require('amqplib');
const { Pool } = require('pg');

const BROKER_URL = process.env.BROKER_URL;
const READ_DATABASE_URL = process.env.READ_DATABASE_URL;
const BROKER_EXCHANGE = process.env.BROKER_EXCHANGE || 'commerce.events';
const QUEUE_NAME = process.env.QUEUE_NAME || 'analytics-projection-queue';
const PREFETCH_COUNT = Number(process.env.PREFETCH_COUNT || 20);

if (!BROKER_URL || !READ_DATABASE_URL) {
  throw new Error('BROKER_URL and READ_DATABASE_URL are required');
}

const pool = new Pool({ connectionString: READ_DATABASE_URL });

function toHourStartIso(dateLike) {
  const d = new Date(dateLike);
  d.setUTCMinutes(0, 0, 0);
  return d.toISOString();
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

async function processOrderCreated(client, event) {
  const data = event.data || {};
  const items = Array.isArray(data.items) ? data.items : [];
  const customerId = Number(data.customerId);
  const total = Number(data.total || 0);
  const eventTime = data.timestamp || event.occurredAt || new Date().toISOString();
  const hourStart = toHourStartIso(eventTime);

  const productMap = new Map();
  const categoryMap = new Map();

  for (const item of items) {
    const productId = Number(item.productId);
    const quantity = Number(item.quantity || 0);
    const lineTotal = Number(item.lineTotal || (Number(item.price || 0) * quantity));
    const category = String(item.category || 'uncategorized');

    const productAgg = productMap.get(productId) || { quantity: 0, revenue: 0 };
    productAgg.quantity += quantity;
    productAgg.revenue += lineTotal;
    productMap.set(productId, productAgg);

    const categoryAgg = categoryMap.get(category) || { revenue: 0 };
    categoryAgg.revenue += lineTotal;
    categoryMap.set(category, categoryAgg);

    if (item.productName) {
      await client.query(
        `INSERT INTO product_catalog (product_id, name, category, current_price, updated_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (product_id)
         DO UPDATE SET
           name = EXCLUDED.name,
           category = EXCLUDED.category,
           current_price = EXCLUDED.current_price,
           updated_at = NOW()`,
        [productId, item.productName, category, Number(item.price || 0)]
      );
    }
  }

  for (const [productId, agg] of productMap.entries()) {
    await client.query(
      `INSERT INTO product_sales_view (product_id, total_quantity_sold, total_revenue, order_count)
       VALUES ($1, $2, $3, 1)
       ON CONFLICT (product_id)
       DO UPDATE SET
         total_quantity_sold = product_sales_view.total_quantity_sold + EXCLUDED.total_quantity_sold,
         total_revenue = product_sales_view.total_revenue + EXCLUDED.total_revenue,
         order_count = product_sales_view.order_count + 1`,
      [productId, agg.quantity, agg.revenue]
    );
  }

  for (const [category, agg] of categoryMap.entries()) {
    await client.query(
      `INSERT INTO category_metrics_view (category_name, total_revenue, total_orders)
       VALUES ($1, $2, 1)
       ON CONFLICT (category_name)
       DO UPDATE SET
         total_revenue = category_metrics_view.total_revenue + EXCLUDED.total_revenue,
         total_orders = category_metrics_view.total_orders + 1`,
      [category, agg.revenue]
    );
  }

  await client.query(
    `INSERT INTO customer_ltv_view (customer_id, total_spent, order_count, last_order_date)
     VALUES ($1, $2, 1, $3)
     ON CONFLICT (customer_id)
     DO UPDATE SET
       total_spent = customer_ltv_view.total_spent + EXCLUDED.total_spent,
       order_count = customer_ltv_view.order_count + 1,
       last_order_date = GREATEST(customer_ltv_view.last_order_date, EXCLUDED.last_order_date)`,
    [customerId, total, eventTime]
  );

  await client.query(
    `INSERT INTO hourly_sales_view (hour_timestamp, total_orders, total_revenue)
     VALUES ($1, 1, $2)
     ON CONFLICT (hour_timestamp)
     DO UPDATE SET
       total_orders = hourly_sales_view.total_orders + 1,
       total_revenue = hourly_sales_view.total_revenue + EXCLUDED.total_revenue`,
    [hourStart, total]
  );
}

async function processProductCreated(client, event) {
  const data = event.data || {};
  await client.query(
    `INSERT INTO product_catalog (product_id, name, category, current_price, updated_at)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (product_id)
     DO UPDATE SET
       name = EXCLUDED.name,
       category = EXCLUDED.category,
       current_price = EXCLUDED.current_price,
       updated_at = NOW()`,
    [Number(data.productId), String(data.name || ''), String(data.category || 'uncategorized'), Number(data.price || 0)]
  );
}

async function processPriceChanged(client, event) {
  const data = event.data || {};
  await client.query(
    `INSERT INTO product_catalog (product_id, name, category, current_price, updated_at)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (product_id)
     DO UPDATE SET
       current_price = EXCLUDED.current_price,
       category = COALESCE(NULLIF(EXCLUDED.category, ''), product_catalog.category),
       name = COALESCE(NULLIF(EXCLUDED.name, ''), product_catalog.name),
       updated_at = NOW()`,
    [
      Number(data.productId),
      String(data.name || ''),
      String(data.category || ''),
      Number(data.newPrice || 0)
    ]
  );
}

async function processEvent(event, fallbackEventId) {
  const eventId = String(event.eventId || fallbackEventId || '');
  const eventType = String(event.eventType || 'UnknownEvent');
  const eventTime = event.occurredAt || new Date().toISOString();

  if (!eventId) {
    throw new Error('Event missing eventId');
  }

  await withTransaction(async (client) => {
    const idempotency = await client.query(
      `INSERT INTO processed_events (event_id)
       VALUES ($1)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [eventId]
    );

    if (!idempotency.rowCount) {
      return;
    }

    if (eventType === 'OrderCreated') {
      await processOrderCreated(client, event);
    } else if (eventType === 'ProductCreated') {
      await processProductCreated(client, event);
    } else if (eventType === 'PriceChanged') {
      await processPriceChanged(client, event);
    }

    await client.query(
      `UPDATE projection_state
       SET last_processed_event_timestamp = CASE
         WHEN last_processed_event_timestamp IS NULL THEN $1::timestamptz
         ELSE GREATEST(last_processed_event_timestamp, $1::timestamptz)
       END
       WHERE singleton = TRUE`,
      [eventTime]
    );
  });
}

async function startConsumer() {
  const connection = await amqp.connect(BROKER_URL);
  const channel = await connection.createChannel();

  await channel.assertExchange(BROKER_EXCHANGE, 'topic', { durable: true });
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  await channel.bindQueue(QUEUE_NAME, BROKER_EXCHANGE, 'order-events');
  await channel.bindQueue(QUEUE_NAME, BROKER_EXCHANGE, 'product-events');

  await channel.prefetch(PREFETCH_COUNT);

  console.log('Consumer started');

  channel.consume(
    QUEUE_NAME,
    async (msg) => {
      if (!msg) {
        return;
      }

      try {
        const body = JSON.parse(msg.content.toString());
        const fallbackEventId = msg.properties.messageId || null;
        await processEvent(body, fallbackEventId);
        channel.ack(msg);
      } catch (error) {
        console.error('Consumer processing error:', error.message);
        channel.nack(msg, false, true);
      }
    },
    { noAck: false }
  );

  connection.on('close', () => {
    console.error('Broker connection closed. Restarting consumer in 5 seconds.');
    setTimeout(() => {
      startConsumer().catch((err) => {
        console.error('Consumer restart failed:', err.message);
      });
    }, 5000);
  });

  connection.on('error', (error) => {
    console.error('Broker connection error:', error.message);
  });
}

startConsumer().catch((error) => {
  console.error('Consumer startup failed:', error);
  process.exit(1);
});
