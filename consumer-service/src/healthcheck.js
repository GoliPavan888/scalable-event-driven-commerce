const amqp = require('amqplib');
const { Pool } = require('pg');

const BROKER_URL = process.env.BROKER_URL;
const READ_DATABASE_URL = process.env.READ_DATABASE_URL;
const BROKER_EXCHANGE = process.env.BROKER_EXCHANGE || 'commerce.events';

async function run() {
  if (!BROKER_URL || !READ_DATABASE_URL) {
    throw new Error('BROKER_URL and READ_DATABASE_URL are required');
  }

  const pool = new Pool({ connectionString: READ_DATABASE_URL });
  let conn;
  let channel;

  try {
    await pool.query('SELECT 1');

    conn = await amqp.connect(BROKER_URL);
    channel = await conn.createChannel();
    await channel.assertExchange(BROKER_EXCHANGE, 'topic', { durable: true });

    process.exit(0);
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  } finally {
    if (channel) {
      await channel.close().catch(() => {});
    }
    if (conn) {
      await conn.close().catch(() => {});
    }
    await pool.end().catch(() => {});
  }
}

run();
