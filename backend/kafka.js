const { Kafka } = require('kafkajs');
const pool = require('./db');
require('dotenv').config();

const TOPIC = process.env.KAFKA_TOPIC || 'inventory-events';

const kafkaOptions = {
  clientId: 'inventory-app',
  brokers: (process.env.KAFKA_BROKER || 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean),
  connectionTimeout: 10000,
};

if (process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD) {
  kafkaOptions.ssl = {
    rejectUnauthorized: process.env.KAFKA_SSL_REJECT_UNAUTHORIZED === 'true',
  };

  if (process.env.KAFKA_CA_CERT) {
    kafkaOptions.ssl = {
      rejectUnauthorized: true,
      ca: [process.env.KAFKA_CA_CERT.replace(/\\n/g, '\n')],
    };
  }

  kafkaOptions.sasl = {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  };
}

const kafka = new Kafka(kafkaOptions);
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'inventory-group' });

function validateEvent(data) {
  if (!data || !data.product_id || !data.event_type || !data.quantity || !data.timestamp) {
    throw new Error('Invalid event: product_id, event_type, quantity, and timestamp are required');
  }

  if (!['purchase', 'sale'].includes(data.event_type)) {
    throw new Error(`Invalid event_type: ${data.event_type}`);
  }

  if (!Number.isFinite(Number(data.quantity)) || Number(data.quantity) <= 0) {
    throw new Error('Invalid quantity: quantity must be greater than 0');
  }

  if (data.event_type === 'purchase' && (!Number.isFinite(Number(data.unit_price)) || Number(data.unit_price) <= 0)) {
    throw new Error('Invalid unit_price: purchase events need unit_price greater than 0');
  }
}

async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const topics = await admin.listTopics();
    if (!topics.includes(TOPIC)) {
      await admin.createTopics({
        topics: [{ topic: TOPIC, numPartitions: 1 }],
        waitForLeaders: true,
      });
    }
  } finally {
    await admin.disconnect();
  }
}

async function handleInventoryEvent(data) {
  validateEvent(data);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query(
      `
        INSERT INTO products (product_id, name)
        VALUES ($1, $1)
        ON CONFLICT (product_id) DO NOTHING
      `,
      [data.product_id]
    );

    if (data.event_type === 'purchase') {
      await client.query(
        `
          INSERT INTO inventory_batches (product_id, quantity, remaining_quantity, unit_price, timestamp)
          VALUES ($1, $2, $2, $3, $4)
        `,
        [data.product_id, Number(data.quantity), Number(data.unit_price), data.timestamp]
      );
      await client.query('COMMIT');
      console.log(`Purchase saved: ${data.product_id}, qty ${data.quantity}`);
      return;
    }

    let quantityToSell = Number(data.quantity);
    let totalCostCalculated = 0;
    const consumptions = [];

    const { rows: batches } = await client.query(
      `
        SELECT id, remaining_quantity, unit_price
        FROM inventory_batches
        WHERE product_id = $1 AND remaining_quantity > 0
        ORDER BY timestamp ASC, id ASC
        FOR UPDATE
      `,
      [data.product_id]
    );

    for (const batch of batches) {
      if (quantityToSell <= 0) break;

      const qtyTakenFromBatch = Math.min(quantityToSell, Number(batch.remaining_quantity));
      const unitPrice = Number(batch.unit_price);
      const cost = qtyTakenFromBatch * unitPrice;

      consumptions.push({
        batchId: batch.id,
        quantity: qtyTakenFromBatch,
        unitPrice,
        cost,
      });

      totalCostCalculated += cost;
      quantityToSell -= qtyTakenFromBatch;
    }

    if (quantityToSell > 0) {
      throw new Error(`Insufficient stock for ${data.product_id}. Missing quantity: ${quantityToSell}`);
    }

    const saleResult = await client.query(
      `
        INSERT INTO sales (product_id, quantity_sold, total_cost, timestamp)
        VALUES ($1, $2, $3, $4)
        RETURNING id
      `,
      [data.product_id, Number(data.quantity), totalCostCalculated, data.timestamp]
    );

    const saleId = saleResult.rows[0].id;

    for (const item of consumptions) {
      await client.query(
        `
          UPDATE inventory_batches
          SET remaining_quantity = remaining_quantity - $1
          WHERE id = $2
        `,
        [item.quantity, item.batchId]
      );

      await client.query(
        `
          INSERT INTO sale_consumptions (sale_id, batch_id, quantity, unit_price, cost)
          VALUES ($1, $2, $3, $4, $5)
        `,
        [saleId, item.batchId, item.quantity, item.unitPrice, item.cost]
      );
    }

    await client.query('COMMIT');
    console.log(`Sale saved: ${data.product_id}, qty ${data.quantity}, FIFO cost ${totalCostCalculated}`);
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

const connectKafka = async () => {
  try {
    await ensureTopic();
    await producer.connect();
    console.log(`Kafka producer connected on topic ${TOPIC}`);
  } catch (error) {
    console.error('Kafka producer/topic error:', error);
    throw error;
  }
};

const connectConsumer = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const data = JSON.parse(message.value.toString());
          console.log('Kafka event received:', data);
          await handleInventoryEvent(data);
        } catch (error) {
          console.error('Inventory event failed:', error.message);
        }
      },
    });
  } catch (error) {
    console.error('Consumer error:', error);
    throw error;
  }
};

module.exports = {
  TOPIC,
  producer,
  connectKafka,
  connectConsumer,
  handleInventoryEvent,
};
