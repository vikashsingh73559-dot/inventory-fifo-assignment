const express = require('express');
const cors = require('cors');
require('dotenv').config();
const { connectKafka, connectConsumer, producer, TOPIC } = require('./kafka');
const pool = require('./db');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 5000;

app.get('/api/health', (req, res) => {
  res.json({ ok: true, service: 'inventory-fifo-api' });
});

app.post('/api/inventory', async (req, res) => {
  try {
    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(req.body) }],
    });
    res.status(202).json({ message: 'Event sent to Kafka', topic: TOPIC, event: req.body });
  } catch (error) {
    console.error('Kafka send error:', error);
    res.status(500).json({ error: 'Failed to send inventory event' });
  }
});

app.get('/api/stock', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        product_id,
        SUM(remaining_quantity)::INT AS current_quantity,
        SUM(remaining_quantity * unit_price)::NUMERIC(12, 2) AS total_cost,
        CASE
          WHEN SUM(remaining_quantity) > 0
          THEN (SUM(remaining_quantity * unit_price) / SUM(remaining_quantity))::NUMERIC(12, 2)
          ELSE 0
        END AS average_cost
      FROM inventory_batches
      WHERE remaining_quantity > 0
      GROUP BY product_id
      ORDER BY product_id
    `);
    res.json(result.rows);
  } catch (error) {
    console.error('Stock fetch error:', error);
    res.status(500).json({ error: 'Failed to fetch stock' });
  }
});

app.get('/api/ledger', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT *
      FROM (
        SELECT
          'purchase' AS event_type,
          NULL::INT AS sale_id,
          product_id,
          quantity,
          unit_price,
          (quantity * unit_price)::NUMERIC(12, 2) AS total_cost,
          timestamp,
          NULL::JSON AS fifo_breakdown
        FROM inventory_batches

        UNION ALL

        SELECT
          'sale' AS event_type,
          s.id AS sale_id,
          s.product_id,
          s.quantity_sold AS quantity,
          NULL::NUMERIC AS unit_price,
          s.total_cost,
          s.timestamp,
          COALESCE(
            json_agg(
              json_build_object(
                'batch_id', sc.batch_id,
                'quantity', sc.quantity,
                'unit_price', sc.unit_price,
                'cost', sc.cost
              )
              ORDER BY sc.id
            ) FILTER (WHERE sc.id IS NOT NULL),
            '[]'::JSON
          ) AS fifo_breakdown
        FROM sales s
        LEFT JOIN sale_consumptions sc ON sc.sale_id = s.id
        GROUP BY s.id
      ) ledger
      ORDER BY timestamp DESC, sale_id DESC NULLS LAST
    `);
    res.json(result.rows);
  } catch (error) {
    console.error('Ledger fetch error:', error);
    res.status(500).json({ error: 'Failed to fetch ledger' });
  }
});

app.get('/api/sales/:id/consumptions', async (req, res) => {
  try {
    const result = await pool.query(
      `
        SELECT sale_id, batch_id, quantity, unit_price, cost
        FROM sale_consumptions
        WHERE sale_id = $1
        ORDER BY id
      `,
      [req.params.id]
    );
    res.json(result.rows);
  } catch (error) {
    console.error('Sale consumption fetch error:', error);
    res.status(500).json({ error: 'Failed to fetch sale FIFO breakdown' });
  }
});

app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT} 🚀`);
  await connectKafka();
  await connectConsumer();
});