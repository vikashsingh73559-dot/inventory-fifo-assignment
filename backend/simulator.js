const { producer, connectKafka, TOPIC } = require('./kafka');

const productIds = ['PRD001', 'PRD002', 'PRD003'];

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomPrice() {
  return Number((randomInt(80, 180) + Math.random()).toFixed(2));
}

function buildEvents() {
  const now = Date.now();

  return [
    { product_id: 'PRD001', event_type: 'purchase', quantity: 50, unit_price: 100, timestamp: new Date(now).toISOString() },
    { product_id: 'PRD001', event_type: 'purchase', quantity: 30, unit_price: 120, timestamp: new Date(now + 1000).toISOString() },
    { product_id: 'PRD001', event_type: 'sale', quantity: 60, timestamp: new Date(now + 2000).toISOString() },
    { product_id: 'PRD002', event_type: 'purchase', quantity: 40, unit_price: 90, timestamp: new Date(now + 3000).toISOString() },
    { product_id: 'PRD002', event_type: 'sale', quantity: 12, timestamp: new Date(now + 4000).toISOString() },
    ...Array.from({ length: 5 }, (_, index) => {
      const isPurchase = Math.random() > 0.35;
      const event = {
        product_id: productIds[randomInt(0, productIds.length - 1)],
        event_type: isPurchase ? 'purchase' : 'sale',
        quantity: randomInt(5, 20),
        timestamp: new Date(now + (index + 5) * 1000).toISOString(),
      };

      if (isPurchase) {
        event.unit_price = randomPrice();
      }

      return event;
    }),
  ];
}

async function run() {
  await connectKafka();
  const events = buildEvents();

  for (const event of events) {
    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(event) }],
    });
    console.log('Sent event:', event);
  }

  await producer.disconnect();
  console.log(`Sent ${events.length} events to ${TOPIC}`);
}

run().catch(async (error) => {
  console.error('Simulator failed:', error);
  try {
    await producer.disconnect();
  } catch (_) {
    // Ignore disconnect errors while exiting.
  }
  process.exit(1);
});
