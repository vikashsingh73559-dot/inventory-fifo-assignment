# Inventory Management System (FIFO)

Real-time inventory dashboard for a small trading business using FIFO costing, PostgreSQL, Kafka, Express, and React.

## Login

- User ID: `admin`
- Password: `admin123`

## FIFO Logic

- Purchase events create inventory batches with `quantity`, `remaining_quantity`, `unit_price`, and `timestamp`.
- Sale events consume the oldest available batches first using `ORDER BY timestamp ASC, id ASC`.
- Each sale is processed inside a PostgreSQL transaction.
- The total FIFO cost is stored in `sales`.
- The exact batch split for every sale is stored in `sale_consumptions`.

Example:

- Purchase 50 units at 100
- Purchase 30 units at 120
- Sale 60 units
- FIFO cost = `50 * 100 + 10 * 120 = 6200`

## Backend

```bash
cd backend
npm install
npm run setup
npm start
```

Required backend environment variables:

```env
PORT=5000
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DB
KAFKA_BROKER=broker-host:9092
KAFKA_USERNAME=username
KAFKA_PASSWORD=password
KAFKA_TOPIC=inventory-events
```

## Kafka Simulator

Run this after backend env variables are configured:

```bash
cd backend
npm run simulate
```

The simulator sends 10 purchase/sale events to the `inventory-events` Kafka topic.

## Frontend

```bash
cd frontend
npm install
npm run dev
```

For deployed frontend, set:

```env
VITE_API_BASE_URL=https://your-backend-url
```

## API Endpoints

- `GET /api/health`
- `POST /api/inventory`
- `GET /api/stock`
- `GET /api/ledger`
- `GET /api/sales/:id/consumptions`

## Deployment Links

- Frontend: https://inventory-fifo-assignment.vercel.app
- Backend API: https://inventory-fifo-assignment.onrender.com
- GitHub Repository: https://github.com/vikashsingh73559-dot/inventory-fifo-assignment
