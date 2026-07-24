const pool = require('./db');

async function createTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100)
      );
    `);
    await pool.query(`ALTER TABLE products ALTER COLUMN name DROP NOT NULL;`);
    console.log('Products table ready');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS inventory_batches (
        id SERIAL PRIMARY KEY,
        product_id VARCHAR(50) REFERENCES products(product_id),
        quantity INT NOT NULL,
        remaining_quantity INT NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('Inventory_batches table ready');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sales (
        id SERIAL PRIMARY KEY,
        product_id VARCHAR(50) REFERENCES products(product_id),
        quantity_sold INT NOT NULL,
        total_cost DECIMAL(12, 2) NOT NULL,
        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('Sales table ready');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sale_consumptions (
        id SERIAL PRIMARY KEY,
        sale_id INT NOT NULL REFERENCES sales(id) ON DELETE CASCADE,
        batch_id INT NOT NULL REFERENCES inventory_batches(id),
        quantity INT NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        cost DECIMAL(12, 2) NOT NULL
      );
    `);
    console.log('Sale_consumptions table ready');

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_inventory_batches_fifo
      ON inventory_batches (product_id, timestamp, id)
      WHERE remaining_quantity > 0;
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sale_consumptions_sale_id
      ON sale_consumptions (sale_id);
    `);

    console.log('All tables successfully created/updated');
    process.exit();
  } catch (error) {
    console.error('Error while creating tables:', error);
    process.exit(1);
  }
}

createTables();
