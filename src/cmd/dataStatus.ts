import { PoolClient } from 'pg';
import format from 'pg-format';
import config from '../config';
import { retryDatabaseOperation } from '../db/retry';

const attempts = 10;
const backOffSecs = 10;

async function run(): Promise<void> {
  const pool = await config.pg();

  let client: PoolClient | null = null;

  for (let i = 0; i < attempts; i++) {
    try {
      client = await pool.connect();
      break;
    } catch (e: unknown) {
      const error = e as Error;
      config.logger.error(
        `Waiting for PostgreSQL to become available: ${error.message}`
      );
      await new Promise((resolve) => {
        setTimeout(resolve, backOffSecs * 1000);
      });
    }
  }

  if (client === null) {
    throw new Error('Failed to connect to PostgreSQL');
  }

  const connectedClient = client;
  
  try {
    const counts = await retryDatabaseOperation(async () =>
      connectedClient.query(
        format(
          `SELECT "vendorName", count(*) as "productCount" FROM %I GROUP BY "vendorName"`,
          config.productTableName
        )
      )
    );
    config.logger.info(`\n${JSON.stringify(counts.rows, null, 2)}`);
  } finally {
    connectedClient.release();
  }
}

config.logger.info('Starting: Data status');
run()
  .then(() => {
    config.logger.info('Completed: Data status');
    process.exit(0);
  })
  .catch((err) => {
    config.logger.error(err);
    process.exit(1);
  });
