import { PoolClient } from 'pg';
import config from '../config';
import {
  createInstallsTable,
  createProductsTable,
  createProductsTableIndex,
  createStatsTable,
} from '../db/setup';
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
    await retryDatabaseOperation(async () => connectedClient.query('BEGIN'));
    await createProductsTable(connectedClient, config.productTableName, true);
    await createProductsTableIndex(connectedClient, config.productTableName, true);
    await createStatsTable(connectedClient, config.statsTableName, true);
    await createInstallsTable(connectedClient, config.installsTableName, true);
    await retryDatabaseOperation(async () => connectedClient.query('COMMIT'));
  } catch (e) {
    await retryDatabaseOperation(async () => connectedClient.query('ROLLBACK'));
    throw e;
  } finally {
    connectedClient.release();
  }
}

config.logger.info('Starting: DB setup');
run()
  .then(() => {
    config.logger.info('Completed: DB setup');
    process.exit(0);
  })
  .catch((err) => {
    config.logger.error(err);
    process.exit(1);
  });
