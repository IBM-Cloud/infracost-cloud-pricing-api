import glob from 'glob';
import fs from 'fs';
import zlib from 'zlib';
import { promisify } from 'util';
import { pipeline } from 'stream';

import { from as copyFrom } from 'pg-copy-streams';
import { PoolClient } from 'pg';
import format from 'pg-format';
import yargs from 'yargs';
import ProgressBar from 'progress';
import config from '../config';
import {
  createProductsTable,
  createProductsTableIndex,
  renameProductsTable,
} from '../db/setup';
import { setPriceUpdateFailed, setPriceUpdateSuccessful } from '../stats/stats';
import { retryDatabaseOperation } from '../db/retry';

async function run(): Promise<void> {
  const pool = await config.pg();

  const argv = await yargs
    .usage(
      'Usage: $0 --path=[ location of *.csv.gz files, default: ./data/products ]'
    )
    .options({
      path: { type: 'string', default: './data/products' },
    }).argv;

  const client = await pool.connect();
  try {
    await retryDatabaseOperation(async () => client.query('BEGIN'));

    await retryDatabaseOperation(async () =>
      client.query(format(`DROP TABLE IF EXISTS %I`, 'ProductLoad'))
    );

    await createProductsTable(client, 'ProductLoad');

    await loadFiles(argv.path, client);

    await createProductsTableIndex(client, 'ProductLoad');

    await replaceProductTable(client);

    await setPriceUpdateSuccessful(client);

    await retryDatabaseOperation(async () => client.query('COMMIT'));
  } catch (e) {
    await retryDatabaseOperation(async () => client.query('ROLLBACK'));

    await setPriceUpdateFailed(client);

    throw e;
  } finally {
    client.release();
  }
}

async function replaceProductTable(client: PoolClient) {
  await retryDatabaseOperation(async () =>
    client.query(format(`DROP TABLE IF EXISTS %I`, config.productTableName))
  );

  await renameProductsTable(client, 'ProductLoad', config.productTableName);
}

async function loadFiles(path: string, client: PoolClient): Promise<void> {
  const filenames = glob.sync(`${path}/*.csv.gz`);
  if (filenames.length === 0) {
    config.logger.error(
      `Could not load prices: There are no data files at '${path}/*.csv.gz'`
    );
    config.logger.error(
      `The latest data files can be downloaded with "npm run-scripts data:download"`
    );
    process.exit(1);
  }

  for (const filename of filenames) {
    config.logger.info(`Loading file: ${filename}`);
    await loadFile(client, filename);
  }
}

async function loadFile(client: PoolClient, filename: string): Promise<void> {
  const promisifiedPipeline = promisify(pipeline);

  const gunzip = zlib.createGunzip().on('error', (e) => {
    config.logger.error(
      `There was an error decompressing the file: ${e.message}`
    );
    config.logger.error(
      `The latest data files can be downloaded with "npm run-scripts data:download"`
    );
    process.exit(1);
  });

  const pgCopy = client.query(
    copyFrom(`
    COPY "ProductLoad" FROM STDIN WITH (
      FORMAT csv,
      HEADER true,
      DELIMITER ',',
      FORCE_NOT_NULL ("productFamily")
    )`)
  );

  const { size } = fs.statSync(filename);
  const progressBar = new ProgressBar(
    '-> loading [:bar] :percent (:etas remaining)',
    {
      width: 40,
      complete: '=',
      incomplete: ' ',
      renderThrottle: 500,
      total: size,
    }
  );

  const readStream = fs.createReadStream(filename);
  readStream.on('data', (buffer) => progressBar.tick(buffer.length));

  return promisifiedPipeline(readStream, gunzip, pgCopy);
}

config.logger.info('Starting: loading data into DB');
run()
  .then(() => {
    config.logger.info('Completed: loading data into DB');
    process.exit(0);
  })
  .catch((err) => {
    config.logger.error(err);
    process.exit(1);
  });
