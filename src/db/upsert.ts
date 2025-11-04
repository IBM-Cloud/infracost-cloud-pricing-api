import format from 'pg-format';
import { Product, Price } from './types';
import config from '../config';
import { retryDatabaseOperation } from './retry';

const batchSize = 1000;

async function upsertProducts(products: Product[]): Promise<void> {
  const pool = await config.pg();
  config.logger.info(`Upserting ${products.length} products`);

  const insertSql = format(
    `INSERT INTO %I ("productHash", "sku", "vendorName", "region", "service", "productFamily", "attributes", "prices") VALUES `,
    config.productTableName
  );

  const onConflictSql = format(
    ` 
    ON CONFLICT ("productHash") DO UPDATE SET
    "sku" = excluded."sku",
    "vendorName" = excluded."vendorName",
    "region" = excluded."region",
    "service" = excluded."service",
    "productFamily" = excluded."productFamily",
    "attributes" = excluded."attributes",
    "prices" = %I."prices" || excluded."prices"        
    `,
    config.productTableName
  );

  // Collect products for bulk update in a map so we can avoid updating the same product in a single batch since
  // postgres doesn't allow that.
  const productHashToInsertRow: Map<string, string> = new Map();

  for (const product of products) {
    if (
      productHashToInsertRow.size > batchSize ||
      productHashToInsertRow.has(product.productHash)
    ) {
      await retryDatabaseOperation(async () =>
        pool.query(
          insertSql +
            Array.from(productHashToInsertRow.values()).join(',') +
            onConflictSql
        )
      );
      productHashToInsertRow.clear();
    }

    // Prices are stored as { pricesHash: prices[] } so we can update/merge them using the postgres jsonb concatenation
    const pricesMap: { [priceHash: string]: Price[] } = {};
    product.prices.forEach((price) => {
      if (pricesMap[price.priceHash]) {
        pricesMap[price.priceHash].push(price);
      } else {
        pricesMap[price.priceHash] = [price];
      }
    });

    productHashToInsertRow.set(
      product.productHash,
      format(
        `(%L, %L, %L, %L, %L, %L, %L, %L)`,
        product.productHash,
        product.sku,
        product.vendorName,
        product.region,
        product.service,
        product.productFamily || '',
        product.attributes,
        pricesMap
      )
    );
  }

  if (productHashToInsertRow.size > 0) {
    await retryDatabaseOperation(async () =>
      pool.query(
        insertSql +
          Array.from(productHashToInsertRow.values()).join(',') +
          onConflictSql
      )
    );
  }
}

async function upsertPrice(product: Product, price: Price): Promise<void> {
  const pool = await config.pg();

  await retryDatabaseOperation(async () =>
    pool.query(
      format(
        `UPDATE %I SET "prices" = "prices" || %L WHERE "productHash" = %L`,
        config.productTableName,
        { [price.priceHash]: [price] },
        product.productHash
      )
    )
  );
}

export { upsertProducts, upsertPrice };
