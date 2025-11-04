import { PoolClient } from 'pg';
import format from 'pg-format';
import { retryDatabaseOperation } from './retry';

export async function createProductsTable(
  client: PoolClient,
  tableName: string,
  ifNotExists?: boolean
): Promise<void> {
  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `
      CREATE TABLE ${ifNotExists ? 'IF NOT EXISTS' : ''} %I
      (
        "productHash" text,
        sku text NOT NULL,
        "vendorName" text NOT NULL,
        region text,
        service text NOT NULL,
        "productFamily" text DEFAULT ''::text NOT NULL,
        attributes jsonb NOT NULL,
        prices jsonb NOT NULL,
        CONSTRAINT %I PRIMARY KEY("productHash")
      )
    `,
        tableName,
        `${tableName}_pkey`
      )
    )
  );
}

export async function createProductsTableIndex(
  client: PoolClient,
  tableName: string,
  ifNotExists?: boolean
): Promise<void> {
  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `CREATE INDEX ${
          ifNotExists ? 'IF NOT EXISTS' : ''
        } %I ON %I USING btree (service, region)`,
        `${tableName}_service_region_index`,
        tableName
      )
    )
  );

  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `CREATE INDEX ${
          ifNotExists ? 'IF NOT EXISTS' : ''
        } %I ON %I USING btree (service, region, "productFamily", (attributes->>'instanceType'), (attributes->>'operatingSystem'), (attributes->>'capacitystatus'), (attributes->>'preInstalledSw'))`,
        `${tableName}_ec2_instances_index`,
        tableName
      )
    )
  );
}

export async function createStatsTable(
  client: PoolClient,
  tableName: string,
  ifNotExists?: boolean
): Promise<void> {
  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `
      CREATE TABLE ${ifNotExists ? 'IF NOT EXISTS' : ''} %I
      (
        id serial PRIMARY KEY NOT NULL,
        created_at timestamp DEFAULT NOW() NOT NULL,
        updated_at timestamp NOT NULL,
        prices_last_successfully_updated_at timestamp,
        prices_last_update_successful boolean,
        total_runs bigint DEFAULT 0,
        ci_runs bigint DEFAULT 0,
        non_ci_runs bigint DEFAULT 0
      )
    `,
        tableName
      )
    )
  );

  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `
      INSERT INTO %I (id, updated_at) VALUES (1, NOW())
      ON CONFLICT DO NOTHING
    `,
        tableName
      )
    )
  );
}

export async function createInstallsTable(
  client: PoolClient,
  tableName: string,
  ifNotExists?: boolean
): Promise<void> {
  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `
      CREATE TABLE ${ifNotExists ? 'IF NOT EXISTS' : ''} %I
      (
        install_id uuid PRIMARY KEY NOT NULL,
        created_at timestamp DEFAUlT NOW() NOT NULL
      )
    `,
        tableName
      )
    )
  );
}

export async function renameProductsTable(
  client: PoolClient,
  oldTableName: string,
  newTableName: string
): Promise<void> {
  await retryDatabaseOperation(async () =>
    client.query(
      format(`ALTER TABLE %I RENAME TO %I`, oldTableName, newTableName)
    )
  );
  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `ALTER INDEX %I RENAME TO %I`,
        `${oldTableName}_pkey`,
        `${newTableName}_pkey`
      )
    )
  );
  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `ALTER INDEX %I RENAME TO %I`,
        `${oldTableName}_service_region_index`,
        `${newTableName}_service_region_index`
      )
    )
  );
  await retryDatabaseOperation(async () =>
    client.query(
      format(
        `ALTER INDEX %I RENAME TO %I`,
        `${oldTableName}_ec2_instances_index`,
        `${newTableName}_ec2_instances_index`
      )
    )
  );
}
