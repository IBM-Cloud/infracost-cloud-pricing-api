import dotenv from 'dotenv';
import * as loggerUtils from '@console/console-platform-log4js-utils';
import NodeCache from 'node-cache';
import { Pool, PoolConfig } from 'pg';
import tmp from 'tmp';
import fs from 'fs';
import path from 'path';

dotenv.config({ path: '.env.local' });
dotenv.config();

const createPaths = [
  path.join(__dirname, '../data'),
  path.join(__dirname, '../data/products'),
];

createPaths.forEach((path) => {
  if (!fs.existsSync(path)) {
    fs.mkdirSync(path);
  }
});

interface CredentialHost {
  hostname: string;
  port: number;
}
interface Credentials {
  connection: {
    postgres: {
      authentication: {
        method: string;
        password: string;
        username: string;
      };
      certificate: {
        certificate_authority: string;
        certificate_base64: string;
        name: string;
      };
      composed: Array<string>;
      database: string;
      hosts: Array<CredentialHost>;
      path: string;
      query_options: {
        sslmode: string;
      };
      scheme: string;
      type: string;
    }
  };
  instance_administration_api: {
    deployment_id: string;
    instance_id: string;
    root: string;
  }
}

const jsonPgCredentials = process.env.POSTGRES_CREDENTIALS;

let pgPool: Pool;
let user: string;
const database = process.env.POSTGRES_DB || 'cloud_pricing';
let password: string;
let host: string;
let port: number;
let cert64: string | undefined;

async function pg(): Promise<Pool> {
  if (!pgPool) {
    let pgCredentials: Credentials
    if (jsonPgCredentials) {
      try {
        pgCredentials = JSON.parse(jsonPgCredentials);
        user = pgCredentials?.connection.postgres.authentication.username;
        password = pgCredentials?.connection.postgres.authentication.password;
        host = pgCredentials?.connection.postgres.hosts[0].hostname;
        port = pgCredentials?.connection.postgres.hosts[0].port;
        cert64 = pgCredentials?.connection.postgres.certificate.certificate_base64;
      } catch (error: unknown) {
        let message = 'Unknown Error'
        if (error instanceof Error) message = error.message
        logger.error(`Error parsing POSTGRES_CREDENTIALS ${message}`)
      }

    } else {
      logger.warn(`Error POSTGRES_CREDENTIALS are missing`)
      user = process.env.POSTGRES_USER || 'postgres'; 
      password = process.env.POSTGRES_PASSWORD || '';
      host = process.env.POSTGRES_HOST || 'localhost';
      port = Number(process.env.POSTGRES_PORT) || 5432;
      cert64 = process.env.POSTGRES_CERTIFICATE_BASE64;
    }

    let poolConfig: PoolConfig = {
      user,
      database,
      password,
      port,
      host,
      max: Number(process.env.POSTGRES_MAX_CLIENTS) || 10,
    };

    if (process.env.POSTGRES_URI) {
      poolConfig = {
        connectionString:
          process.env.POSTGRES_URI ||
          'postgresql://postgres:@localhost:5432/cloud_pricing',
      };
    }
    // support for cloud hosted postgres db's which provide self-signed certs for TLS connections
    if (cert64) {
      const cert = Buffer.from(cert64, 'base64')?.toString('utf8')
      poolConfig.ssl = {
        ca: cert
      };
    }

    pgPool = new Pool(poolConfig);
  }
  return pgPool;
}

function generateGcpKeyFile(): string {
  if (process.env.GCP_KEY_FILE) {
    return process.env.GCP_KEY_FILE;
  }

  const tmpFile = tmp.fileSync({ postfix: '.json' });
  tmp.setGracefulCleanup();

  fs.writeFileSync(tmpFile.name, process.env.GCP_KEY_FILE_CONTENT || '');
  return tmpFile.name;
}

const logger = loggerUtils.getLogger('cloud-pricing-api');

const cache = new NodeCache();

const config = {
  logger,
  pg,
  productTableName: 'products',
  statsTableName: 'stats',
  installsTableName: 'installs',
  infracostPricingApiEndpoint:
    process.env.INFRACOST_PRICING_API_ENDPOINT ||
    'https://pricing.api.infracost.io',
  infracostDashboardApiEndpoint:
    process.env.INFRACOST_DASHBOARD_API_ENDPOINT ||
    'https://dashboard.api.infracost.io',
  disableTelemetry:
    process.env.DISABLE_TELEMETRY?.toLowerCase() === 'true' ||
    process.env.DISABLE_TELEMETRY === '1',
  infracostAPIKey: process.env.INFRACOST_API_KEY,
  selfHostedInfracostAPIKey: process.env.SELF_HOSTED_INFRACOST_API_KEY,
  cache,
  port: Number(process.env.PORT) || 4000,
  gcpApiKey: process.env.GCP_API_KEY,
  gcpKeyFile: generateGcpKeyFile(),
  gcpProject: process.env.GCP_PROJECT,
  ibmCloudApiKey: process.env.IBM_CLOUD_API_KEY,
  region: process.env.CLOUD_REGION || 'local',
  hostname: process.env.HOSTNAME || 'local',
  version: process.env.IMAGE_VERSION?.split(":")[1] || 'local' 
};

export default config;
