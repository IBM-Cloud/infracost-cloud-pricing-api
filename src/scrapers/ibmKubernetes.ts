import fs from 'fs';
import axios, { AxiosResponse } from 'axios';
import type { Product, Price } from '../db/types';
import { generateProductHash, generatePriceHash } from '../db/helpers';
import { upsertProducts } from '../db/upsert';
import config from '../config';
import { PricingModels } from './ibmCatalog';

// pricing api for IBM Kubernetes infrastructure
const baseUrl = 'https://cloud.ibm.com/containers/cluster-management/api';
const dataFolder = `data/`
const filePrefix = `ibmkube`;
const RETRY_DELAY_MS = 30000;
const MAX_RETRIES = 3;
const vendorName = 'ibm';
const serviceId = 'containers-kubernetes';
// any threshold of nine 9's will be taken to mean infinity and substituted with Inf
const lastThresholdAmountPattern = /999999999/;
const lastThresholdAmount = 'Inf';

// shape of JSON from pricing API
type ibmProductJson = {
  plan_id: string;
  region: string | '';
  flavor: string | '';
  operating_system: string | '';
  unit: string;
  price: string;
  country: string | '';
  currency: string;
  tiers: ibmTiersJson[];
  provider?: string;
  isolation?: string;
  contract_duration?: string;
  ocp_included: string;
  flavor_class?: string;
  catalog_region?: string;
  server_type?: string;
  min_quantity?: number;
  max_quantity?: number;
  deprecated?: string;
  billing_type?: string;
  effective_from?: string;
  effective_until?: string;
};

type ibmTiersJson = {
  price: number;
  instance_hours?: number;
};

type productGroupJson = {
  [key: string]: ibmProductJson[];
};

// schema for attributes of IBM Kubernetes products
export type ibmKubernetesAttributes = {
  currency: string;
  provider?: string;
  flavor?: string;
  flavorClass?: string;
  isolation?: string;
  operatingSystem?: string;
  ocpIncluded: string;
  catalogRegion?: string;
  serverType?: string;
  billingType?: string;
  country?: string;
};

const regions = ['jp-tok','au-syd', 'br-sao', 'ca-tor', 'eu-de', 'eu-es', 'eu-fr2', 'jp-osa', 'eu-gb', 'us-east', 'us-south'];
const platforms = ['kube', 'openshift'];

async function scrape(): Promise<void> {
  await downloadAll();
  await loadAll();
}

function sleep(ms: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function downloadAll(): Promise<void[]> {
  let downloadPromises: Promise<void>[] = []
  platforms.forEach(platform => {
    regions.forEach(region => {
      downloadPromises.push(download(platform, 'vpc-gen2', region))
    });  
  })
  return Promise.all(downloadPromises)
}

// possible providers are ['vpc-gen2', 'classic']
// possible platforms are 'kube', 'addons', and 'dhost'
async function download(platform: string, provider: string, region:string): Promise<void> {
  config.logger.info(`Downloading pricing ${provider}, ${region}`);

  let resp: AxiosResponse | null = null;
  let success = false;
  let attempts = 0;

  do {
    try {
      attempts++;

      resp = await axios({
        method: 'get',
        url: `${baseUrl}/prices/?platform=${platform}&country=USA&region=${region}&provider=${provider}`,
        headers: {
          'Accept': 'application/json',
          'Accept-Language': 'en-US;q=0.9',
          'Referer': 'https://cloud.ibm.com/containers/cluster-management/catalog/create',
          'User-Agent': 'Mozilla/5.0'
        },
      });
      success = true;
    } catch (err: any) {
      // Too many requests, sleep and retry
      if (err.response.status === 429) {
        config.logger.info('Too many requests, sleeping for 30s and retrying');
        await sleep(RETRY_DELAY_MS);
      } else {
        throw err;
      }
    }
  } while (!success && attempts < MAX_RETRIES);

  try {
    const filename=`${dataFolder}${filePrefix}-${provider}-${platform}-${region}.json`
    const writer = fs.createWriteStream(filename);
    await new Promise((resolve, reject) => {
      if (!resp) {
        reject(new Error('empty response'));
        return;
      }
      writer.write(JSON.stringify(resp.data), resolve);
    });
    writer.close();
  } catch (writeErr) {
    config.logger.error(`Skipping IBM instances due to error ${writeErr}.`);
  }
}

/**
 * tiers from the pricing api don't specify a start usage amount (only an end amount);
 * they are inferred based on the previous tier's end amount. this helper is used to populate
 * an appropriate start amount threshold
 */
function getStartUsageAmount(
  productJson: ibmProductJson,
  tierJson: ibmTiersJson,
  prevTierJson: ibmTiersJson
): string {
  if (productJson.min_quantity) return productJson.min_quantity.toString();
  if (tierJson.instance_hours) {
    if (prevTierJson?.instance_hours)
      return prevTierJson.instance_hours.toString();
    return '0';
  }
  return '';
}

/**
 * for the last tier (in a multi-tier), set end threshold to 'Inf' instead of 9999999990 or 999999999
 * @param productJson
 * @param tierJson
 * @param prevTierJson
 * @returns
 */
function getEndUsageAmount(
  productJson: ibmProductJson,
  tierJson: ibmTiersJson
): string {
  if (productJson.max_quantity) return productJson.max_quantity.toString();
  if (tierJson.instance_hours) {
    if (tierJson.instance_hours.toString().match(lastThresholdAmountPattern))
      return lastThresholdAmount;
    return tierJson.instance_hours.toString();
  }
  return '';
}

/**
 * Price Mapping:
 * DB Price:           | ibmProductJson & ibmTiersJson:
 * ------------------- | -------------------------
 * priceHash:          | md5()
 * purchaseOption:     | ''
 * unit:               | unit
 * tierModel:          | PricingModels.LINEAR || PricingModels.STEP_TIER
 * USD?:               | ibmTiersJson.price
 * CNY?:               | NOT USED
 * effectiveDateStart: | effective_from
 * effectiveDateEnd:   | effective_until
 * startUsageAmount:   | min_quantity || ibmTiersJson.instance_hours || 0 || ''
 * endUsageAmount:     | max_quantity || ibmTiersJson.instance_hours || 'Inf' || ''
 * termLength:         | contract_duration || ''
 * termPurchaseOption  | NOT USED
 * termOfferingClass   | NOT USED
 * description         | NOT USED
 */
function parsePrices(product: Product, productJson: ibmProductJson): Price[] {
  const prices: Price[] = [];

  const numTiers = productJson.tiers.length;
  for (let i = 0; i < numTiers; i++) {
    const tierJson = productJson.tiers[i];
    const prevTierJson = i - 1 >= 0 ? productJson.tiers[i - 1] : { price: 0 };
    const price: Price = {
      priceHash: '',
      purchaseOption: '',
      tierModel: numTiers > 1 ? PricingModels.STEP_TIER : PricingModels.LINEAR,
      unit: productJson.unit,
      USD: tierJson.price?.toString(),
      effectiveDateStart: productJson.effective_from || '',
      effectiveDateEnd: productJson.effective_until || '',
      startUsageAmount: getStartUsageAmount(
        productJson,
        tierJson,
        prevTierJson
      ),
      endUsageAmount: getEndUsageAmount(productJson, tierJson),
      termLength: productJson.contract_duration,
    };

    price.priceHash = generatePriceHash(product, price);

    prices.push(price);
  }

  return prices;
}

function parseAttributes(productJson: ibmProductJson): ibmKubernetesAttributes {
  const attributes: ibmKubernetesAttributes = {
    currency: productJson.currency,
    provider: productJson.provider,
    flavor: productJson.flavor,
    flavorClass: productJson.flavor_class,
    isolation: productJson.isolation,
    operatingSystem: productJson.operating_system,
    ocpIncluded: productJson.ocp_included,
    catalogRegion: productJson.catalog_region,
    serverType: productJson.server_type,
    billingType: productJson.billing_type,
    country: productJson.country,
  };

  return attributes;
}

/**
 * Product Mapping:
 * DB:             | ibmProductJson:
 * --------------- | -----------------
 * productHash:    | md5(vendorName + region + sku);
 * sku:            | plan_id - country - currency - flavor - operating_system
 * vendorName:     | 'ibm'
 * region:         | region
 * service:        | 'containers-kubernetes'
 * productFamily:  | ''
 * attributes:     | ibmKubernetesAttributes
 * prices:         | Price[]
 */
function parseIbmProduct(productJson: ibmProductJson): Product {
  const product: Product = {
    productHash: '',
    sku: `${productJson.plan_id}-${productJson.country}-${productJson.currency}-${productJson.flavor}-${productJson.operating_system}-${productJson?.ocp_included ? 'ocp' : 'noocp'}`,
    vendorName,
    region: productJson.region,
    service: serviceId,
    productFamily: '',
    attributes: {},
    prices: [],
  };
  product.productHash = generateProductHash(product);
  product.attributes = parseAttributes(productJson);
  product.attributes.ocpIncluded = productJson?.ocp_included ? 'true' : 'false'
  product.prices = parsePrices(product, productJson);

  return product;
}

// pricing for some products that are deprecated may be provided in the response
// and can be ignored
function isDeprecated(productJson: ibmProductJson): boolean {
  return !!productJson?.deprecated;
}

async function load(filename: string): Promise<void> {
  try {
    console.log(`loading ${filename}`);

    const body = fs.readFileSync(filename);
    const sample = body.toString();
    const json = <productGroupJson>JSON.parse(sample);

    const products: Product[] = [];

    Object.values(json).forEach((productGroup) => {
      productGroup.forEach((ibmProduct) => {
        if (!isDeprecated(ibmProduct)) {
          const product = parseIbmProduct(ibmProduct);
          products.push(product);
        }
      });
    });
    return upsertProducts(products);
  } catch (e: any) {
    config.logger.error(`Skipping file ${filename} due to error ${e}`);
    config.logger.error(e.stack);
    throw e
  }
}

async function loadAll(): Promise<void[]> {
  const dataFolder = './data';
  let loadPromises: Promise<void>[] = []

  fs.readdirSync(dataFolder).forEach(filename => {
    if (filename.match(/ibmkube/)) {
      loadPromises.push(load(`${dataFolder}/${filename}`))
    }
  });
  return Promise.all(loadPromises)
}

export default {
  scrape,
};

