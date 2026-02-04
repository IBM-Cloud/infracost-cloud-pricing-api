import fs from 'fs';
import axios, { AxiosResponse } from 'axios';
import type { Product, Price } from '../db/types';
import { generateProductHash, generatePriceHash } from '../db/helpers';
import { upsertProducts } from '../db/upsert';
import config from '../config';
import { PricingModels } from './ibmCatalog';
import { classifyHttpError, ErrorSeverity, retryOperation } from './errorUtils';

// pricing api for IBM Kubernetes infrastructure
const baseUrl = 'https://cloud.ibm.com/containers/cluster-management/api';
// possible providers are ['vpc-gen2', 'classic']
const PROVIDER = 'vpc-gen2'
const REGIONS = ['jp-tok','au-syd', 'br-sao', 'ca-tor', 'eu-de', 'eu-es', 'eu-fr2', 'jp-osa', 'eu-gb', 'us-east', 'us-south'];
// possible platforms are 'kube', 'openshift', 'addons', and 'dhost'
const PLATFORMS = ['kube', 'openshift'];
const dataFolder = `data/`
const FILE_PREFIX = `ibmkube`;
const MAX_RETRIES = 5;
const vendorName = 'ibm';
const serviceId = 'containers-kubernetes';
// any threshold of nine 9's will be taken to mean infinity and substituted with Inf
const lastThresholdAmountPattern = /999999999/;
const lastThresholdAmount = 'Inf';

// shape of JSON from pricing API
type ibmProductOptionsJson = {
  name: string,
  price: number;
}

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
  options?: ibmProductOptionsJson[]; 
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
  option: string;
};

async function scrape(): Promise<void> {
  await downloadAll(PROVIDER);
  await loadAll(FILE_PREFIX);
}


async function downloadAll(provider: string): Promise<void[]> {
  const downloadPromises: Promise<void>[] = []
  PLATFORMS.forEach(platform => {
    REGIONS.forEach(region => {
      downloadPromises.push(download(platform, provider, region))
    });  
  })
  return Promise.all(downloadPromises)
}

async function download(platform: string, provider: string, region: string): Promise<void> {
  config.logger.info(`Downloading pricing ${provider}, ${region}`);

  const operation = async () => {
    return axios({
      method: 'get',
      url: `${baseUrl}/prices/?platform=${platform}&country=USA&region=${region}&provider=${provider}`,
      headers: {
        'Accept': 'application/json',
        'Accept-Language': 'en-US;q=0.9',
        'Referer': 'https://cloud.ibm.com/containers/cluster-management/catalog/create',
        'User-Agent': 'Mozilla/5.0'
      },
      timeout: 30000,
    });
  };

  try {
    const resp = await retryOperation(operation, {
      maxRetries: MAX_RETRIES,
      shouldRetry: (error) => {
        const severity = classifyHttpError(error, `Kubernetes ${platform}/${provider}/${region}`);
        return severity === ErrorSeverity.RETRY;
      },
      onRetry: (attempt, delay) => {
        config.logger.info(`Retrying ${platform}/${provider}/${region} (attempt ${attempt}/${MAX_RETRIES}, delay ${delay}ms)`);
      }
    });
    
    const filename = `${dataFolder}${FILE_PREFIX}-${provider}-${platform}-${region}.json`;
    await fs.promises.writeFile(filename, JSON.stringify(resp.data));
    config.logger.info(`Saved ${filename}`);
    
  } catch (err: any) {
    config.logger.error(`Failed to download ${platform}/${provider}/${region} after retries: ${err.message}`);
    throw err; // Fatal - can't continue without this data
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

  // Validate tiers array exists and is not empty
  if (!productJson.tiers || productJson.tiers.length === 0) {
    config.logger.warn(`No tiers found for product ${product.sku}`);
    return prices;
  }

  const numTiers = productJson.tiers.length;
  for (let i = 0; i < numTiers; i++) {
    const tierJson = productJson.tiers[i];
    const prevTierJson = i - 1 >= 0 ? productJson.tiers[i - 1] : { price: 0 };
    
    // Validate tier has a price
    if (tierJson.price === undefined || tierJson.price === null) {
      config.logger.warn(`Missing price for tier ${i} in product ${product.sku}`);
      continue;
    }
    
    const price: Price = {
      priceHash: '',
      purchaseOption: '',
      tierModel: numTiers > 1 ? PricingModels.STEP_TIER : PricingModels.LINEAR,
      unit: productJson.unit,
      USD: tierJson.price.toString(),
      effectiveDateStart: productJson.effective_from || '1970-01-01T00:00:00.000Z',
      effectiveDateEnd: productJson.effective_until || undefined,
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
    option: 'none'
  };

  return attributes;
}

function getFirstPrice(prices: Price[]): number {
  let price: number = 0

  if (prices.length > 0) {
    price = prices[0].USD? parseFloat(prices[0].USD) : 0
  }

  return price
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
function parseIbmProduct(productJson: ibmProductJson): Product[] {
  const potentialProductList: Product[] = []

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
  
  // Only add product if it has prices
  if (product.prices.length > 0) {
    potentialProductList.push(product)
  } else {
    config.logger.warn(`Skipping product ${product.sku} - no valid prices found`);
  }

  // If a flavor is found to have options, then each option will be added as a new product in our db, using the parent's
  // attributes to fill out the product's fields.
  // The price of the OCP license option does not include the hourly price for the compute, however hourly prices 
  // for ocp computes in our db's are stored with the OCP licensing included. To maintain the way that hourly compute
  // pricing is used by infracost, the hourly compute price for the flavor will be added to the option's price.
  productJson.options?.forEach((option) => {
    const newProduct = structuredClone(product)
    newProduct.prices = []
    newProduct.sku = `${productJson.plan_id}-${productJson.country}-${productJson.currency}-${productJson.flavor}-${productJson.operating_system}-${option.name}`
    newProduct.attributes.option = `${option.name}`
    // The CLI currently checks for the ocpIncluded attribute
    // as a string of either 'true' or 'false'when searching for pricing.
    // to maintain backwards compatibility, and avoid multiple pricing being returned,
    // options that are not ocp related will set the ocpIncluded field to 'na -> not applicable'
    if (option.name === 'worker-ocp-license') {
      newProduct.attributes.ocpIncluded = 'true'
    } else {
      newProduct.attributes.ocpIncluded = 'na'
    }
    const price: Price = {
      priceHash: '',
      purchaseOption: '',
      tierModel: PricingModels.LINEAR,
      unit: productJson.unit,
      USD: (getFirstPrice(product.prices) + option.price).toString(),
      effectiveDateStart: productJson.effective_from || '1970-01-01T00:00:00.000Z',
      effectiveDateEnd: productJson.effective_until || undefined,
    };
    const priceHash = generatePriceHash(newProduct, price);
    price.priceHash = priceHash
    newProduct.prices = [price]
    newProduct.productHash = generateProductHash(newProduct);
    potentialProductList.push(newProduct)
  });

  return potentialProductList;
}

// pricing for some products that are deprecated may be provided in the response
// and can be ignored
function isDeprecated(productJson: ibmProductJson): boolean {
  return !!productJson?.deprecated;
}

function load(filename: string): Promise<void> {
  try {
    config.logger.info(`Loading ${filename}`);

    const body = fs.readFileSync(filename);
    const json = JSON.parse(body.toString()) as productGroupJson;

    const products: Product[] = [];

    Object.values(json).forEach((productGroup) => {
      productGroup.forEach((ibmProduct) => {
        if (!isDeprecated(ibmProduct)) {
          const expandedProducts = parseIbmProduct(ibmProduct);
          products.push(...expandedProducts);
        }
      });
    });
    
    if (products.length === 0) {
      config.logger.warn(`No products found in ${filename}`);
      return Promise.resolve(); // Warning, not error
    }
    
    config.logger.info(`Loaded ${products.length} products from ${filename}`);
    return upsertProducts(products);
    
  } catch (e: any) {
    // Differentiate between parse errors and DB errors
    if (e instanceof SyntaxError) {
      config.logger.error(`Invalid JSON in ${filename}: ${e.message}`);
    } else if (e.code === 'ENOENT') {
      config.logger.error(`File not found: ${filename}`);
    } else {
      config.logger.error(`Failed to load ${filename}: ${e.message}`);
      config.logger.error(e.stack);
    }
    
    // Don't throw - log and continue with other files
    // This allows partial success if some files fail
    return Promise.resolve();
  }
}

async function loadAll(filePrefix: string): Promise<void> {
  const dataFolder = './data';
  const files = fs.readdirSync(dataFolder)
    .filter(filename => filename.startsWith(filePrefix));
  
  if (files.length === 0) {
    throw new Error(`No files found with prefix ${filePrefix} in ${dataFolder}`);
  }
  
  config.logger.info(`Loading ${files.length} Kubernetes pricing files`);
  
  const loadPromises = files.map(filename => load(`${dataFolder}/${filename}`));
  const results = await Promise.allSettled(loadPromises);
  
  const successful = results.filter(r => r.status === 'fulfilled').length;
  const failed = results.filter(r => r.status === 'rejected').length;
  
  config.logger.info(`Loaded ${successful}/${files.length} files successfully`);
  if (failed > 0) {
    config.logger.warn(`${failed} files failed to load`);
  }
}

export default {
  scrape,
};

