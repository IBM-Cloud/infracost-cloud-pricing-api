import GlobalCatalogV1, {
  PricingGet,
} from '@ibm-cloud/platform-services/global-catalog/v1';
import { IamTokenManager } from '@ibm-cloud/platform-services/auth';
import { writeFile } from 'fs/promises';
import _ from 'lodash';
import axios, { AxiosInstance } from 'axios';

import type { Product, Price } from '../db/types';
import { generateProductHash } from '../db/helpers';
import addProducts from '../db/add';
import config from '../config';
import { classifyHttpError, ErrorSeverity, retryOperation, createProgressTracker } from './errorUtils';

const DEBUG = false;
const saasProductFileName = `data/ibm-catalog-saas-products.json`;
const iaasProductFileName = `data/ibm-catalog-iaas-products.json`;
const platformProductFileName = `data/ibm-catalog-platform-products.json`;
const compositeProductFileName = `data/ibm-catalog-composite-products.json`;

const baseURL = 'https://globalcatalog.cloud.ibm.com/api/v1';

const skipList = [
  'containers-kubernetes', // To be scraped with the Kubernetes API
];

// Composite services that shouldn't be scraped, as they're not really products
const compositeSkipList = [
  'billing',
  'context-based-restrictions',
  'iam-access-management'
];

type RecursiveNonNullable<T> = {
  [K in keyof T]-?: RecursiveNonNullable<NonNullable<T[K]>>;
};

type CatalogEntry = GlobalCatalogV1.CatalogEntry & {
  children: CatalogEntry[];
  pricingChildren?: PricingGet[];
};

type GCPrice = {
  price: number;
  quantity_tier: number;
};

type CompletePricingGet = PricingGet & {
  deployment_id?: string;
  deployment_location?: string;
  deployment_region?: string;
  effective_from?: string;
  effective_until?: string;
};

type UsageMetrics = {
  tierModel?: string;
  chargeUnitName?: string;
  chargeUnit?: string;
  chargeUnitQty?: string;
  usageCapQty?: number;
  displayCap?: number;
  effectiveFrom?: string;
  effectiveUntil?: string;
};

type AmountsRecord = Record<string, Record<string, GCPrice[]>>;
type MetricsRecord = Record<string, Omit<UsageMetrics, 'amounts'>>;

type AmountsAndMetrics = {
  amounts?: AmountsRecord;
  metrics?: MetricsRecord;
};

type PricingMetaData = AmountsAndMetrics & {
  type: string;
  region: string;
};

export type Service = {
  id?: string;
  name?: string;
  plans?: (Plan | undefined)[];
};

export type Plan = {
  id?: string;
  name?: string;
  deployments?: (Deployment | undefined)[];
};

export type Deployment = {
  id?: string;
  name?: string;
  geo_tags?: string[];
};

// https://cloud.ibm.com/docs/sell?topic=sell-meteringintera#pricing
export enum PricingModels {
  LINEAR = 'Linear',
  PRORATION = 'Proration',
  GRANULAR_TIER = 'Granular Tier',
  STEP_TIER = 'Step Tier',
  BLOCK_TIER = 'Block Tier',
}

// schema for attributes of IBM products
export type ibmAttributes = {
  planName?: string;
  planType?: string;
  startPrice?: string;
  startQuantityTier?: string;
  region?: string;
};

/**
 * Flattens the tree of pricing info for a plan, as each plan can describe multiple charge models (Metric).
 * A charge model describes the pricing model (tier), the unit, a quantity, and the part number to charge against.
 * Multiple charge models can be in use at one time, which would translate to multiple cost components for a product.
 *
 * The pricing for each country-currency combination is available for each Metric in an 'amounts' array. Any thresholds
 * for tiered pricing models are defined with the price for each country-currency.
 *
 * Pricing for a plan:
 * - plan type
 * - Metrics [] (by part number and tier model)
 *   - tierModel
 *   - unit
 *   - quantity
 *   - part number
 *   - Amounts [] (by country and currency)
 *     - quantity threshold
 *     - price
 *
 * @param pricingObject
 * @returns
 */
function parsePricingJson(
  pricingObject: GlobalCatalogV1.PricingGet
): PricingMetaData | null {
  if (!('metrics' in pricingObject)) {
    return null;
  }
  const {
    type,
    deployment_location: region,
    metrics,
  } = pricingObject as RecursiveNonNullable<CompletePricingGet>;

  let amountAndMetrics: AmountsAndMetrics = { amounts: {}, metrics: {} };
  if (metrics?.length) {
    amountAndMetrics = metrics.reduce(
      (collection, metric: GlobalCatalogV1.Metrics): AmountsAndMetrics => {
        const {
          metric_id: metricId,
          amounts,
          tier_model: tierModel,
          charge_unit_name: chargeUnitName,
          charge_unit: chargeUnit,
          charge_unit_quantity: chargeUnitQty,
          usage_cap_qty: usageCapQty,
          display_cap: displayCap,
          effective_from: effectiveFrom,
          effective_until: effectiveUntil,
        } = metric;

        if (!metricId) {
          return collection;
        }

        if (!collection.metrics) {
          return collection;
        }
        // eslint-disable-next-line no-param-reassign
        collection.metrics[metricId] = {
          tierModel,
          chargeUnitName,
          chargeUnit,
          chargeUnitQty: String(chargeUnitQty),
          usageCapQty,
          displayCap,
          effectiveFrom,
          effectiveUntil,
        };
        amounts?.forEach((amount) => {
          if (amount?.prices?.length && amount?.country && amount?.currency) {
            const key = `${amount.country}-${amount.currency}`;
            if (!collection.amounts) {
              return;
            }
            if (collection.amounts[key]) {
              // eslint-disable-next-line no-param-reassign
              collection.amounts[key][metricId] = amount.prices as GCPrice[];
            } else {
              // eslint-disable-next-line no-param-reassign
              collection.amounts[key] = {
                [metricId]: amount.prices as GCPrice[],
              };
            }
          }
        });
        return collection;
      },
      amountAndMetrics
    );
    return {
      type,
      region,
      ...amountAndMetrics,
    };
  }
  return {
    type,
    region,
  };
}

/**
 * Price Mapping:
 * DB Price:           | ibmCatalogJson:
 * ------------------- | -------------------------
 * priceHash:          | (unit-purchaseOption-country-currency-startUsageAmount-partNumber)
 * purchaseOption:     | chargeUnitQty
 * unit:               | chargeUnitName
 * tierModel:          | PricingMetaData.metrics[partNumber].tierModel
 * USD?:               | PricingMetaData.amounts[geo].costs.price
 * CNY?:               | NOT USED
 * effectiveDateStart: | PricingMetaData.metrics[partNumber].effectiveFrom
 * effectiveDateEnd:   | PricingMetaData.metrics[partNumber].effectiveUntil
 * startUsageAmount:   | NOT USED
 * endUsageAmount:     | quantityTier
 * termLength:         | NOT USED
 * termPurchaseOption  | NOT USED
 * termOfferingClass   | NOT USED
 * description         | chargeUnit
 * country             | country
 * currency            | currency
 * partNumber          | partNumber
 *
 * @param currency https://www.ibm.com/support/pages/currency-code-9comibmhelpsppartneruserdocsaasspcurrencycodehtml
 *
 * @returns an empty array if no pricing found
 */
function getPrices(
  pricing: PricingMetaData,
  country: string,
  currency: string
): Price[] {
  const prices: Price[] = [];

  if (pricing) {
    const { metrics, amounts } = pricing;
    const geoKey = `${country}-${currency}`;
    if (!metrics || !amounts || !amounts[geoKey]) {
      return prices;
    }
    for (const [partNumber, costs] of Object.entries(amounts[geoKey])) {
      // Validate that metrics exist for this part number
      if (!metrics[partNumber]) {
        config.logger.warn(`Missing metrics for part number ${partNumber}`);
        continue;
      }
      
      const {
        tierModel,
        chargeUnitName,
        chargeUnit,
        chargeUnitQty,
        effectiveFrom,
        effectiveUntil,
      } = metrics[partNumber];

      for (const [i, cost] of Object.entries(costs)) {
        const prevCost = costs[Number(i) - 1];
        const { price, quantity_tier: quantityTier } = cost;
        
        // Validate price and quantity tier
        if (price === undefined || price === null || quantityTier === undefined || quantityTier === null) {
          config.logger.warn(`Invalid price or quantity tier for part number ${partNumber}`);
          continue;
        }
        
        const chargeQty = parseInt(chargeUnitQty ?? '1', 10);
        if (isNaN(chargeQty) || chargeQty === 0) {
          config.logger.warn(`Invalid chargeUnitQty for part number ${partNumber}: ${chargeUnitQty}`);
          continue;
        }
        
        prices.push({
          priceHash: `${chargeUnitName}-${chargeUnitQty}-${country}-${currency}-${quantityTier}-${partNumber}`,
          purchaseOption: String(chargeUnitQty),
          USD: String(price / chargeQty),
          startUsageAmount: prevCost?.quantity_tier
            ? String(prevCost?.quantity_tier)
            : '0',
          endUsageAmount: String(quantityTier),
          tierModel,
          description: chargeUnit,
          unit: chargeUnitName ?? '',
          effectiveDateStart: effectiveFrom ?? '1970-01-01T00:00:00.000Z',
          effectiveDateEnd: effectiveUntil,
          country,
          currency,
          partNumber,
        });
      }
    }
  }
  return prices;
}

/**
 * Schema for IBM product Attributes from global catalog
 * ibmAttributes     | CatalogEntryMetadataPricing
 * ----------------- | ----------------------------
 * planName          | id (Global Catalog serviceId)
 * planType          | type (https://cloud.ibm.com/docs/account?topic=account-accounts)
 * startingPrice     | startingPrice
 * startQuantityTier | startQuantityTier
 * region            | region
 *
 * @param pricing
 * @returns
 */
function getAttributes(
  pricing: PricingMetaData,
  planName: string
): ibmAttributes {
  if (!pricing) {
    return {};
  }

  const { type, region } = pricing;

  const attributes: ibmAttributes = {
    planName,
    planType: type,
    region,
  };

  return attributes;
}

function collectPriceComponents(
  product: CatalogEntry,
  plan: CatalogEntry,
  pricing: PricingGet,
  productFamily: string
): Product[] {
  const products: Product[] = [];
  // for now, only grab USA, USD pricing
  const country = 'USA';
  const currency = 'USD';
  const processedPricing = parsePricingJson(pricing);
  if (!processedPricing) {
    return products;
  }
  const prices = getPrices(processedPricing, country, currency);
  const attributes = getAttributes(processedPricing, plan.name);
  const region = attributes?.region || country;

  if (prices?.length) {
    const p = {
      productHash: ``,
      sku: `${product.name}-${plan.name}`,
      vendorName: 'ibm',
      region,
      service: product.name,
      productFamily,
      attributes,
      prices,
    };
    p.productHash = generateProductHash(p);
    products.push(p);
  }
  return products;
}

/**
 * SaaS grouping
 *
 * service (group) - optional
 *  |
 *  |
 *  | 0..n
 *   - - - > service
 *           |
 *           |
 *           | 0..n
 *            - - - > plan -> pricing might be here too, if not region specific
 *                   |
 *                   |
 *                   | 0..n
 *                    - - - > deployment -> pricing might be here, if deployment specific <- pricing on previous level will be returned here too
 *
 *  IaaS grouping
 *
 *  iaas (group) - optional
 *  |
 *  |
 *  | 0..n
 *   - - - > iaas
 *           |
 *           |
 *           | 0..n
 *            - - - > plan - possibly priced (for satellite i.e cos satellite plan)
 *                    |
 *                    |
 *                    | 0..n
 *                     - - - > deployment - possibly  priced
 *                             |
 *                             |
 *                             | 0..n
 *                              - - - > plan - possibly priced
 *                                      |
 *                         s             |
 *                                      | 0..n
 *                                       - - - > deployment - possibly priced
 *
 *  Composite grouping
 *  
 *  There is currently 1 composite service in the catalog that has plans directly contained as children within it: power-iaas.
 * 
 *  composite
 *  |
 *  |
 *   - - - > service
 *  |
 *  | 0..n
 *   - - - > plan
 *           |
 *           | 0..n
 *            - - - > deployment -> pricing might be here
 *   
 * Global Catalog is an hierarchy of things, of which we are interested in 'services' and 'iaas' and 'composite' kinds on the root.
 *
 * Root:
 * | xaas
 *   - plans []
 *     - deployments [] (by region)
 *       - region
 *       - amounts [] (by country-currency)
 *         - part number
 *           - price
 *           - quantity threshold
 *       - metrics [] (by part number)
 *         - tier
 *         - unit
 *         - effective dates
 *
 *
 *
 * Product Mapping:
 * DB:             | CatalogJson:
 * --------------- | -----------------
 * productHash:    | md5(vendorName + region + sku);
 * sku:            | service - plan_id
 * vendorName:     | 'ibm'
 * region:         | PricingMetaData.region || country
 * service:        | serviceName
 * productFamily:  | 'service' || 'iaas'
 * attributes:     | ibmAttributes
 * prices:         | Price[]
 *
 * @param entries
 * @param productFamily
 * @returns
 */
function parseProducts(
  entries: CatalogEntry[],
  productFamily?: string
): Product[] {
  const products: Product[] = [];
  for (const entry of entries) {
    const categorizedChildren = entry?.children?.reduce(
      (acc: { xaas: CatalogEntry[]; other: CatalogEntry[] }, child) => {
        if (child.kind === 'iaas' || child.kind === 'service') {
          acc.xaas.push(child);
        } else {
          acc.other.push(child);
        }
        return acc;
      },
      { xaas: [], other: [] }
    );
    if (categorizedChildren?.xaas?.length > 0) {
      products.push(
        ...parseProducts(categorizedChildren.xaas, productFamily || entry.kind)
      );
    }
    if (categorizedChildren?.other?.length > 0) {
      for (const possiblePlan of categorizedChildren.other) {
        const walkPlan = (plan: CatalogEntry) => {
          if (plan.kind === 'plan') {
            if (plan.children) {
              for (const deployment of plan.children) {
                if (deployment.pricingChildren) {
                  for (const pricing of deployment.pricingChildren) {
                    const results = collectPriceComponents(
                      entry,
                      plan,
                      pricing,
                      productFamily || entry.kind
                    );
                    products.push(...results);
                  }
                }
                if (deployment.children) {
                  for (const child of deployment.children) {
                    walkPlan(child);
                  }
                }
              }
            }
          }
        };
        walkPlan(possiblePlan);
      }
    }
  }
  return products;
}

// q: 'kind:service active:true price:paygo',
const serviceParams = {
  q: 'kind:service active:true',
  include: 'id:geo_tags:kind:name:pricing_tags:tags',
  account: 'global',
};

const infrastuctureParams = {
  q: 'kind:iaas active:true',
  include: 'id:geo_tags:kind:name:pricing_tags:tags',
  account: 'global',
};

const platformServiceParams = {
  q: 'kind:platform_service active:true',
  include: 'id:geo_tags:kind:name:pricing_tags:tags',
  account: 'global',
};

const compositeServiceParams = {
  q: 'kind:composite active:true',
  include: 'id:geo_tags:kind:name:pricing_tags:tags',
  account: 'global',
};

async function getCatalogEntries(
  axiosClient: AxiosInstance,
  params: Pick<
    GlobalCatalogV1.ListCatalogEntriesParams,
    'account' | 'q' | 'include'
  >
): Promise<GlobalCatalogV1.CatalogEntry[]> {
  const limit = 200;
  let offset = 0;
  let next = true;
  const servicesArray = [];

  while (next) {
    try {
      const { data: response } = await axiosClient.get<{
        count: number;
        resources: CatalogEntry[];
      }>('/', {
        params: {
          ...params,
          _limit: limit,
          _offset: offset,
        },
      });
      if (response.resources?.length) {
        servicesArray.push(...response.resources);
      }
      if (!response.count || offset >= response.count) {
        next = false;
      }
      offset += limit;
    } catch (error) {
      const severity = classifyHttpError(error, 'getCatalogEntries');
      if (severity === ErrorSeverity.FATAL) {
        throw error;
      }
      config.logger.error(`Failed to fetch catalog entries at offset ${offset}, stopping pagination`);
      next = false;
    }
  }
  return servicesArray;
}

async function fetchPricingForProduct(
  axiosClient: AxiosInstance,
  product: GlobalCatalogV1.CatalogEntry
): Promise<CatalogEntry> {
  const MAX_RETRIES = 3;

  const operation = async () => {
    const { data: tree } = await axiosClient.get<CatalogEntry>(
      `/${product.id as string}`,
      {
        params: {
          noLocations: true,
          depth: 10,
          include: 'id:kind:name:tags:pricing_tags:geo_tags:metadata',
        },
      }
    );

    const stack = [tree];
    while (stack.length > 0) {
      // Couldn't get here if there were no elems on the stack
      const currentElem = stack.pop() as CatalogEntry;
      // For example satellite located deployments are also priced on the plan level
      if (currentElem.kind === 'plan' && currentElem.children) {
        const deploymentChildren = currentElem.children.filter(
          (child) => child.kind === 'deployment'
        );
        const chunks = _.chunk(deploymentChildren, 8);
        for (const elements of chunks) {
          await Promise.all(
            elements.map(async (element): Promise<void> => {
              try {
                const { data: pricingObject } = await axiosClient.get<PricingGet>(
                  `/${element.id}/pricing`
                );
                if (!pricingObject) {
                  return;
                }
                // eslint-disable-next-line no-param-reassign
                element.pricingChildren = [pricingObject];
              } catch (e: unknown) {
                const severity = classifyHttpError(e, `pricing for ${element.id}`);
                if (severity === ErrorSeverity.FATAL) {
                  throw e;
                }
                // Skip or retry handled by classification
              }
            })
          );
        }
      }
      if (currentElem.children && currentElem.children.length > 0) {
        for (const child of currentElem.children) {
          stack.push(child);
        }
      }
    }
    return tree;
  };

  try {
    return await retryOperation(operation, {
      maxRetries: MAX_RETRIES,
      shouldRetry: (error) => {
        const severity = classifyHttpError(error, `product ${product.name}`);
        return severity === ErrorSeverity.RETRY;
      },
      onRetry: (attempt, delay) => {
        config.logger.info(`Retrying ${product.name} (attempt ${attempt}/${MAX_RETRIES}, delay ${delay}ms)`);
      }
    });
  } catch (error) {
    const severity = classifyHttpError(error, `product ${product.name}`);
    if (severity === ErrorSeverity.FATAL) {
      throw error;
    }
    // Return empty tree for skipped products
    config.logger.warn(`Skipping product ${product.name} due to error`);
    return { ...product, children: [] } as CatalogEntry;
  }
}

async function scrape(): Promise<void> {
  config.logger.info(`Started IBM Cloud scraping at ${new Date()}`);
  const saasResults: CatalogEntry[] = [];
  const iaasResults: CatalogEntry[] = [];
  const psResults: CatalogEntry[] = [];
  const compositeResults: CatalogEntry[] = [];

  const apikey = config.ibmCloudApiKey;
  let dataString;

  if (typeof apikey !== 'string' || apikey.trim().length === 0) {
    throw new Error('No IBM_CLOUD_API_KEY provided!');
  }

  const tokenManager = new IamTokenManager({
    apikey,
  });

  // We won't need token refreshing
  const axiosClient = axios.create({
    baseURL,
    timeout: 60000, // 60 second timeout for catalog operations
    headers: {
      Authorization: `Bearer ${await tokenManager.getToken()}`,
    },
  });

  config.logger.info('Fetching Service products...');

  const serviceEntries = await getCatalogEntries(axiosClient, {
    ...serviceParams,
  });

  const filteredServices = serviceEntries.filter(
    s => s.kind === 'service' && s.name && !skipList.includes(s.name)
  );

  const serviceProgress = createProgressTracker(filteredServices.length, 'Service scraping');
  for (const service of filteredServices) {
    config.logger.info(`Scraping pricing for ${service.name}`);
    const tree = await fetchPricingForProduct(axiosClient, service);
    saasResults.push(tree);
    serviceProgress.increment();
  }

  const saasProducts = parseProducts(saasResults);
  if (saasProducts.length === 0) {
    config.logger.warn('No SaaS products scraped - possible scraper failure');
  } else {
    config.logger.info(`Scraped ${saasProducts.length} SaaS products`);
  }

  if (DEBUG) {
    dataString = JSON.stringify(saasProducts);
    await writeFile(saasProductFileName, dataString);
  }

  config.logger.info('Fetching Infrastructure products...');

  const infrastructureEntries = await getCatalogEntries(axiosClient, {
    ...infrastuctureParams,
  });

  const filteredInfra = infrastructureEntries.filter(
    i => i.kind === 'iaas' && i.name && !skipList.includes(i.name)
  );

  const infraProgress = createProgressTracker(filteredInfra.length, 'Infrastructure scraping');
  for (const infra of filteredInfra) {
    config.logger.info(`Scraping pricing for ${infra.name}`);
    const tree = await fetchPricingForProduct(axiosClient, infra);
    iaasResults.push(tree);
    infraProgress.increment();
  }

  const iaasProducts = parseProducts(iaasResults);
  if (iaasProducts.length === 0) {
    config.logger.warn('No IaaS products scraped - possible scraper failure');
  } else {
    config.logger.info(`Scraped ${iaasProducts.length} IaaS products`);
  }

  if (DEBUG) {
    dataString = JSON.stringify(iaasProducts);
    await writeFile(iaasProductFileName, dataString);
  }


  config.logger.info('Fetching Platform Service products...');

  const platformServiceEntries = await getCatalogEntries(axiosClient, {
    ...platformServiceParams,
  });

  const filteredPS = platformServiceEntries.filter(
    ps => ps.kind === 'platform_service' && ps.name && !skipList.includes(ps.name)
  );

  const psProgress = createProgressTracker(filteredPS.length, 'Platform Service scraping');
  for (const ps of filteredPS) {
    config.logger.info(`Scraping pricing for ${ps.name}`);
    const tree = await fetchPricingForProduct(axiosClient, ps);
    psResults.push(tree);
    psProgress.increment();
  }

  const psProducts = parseProducts(psResults, 'service');
  if (psProducts.length === 0) {
    config.logger.warn('No Platform Service products scraped - possible scraper failure');
  } else {
    config.logger.info(`Scraped ${psProducts.length} Platform Service products`);
  }

  if (DEBUG) {
    dataString = JSON.stringify(psProducts);
    await writeFile(platformProductFileName, dataString);
  }

  config.logger.info('Fetching Composite products...');

  const compositeServiceEntries = await getCatalogEntries(axiosClient, {
    ...compositeServiceParams,
  });

  const filteredComposite = compositeServiceEntries.filter(
    s => s.name && !compositeSkipList.includes(s.name)
  );

  const compositeProgress = createProgressTracker(filteredComposite.length, 'Composite scraping');
  for (const service of filteredComposite) {
    config.logger.info(`Scraping pricing for ${service.name}`);
    const tree = await fetchPricingForProduct(axiosClient, service);
    compositeResults.push(tree);
    compositeProgress.increment();
  }

  const compositeProducts = parseProducts(compositeResults, 'service');
  if (compositeProducts.length === 0) {
    config.logger.warn('No Composite products scraped - possible scraper failure');
  } else {
    config.logger.info(`Scraped ${compositeProducts.length} Composite products`);
  }

  if (DEBUG) {
    dataString = JSON.stringify(compositeProducts);
    await writeFile(compositeProductFileName, dataString);
  }

  await addProducts(saasProducts);
  await addProducts(iaasProducts);
  await addProducts(psProducts);
  await addProducts(compositeProducts);

  config.logger.info(`Ended IBM Cloud scraping at ${new Date()}`);
}

export default {
  scrape,
};
