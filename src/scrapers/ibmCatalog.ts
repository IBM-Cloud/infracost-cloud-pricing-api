import GlobalCatalogV1, {
  PricingGet,
} from '@ibm-cloud/platform-services/global-catalog/v1';
import { IamTokenManager } from '@ibm-cloud/platform-services/auth';
import { writeFile } from 'fs/promises';
import _ from 'lodash';
import axios, { AxiosInstance } from 'axios';

import type { Product, Price } from '../db/types';
import { generateProductHash } from '../db/helpers';
import { upsertProducts } from '../db/upsert';
import config from '../config';

const saasDataFileName = `data/ibm-catalog-saas.json`;
const saasProductFileName = `data/ibm-catalog-saas-products.json`;

const iaasDataFileName = `data/ibm-catalog-iaas.json`;
const iaasProductFileName = `data/ibm-catalog-iaas-products.json`;

const baseURL = 'https://globalcatalog.cloud.ibm.com/api/v1';

const skipList = [
  'containers-kubernetes', // To be scraped with the Kubernetes API
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
          chargeUnitQty,
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
        prices.push({
          priceHash: `${chargeUnitName}-${chargeUnitQty}-${country}-${currency}-${quantityTier}-${partNumber}`,
          purchaseOption: String(chargeUnitQty),
          USD: String(price),
          startUsageAmount: prevCost?.quantity_tier
            ? String(prevCost?.quantity_tier)
            : '0',
          endUsageAmount: String(quantityTier),
          tierModel,
          description: chargeUnit,
          unit: chargeUnitName ?? '',
          effectiveDateStart: effectiveFrom ?? new Date().toISOString(),
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
 * Global Catalog is an hierarchy of things, of which we are interested in 'services' and 'iaas' kinds on the root.
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
    if (!response.count || offset > response.count) {
      next = false;
    }
    offset += limit;
  }
  return servicesArray;
}

async function fetchPricingForProduct(
  axiosClient: AxiosInstance,
  product: GlobalCatalogV1.CatalogEntry
): Promise<CatalogEntry> {
  const { data: tree } = await axiosClient.get<CatalogEntry>(
    `/${product.id as string}`,
    {
      params: {
        noLocations: true,
        depth: 10,
        include: 'id:kind:name:tags:pricing_tags:geo_tags:meatadata',
      },
    }
  );
  const stack = [tree];
  while (stack.length > 0) {
    // Couldn't get here if the were no elems on the stack
    const currentElem = stack.pop() as CatalogEntry;
    // For example satellite located deployments area also priced on the plan level
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
            } catch (e) {
              if (axios.isAxiosError(e)) {
                if (!e?.response?.status || e?.response?.status !== 404) {
                  config.logger.error(e.message);
                }
              } else if (e instanceof Error) {
                config.logger.error(e.message);
              } else {
                config.logger.error(e);
              }
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
}

async function scrape(): Promise<void> {
  config.logger.info(`Started IBM Cloud scraping at ${new Date()}`);
  const saasResults: CatalogEntry[] = [];
  const iaasResults: CatalogEntry[] = [];

  const tokenManager = new IamTokenManager({
    apikey: config.ibmCloudApiKey as string,
  });

  // We won't need token refreshing
  const axiosClient = axios.create({
    baseURL,
    headers: {
      Authorization: `Bearer ${await tokenManager.getToken()}`,
    },
  });

  config.logger.info('Fetching Service products...');

  const serviceEntries = await getCatalogEntries(axiosClient, {
    ...serviceParams,
  });

  for (const service of serviceEntries) {
    if (service.kind === 'service' && !skipList.includes(service.name)) {
      config.logger.info(`Scraping pricing for ${service.name}`);
      const tree = await fetchPricingForProduct(axiosClient, service);
      saasResults.push(tree);
    }
  }

  await writeFile(saasDataFileName, JSON.stringify(saasResults, null, 2));
  const saasProducts = parseProducts(saasResults);
  await writeFile(saasProductFileName, JSON.stringify(saasProducts, null, 2));

  config.logger.info('Fetching Infrastructure products...');

  const infrastructureEntries = await getCatalogEntries(axiosClient, {
    ...infrastuctureParams,
  });

  for (const infra of infrastructureEntries) {
    if (infra.kind === 'iaas' && !skipList.includes(infra.name)) {
      config.logger.info(`Scraping pricing for ${infra.name}`);
      const tree = await fetchPricingForProduct(axiosClient, infra);
      iaasResults.push(tree);
    }
  }

  await writeFile(iaasDataFileName, JSON.stringify(iaasResults, null, 2));
  const iaasProducts = parseProducts(iaasResults);
  await writeFile(iaasProductFileName, JSON.stringify(iaasProducts, null, 2));

  await upsertProducts(saasProducts);
  await upsertProducts(iaasProducts);

  config.logger.info(`Ended IBM Cloud scraping at ${new Date()}`);
}

export default {
  scrape,
};
