import GlobalCatalogV1, {
  PricingGet,
} from '@ibm-cloud/platform-services/global-catalog/v1';
import { IamAuthenticator } from '@ibm-cloud/platform-services/auth';
import { writeFile } from 'fs/promises';
import _ from 'lodash';

import type { Product, Price } from '../db/types';
import { generateProductHash } from '../db/helpers';
import { upsertProducts } from '../db/upsert';
import config from '../config';

const client = GlobalCatalogV1.newInstance({
  authenticator: new IamAuthenticator({
    apikey: config.ibmCloudApiKey as string,
  }),
});

const filename = `data/ibm-catalog.json`;

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

type Kinds = 'service' | 'plan' | 'deployment' | 'iaas' | 'pricing';

const globalCatalogHierarchy: { [K in Kinds]?: Kinds } = {
  service: 'plan',
  plan: 'deployment',
  deployment: 'pricing',
  iaas: 'iaas',
};

type RecursiveNonNullable<T> = {
  [K in keyof T]-?: RecursiveNonNullable<NonNullable<T[K]>>;
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
      (
        collection,
        metric: GlobalCatalogV1.Metrics
      ): AmountsAndMetrics => {
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
              if(!collection.amounts) {
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

// q: 'kind:service active:true price:paygo',
const serviceParams = {
  q: 'kind:service active:true',
  include: 'id:geo_tags:kind:name:pricing_tags',
};

const infrastuctureQuery = {
  q: 'kind:iaas active:true',
  limit: 5,
  include: 'id:geo_tags:kind:name:pricing_tags',
};

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
  let prices: Price[] = [];

  if (pricing) {
    const { metrics, amounts } = pricing;

    if (!metrics || !amounts) {
      return prices;
    }
    const geoKey = `${country}-${currency}`;
    prices = Object.entries(amounts[geoKey])
      .map(([partNumber, costs]): Price[] => {
        const {
          tierModel,
          chargeUnitName,
          chargeUnit,
          chargeUnitQty,
          // usageCapQty,
          // displayCap,
          effectiveFrom,
          effectiveUntil,
        } = metrics[partNumber];

        return costs.map((cost: GCPrice): Price => {
          const { price, quantity_tier: quantityTier } = cost;

          return {
            priceHash: `${chargeUnitName}-${chargeUnitQty}-${country}-${currency}-${quantityTier}-${partNumber}`,
            purchaseOption: String(chargeUnitQty),
            USD: String(price),
            endUsageAmount: String(quantityTier),
            tierModel,
            // usageCapQty,
            // displayCap,
            description: chargeUnit,
            unit: chargeUnitName ?? '',
            effectiveDateStart: effectiveFrom ?? new Date().toISOString(),
            effectiveDateEnd: effectiveUntil,
            country,
            currency,
            partNumber,
          };
        });
      })
      .flat();
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

/**
 * Global Catalog is an hierarchy of things, of which we are interested in 'services' and 'iaas' kinds on the root.
 *
 * Root:
 * | service
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
 *  | iaas
 *    -
 *
 * Product Mapping:
 * DB:             | CatalogJson:
 * --------------- | -----------------
 * productHash:    | md5(vendorName + region + sku);
 * sku:            | service - plan_id
 * vendorName:     | 'ibm'
 * region:         | PricingMetaData.region || country
 * service:        | serviceName
 * productFamily:  | 'service'
 * attributes:     | ibmAttributes
 * prices:         | Price[]
 *
 * @param services
 * @returns
 */
function parseProducts(services: CatalogEntry[]): Product[] {
  const products: Product[] = [];
  // for now, only grab USA, USD pricing
  const country = 'USA';
  const currency = 'USD';
  for (const service of services) {
    if (service.children) {
      for (const plan of service.children) {
        if (plan.children) {
          for (const deployment of plan.children) {
            if (deployment.pricingChildren) {
              for (const pricing of deployment.pricingChildren) {
                const processedPricing = parsePricingJson(pricing);
                if (!processedPricing) {
                  continue;
                }
                const prices = getPrices(processedPricing, country, currency);
                const attributes = getAttributes(processedPricing, plan.name);
                const region = attributes?.region || country;

                if (prices?.length) {
                  const p = {
                    productHash: ``,
                    sku: `${service.name}-${plan.name}`,
                    vendorName: 'ibm',
                    region,
                    service: service.name,
                    productFamily: 'service',
                    attributes,
                    prices,
                  };
                  p.productHash = generateProductHash(p);
                  products.push(p);
                }
              }
            }
          }
        }
      }
    }
  }
  return products;
}

async function getCatalogEntries(
  globalCatalog: GlobalCatalogV1
): Promise<GlobalCatalogV1.CatalogEntry[]> {
  const limit = 200;
  let offset = 0;
  let next = true;
  const servicesArray = [];

  while (next) {
    const response = await globalCatalog.listCatalogEntries({
      q: 'kind:service active:true',
      include: 'id:geo_tags:kind:name:pricing_tags',
      account: 'global',
      limit,
      offset,
    });
    if (response.result?.resources?.length) {
      servicesArray.push(...response.result.resources);
    }
    if (!response.result.count || offset > response.result.count) {
      next = false;
    }
    offset += limit;
  }
  return servicesArray;
}

async function scrape(): Promise<void> {
  config.logger.info(`Started IBM Cloud scraping at ${new Date()}`);
  const results: CatalogEntry[] = [];
  const serviceEntries = await getCatalogEntries(client);
  for (const service of serviceEntries) {
    config.logger.info(`Scraping pricing for ${service.name}`);
    const serviceEntryTree = (
      await client.getCatalogEntry({
        id: service.id as string,
        depth: 3,
        include: 'children:kind:tags:geo_tags:pricing_tags:name',
      })
    ).result as CatalogEntry;
    if (serviceEntryTree.children) {
      for (const plan of serviceEntryTree.children) {
        if (plan.children) {
          const chunks = _.chunk(plan.children, 5);
          for (const deployments of chunks) {
            await Promise.all(
              deployments.map(async (deployment): Promise<void> => {
                try {
                  const pricingObject = (
                    await client.getChildObjects({
                      id: deployment.id as string,
                      kind: 'pricing',
                    })
                  ).result as PricingGet;
                  if (!pricingObject) {
                    return;
                  }
                  // eslint-disable-next-line no-param-reassign
                  deployment.pricingChildren = [pricingObject];
                } catch (e) {
                  config.logger.info(e?.message);
                }
              })
            );
          }
        }
      }
    }
    results.push(serviceEntryTree);
  }
  await writeFile(filename, JSON.stringify(results, null, 2));
  const products = parseProducts(results);
  await writeFile('products2.json', JSON.stringify(products, null, 2));
  await upsertProducts(products);
  config.logger.info(`Ended IBM Cloud scraping at ${new Date()}`);
}

export default {
  scrape,
};
