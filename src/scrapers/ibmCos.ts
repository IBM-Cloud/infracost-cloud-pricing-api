import axios from 'axios';
import { URLSearchParams } from 'url';

import config from '../config';
import type { Product, Price } from '../db/types';
import { upsertProducts } from '../db/upsert';
import { generatePriceHash, generateProductHash } from '../db/helpers';
import { classifyHttpError, ErrorSeverity, retryOperation } from './errorUtils';

type Plan = { name: string; id: string; regions: string[] };

const baseURL = 'https://cloud.ibm.com/objectstorage/provision/api/v1';

const standardRegions = [
  // Cross region
  'ap',
  'eu',
  'us',
  // Region
  'au-syd',
  'br-sao',
  'ca-tor',
  'eu-de',
  'eu-gb',
  'eu-es',
  'jp-osa',
  'jp-tok',
  'us-east',
  'us-south',
  // Locations
  'ams03',
  'che01',
  'mil01',
  'mon01',
  'par01',
  'sjc04',
  'sng01',
];

const oneRateRegions = ['na', 'eu', 'sa', 'ap'];

const plans: Plan[] = [
  {
    name: 'standard',
    id: '744bfc56-d12c-4866-88d5-dac9139e0e5d',
    regions: standardRegions,
  },
  {
    name: 'cos-one-rate-plan',
    id: '1e4e33e4-cfa6-4f12-9016-be594a6d5f87',
    regions: oneRateRegions,
  },
  {
    name: 'cos-satellite-12tb-plan',
    id: 'e7da9262-3988-465b-b66c-c9e3985227f7',
    regions: standardRegions,
  },
  {
    name: 'cos-satellite-24tb-plan',
    id: '1106e13f-da56-4bb2-8a80-4d74712f89f1',
    regions: standardRegions,
  },
  {
    name: 'cos-satellite-48tb-plan',
    id: '32a58321-b3c4-44d7-a2e4-0c65a7a10400',
    regions: standardRegions,
  },
  {
    name: 'cos-satellite-96tb-plan',
    id: '36bfef2b-8bfc-409e-8718-ce73fe7ca2d2',
    regions: standardRegions,
  }
];

export interface PricingData {
  country: string;
  currency: string;
  prices: Prices;
}

export interface Prices {
  [category: string]: PriceCategory;
}

export interface PriceCategory {
  [subCategory: string]: Tiers[];
}

export interface Tiers {
  price: number;
  quantity_tier: number;
}

const productTransformer = (
  priceResponse: PricingData,
  region: string,
  plan: Plan
): Product => {
  const product: Product = {
    vendorName: 'ibm',
    productFamily: 'iaas',
    service: 'cloud-object-storage',
    region,
    productHash: '',
    sku: `cloud-object-storage-${plan.name}-${region}`,
    attributes: {
      region,
      planName: plan.name,
    },
    prices: [],
  };
  product.productHash = generateProductHash(product);
  for (const [categoryName, categoryPrices] of Object.entries(
    priceResponse.prices
  )) {
    for (const [subCategoryName, subCategoryPrices] of Object.entries(
      categoryPrices
    )) {
      for (const [i, tier] of Object.entries(subCategoryPrices)) {
        const currentIndex = Number(i);
        const prevIndex = Number(currentIndex - 1);
        const prevTier = subCategoryPrices[prevIndex];
        const price: Price = {
          unit: `${categoryName.toUpperCase()}_${subCategoryName.toUpperCase()}`,
          priceHash: '',
          country: priceResponse.country,
          purchaseOption: '1',
          currency: priceResponse.currency,
          startUsageAmount: prevTier?.quantity_tier
            ? String(prevTier.quantity_tier)
            : '0',
          endUsageAmount: String(tier.quantity_tier),
          effectiveDateStart: '1970-01-01T00:00:00.000Z',
          effectiveDateEnd: undefined,
          USD: String(tier.price),
        };
        price.priceHash = generatePriceHash(product, price);
        product.prices.push(price);
      }
    }
  }
  return product;
};

const scrape = async () => {
  const axiosClient = axios.create({
    baseURL,
    timeout: 30000, // 30 second timeout
  });

  const products: Product[] = [];
  const MAX_RETRIES = 3;

  for (const plan of plans) {
    for (const region of plan.regions) {
      const searchParams = new URLSearchParams();
      searchParams.append('plan', plan.id);
      searchParams.append('region', region);
      searchParams.append('country', 'USA');
      
      const operation = async () => {
        config.logger.info(`Fetching COS pricing: ${plan.name} in ${region}`);
        const { data } = await axiosClient.get<PricingData>('/pricing', {
          params: searchParams,
        });
        
        // Validate response
        if (!data || !data.prices || Object.keys(data.prices).length === 0) {
          config.logger.warn(`Empty pricing data for ${plan.name} in ${region}`);
          return null;
        }
        
        return data;
      };

      try {
        const data = await retryOperation(operation, {
          maxRetries: MAX_RETRIES,
          shouldRetry: (error) => {
            const severity = classifyHttpError(error, `COS ${plan.name} in ${region}`);
            return severity === ErrorSeverity.RETRY;
          },
          onRetry: (attempt, delay) => {
            config.logger.info(`Retrying COS ${plan.name} in ${region} (attempt ${attempt}/${MAX_RETRIES}, delay ${delay}ms)`);
          }
        });
        
        if (data) {
          const product = productTransformer(data, region, plan);
          products.push(product);
        }
      } catch (e) {
        const severity = classifyHttpError(e, `COS ${plan.name} in ${region}`);
        if (severity === ErrorSeverity.FATAL) {
          throw e;
        }
        // Skip this combination for non-fatal errors
      }
    }
  }

  // Validate results before upserting
  if (products.length === 0) {
    throw new Error('No COS products retrieved - scraper may have failed completely');
  }

  config.logger.info(`Successfully scraped ${products.length} COS products`);

  try {
    await upsertProducts(products);
    config.logger.info(`Upserted ${products.length} COS products to database`);
  } catch (e: any) {
    config.logger.error(`Failed to upsert COS products: ${e.message}`);
    config.logger.error(e.stack);
    throw e; // Fatal - don't continue if DB write fails
  }
};

export default {
  scrape,
};
