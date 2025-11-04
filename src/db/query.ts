// In order to make upserting more efficient, prices in postgres are stored as a map of priceHash -> prices.
import format from 'pg-format';
import { Price, Product, ProductAttributes } from './types';
import config from '../config';
import { retryDatabaseOperation } from './retry';

type AttributeFilter = {
  key: string;
  value?: string;
  // eslint-disable-next-line camelcase
  value_regex?: string;
};

type ProductWithPriceMap = {
  productHash: string;
  sku: string;
  vendorName: string;
  region: string | null;
  service: string;
  productFamily: string;
  attributes: ProductAttributes;
  prices: { [priceHash: string]: Price[] };
};

function flattenPrices(p: ProductWithPriceMap): Product {
  return { ...p, prices: Object.values(p.prices).flat() };
}

// eslint-disable-next-line import/prefer-default-export
export async function findProducts(
  filters: { [key: string]: string | string[] },
  attributeFilters: AttributeFilter[],
  limit?: number
): Promise<Product[]> {
  try {
    const pool = await config.pg();

    const where: string[] = [];
    Object.entries(filters).forEach((filterItem) => {
      where.push(filterToCondition(filterItem[0], filterItem[1]));
    });
  
    if (attributeFilters) {
      attributeFilters.forEach((f) => {
        where.push(attributeFilterToCondition(f));
      });
    }
  
    let sql = format(
      `SELECT * FROM %I WHERE ${where.join(' AND ')}`,
      config.productTableName
    );
    if (limit !== undefined) {
      sql = format(`${sql} LIMIT %L`, limit);
    }
  
    const response = await retryDatabaseOperation(async () => pool.query(sql));
    const products = response?.rows as ProductWithPriceMap[];
    return products.map((product) => flattenPrices(product));
  } catch (err) {
    config.logger.error(`Error trying to query for product ${err}`);
    return [];
  }
}

function strToRegex(str: string): RegExp {
  const pattern = (str.match(/\/(.+)\/.*/) || [''])[1];
  const options = (str.match(/\/.+\/(.*)/) || [undefined])[1];
  return new RegExp(pattern, options);
}

function filterToCondition(keyPart: string, value: string | string[]): string {
  const [key, opPart] = keyPart.split('_');
  if (Array.isArray(value)) {
    return format('%I IN (%L)', key, value);
  }
  if (opPart === 'regex') {
    const regex = strToRegex(value);
    return format(`%I ${regex.ignoreCase ? '~*' : '~'} %L`, key, regex.source);
  }
  if (value === '') {
    return format("(%I = '' OR %I IS NULL)", key, key, value);
  }

  return format('%I = %L', key, value);
}

function attributeFilterToCondition(filter: AttributeFilter) {
  if (filter.value_regex) {
    const regex = strToRegex(filter.value_regex);
    return format(
      `attributes ->> %L ${regex.ignoreCase ? '~*' : '~'} %L`,
      filter.key,
      regex.source
    );
  }
  if (filter.value === '') {
    return format(
      "(attributes -> %L IS NULL OR attributes ->> %L = '')",
      filter.key,
      filter.key
    );
  }
  return format('attributes ->> %L = %L', filter.key, filter.value);
}
