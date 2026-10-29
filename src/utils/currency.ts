import axios from 'axios';
import { Mutex } from 'async-mutex';
import Decimal from 'decimal.js';
import config from '../config';

type RateResp = {
  rates: { [code: string]: number };
  base: string;
  date: string;
};

const currencyTTL = 24 * 60 * 60;

const mutex = new Mutex();

const currencyFallbacks: { [from: string]: { [to: string]: number } } = {
  CNY: { USD: 0.154 },
  USD: {
    AED: 3.670007,
    AFN: 86.068399,
    ALL: 103.059173,
    AMD: 492.106215,
    ANG: 1.793583,
    AOA: 637.165361,
    ARS: 97.052591,
    AUD: 1.377149,
    AWG: 1.799044,
    AZN: 1.699346,
    BAM: 1.661469,
    BBD: 1.998398,
    BDT: 84.869323,
    BGN: 1.668453,
    BHD: 0.376919,
    BIF: 1981.326128,
    BMD: 0.999547,
    BND: 1.357425,
    BOB: 6.899782,
    BRL: 5.291748,
    BSD: 0.999837,
    BTN: 74.199087,
    BWP: 11.138747,
    BYN: 2.49489,
    BZD: 2.014644,
    CAD: 1.26193,
    CDF: 1984.055429,
    CHF: 0.914847,
    CLF: 0.029178,
    CLP: 789.240698,
    CNY: 6.493506,
    COP: 3858.578223,
    CRC: 619.599679,
    CUC: 0.999519,
    CUP: 25.726427,
    CVE: 93.671202,
    CZK: 21.724916,
    DJF: 177.875361,
    DKK: 6.347616,
    DOP: 56.971699,
    DZD: 135.270223,
    EGP: 15.69734,
    ERN: 14.991279,
    ETB: 45.106684,
    EUR: 0.853994,
    FJD: 2.095874,
    FKP: 0.727939,
    GBP: 0.727448,
    GEL: 3.100408,
    GGP: 0.727533,
    GHS: 6.020056,
    GIP: 0.727858,
    GMD: 51.103024,
    GNF: 9755.764983,
    GTQ: 7.738676,
    GYD: 208.852596,
    HKD: 7.784545,
    HNL: 23.719733,
    HRK: 6.393249,
    HTG: 96.419114,
    HUF: 300.026284,
    IDR: 14411.815341,
    ILS: 3.234321,
    IMP: 0.727274,
    INR: 74.296314,
    IQD: 1457.776185,
    IRR: 42113.532418,
    ISK: 126.593483,
    JEP: 0.727873,
    JMD: 154.370424,
    JOD: 0.708708,
    JPY: 109.49588,
    KES: 109.32443,
    KGS: 84.634674,
    KHR: 4074.05535,
    KMF: 415.666203,
    KPW: 899.168921,
    KRW: 1176.868979,
    KWD: 0.300553,
    KYD: 0.833672,
    KZT: 425.190845,
    LAK: 9578.000732,
    LBP: 1510.686397,
    LKR: 199.332339,
    LRD: 171.467098,
    LSL: 14.803635,
    LYD: 4.51441,
    MAD: 8.962616,
    MDL: 17.531325,
    MKD: 52.319092,
    MMK: 1644.614639,
    MNT: 2848.729366,
    MOP: 8.020266,
    MUR: 42.412805,
    MVR: 15.446493,
    MWK: 811.822505,
    MXN: 19.986613,
    MYR: 4.234786,
    MZN: 63.62144,
    NAD: 14.357187,
    NGN: 411.518031,
    NIO: 35.086462,
    NOK: 8.916718,
    NPR: 118.718479,
    NZD: 1.443073,
    OMR: 0.385146,
    PAB: 0.999635,
    PEN: 4.079734,
    PGK: 3.508118,
    PHP: 50.366842,
    PKR: 164.376897,
    PLN: 3.895594,
    PYG: 6912.18743,
    QAR: 3.696535,
    RON: 4.203613,
    RSD: 99.839742,
    RUB: 73.584549,
    RWF: 1007.27335,
    SAR: 3.746805,
    SBD: 8.044291,
    SCR: 13.180352,
    SDG: 446.087911,
    SEK: 8.754304,
    SGD: 1.361594,
    SHP: 0.727845,
    SLL: 10243.522165,
    SOS: 578.003538,
    SRD: 21.373303,
    SSP: 130.1404,
    STD: 20695.191392,
    SVC: 8.74299,
    SYP: 1256.47597,
    SZL: 14.80374,
    THB: 33.33528,
    TJS: 11.401408,
    TMT: 3.497068,
    TND: 2.785394,
    TOP: 2.256142,
    TRY: 8.425596,
    TTD: 6.789093,
    TWD: 27.8924,
    TZS: 2317.651295,
    UAH: 26.636606,
    UGX: 3529.012076,
    UYU: 43.246205,
    UZS: 10643.293155,
    VND: 22930.194953,
    VUV: 110.91287,
    WST: 2.560348,
    XAF: 559.836881,
    XAG: 0.042256,
    XAU: 0.001018,
    XCD: 2.700536,
    XDR: 0.703033,
    XPF: 101.846424,
    YER: 249.769237,
    ZAR: 14.907877,
    ZMW: 19.129442,
  },
};

export const CURRENCY_CODES = Object.keys(currencyFallbacks.USD);

async function convert(
  from: string,
  to: string,
  amount: number
): Promise<number> {
  const rate = await getRate(from, to);

  return new Decimal(amount).times(rate).toDecimalPlaces(10).toNumber();
}

async function getRate(from: string, to: string): Promise<Decimal> {
  const cacheKey = `currency-${from}-${to}`;

  // Use a mutex so we only query the API once
  const release = await mutex.acquire();

  try {
    let rate = config.cache.get<number>(`currency-${from}-${to}`);
    if (rate !== undefined) {
      return new Decimal(rate);
    }

    rate = await queryRate(from, to);
    if (rate !== undefined) {
      config.logger.debug(
        `Saving ${cacheKey}=${rate} to cache with TTL ${currencyTTL}`
      );
      const ok = config.cache.set(`currency-${from}-${to}`, rate, currencyTTL);
      if (!ok) {
        config.logger.warn('Could not store exchange rate in cache');
      }

      return new Decimal(rate);
    }

    config.logger.warn('No exchange rate found, falling back to default rate');
    return new Decimal(currencyFallbacks[from][to]);
  } finally {
    release();
  }
}

async function queryRate(
  from: string,
  to: string
): Promise<number | undefined> {
  const url = `https://api.exchangerate.host/latest?base=${from}&symbols=${to}`;

  try {
    config.logger.debug(`Querying exchange rate from: ${url}`);
    const resp = await axios({
      method: 'get',
      url,
    });

    return (<RateResp>resp.data).rates[to];
  } catch (err: any) {
    config.logger.error('Error querying exchange rate:');
    config.logger.error(err);
    return undefined;
  }
}

export default {
  convert,
};
