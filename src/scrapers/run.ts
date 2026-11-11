import yargs from 'yargs';
import config from '../config';
import awsBulk from './awsBulk';
import awsSpot from './awsSpot';
import azureRetail from './azureRetail';
import gcpCatalog from './gcpCatalog';
import gcpMachineTypes from './gcpMachineTypes';
import ibmKubernetes from './ibmKubernetes';
import ibmCatalog from './ibmCatalog';
import ibmCos from './ibmCos';

interface ScraperConfig {
  vendor: string;
  source: string;
  scraperFunc: () => void;
}

const Scrapers = {
  aws: {
    bulk: awsBulk.scrape,
    spot: awsSpot.scrape,
  },
  azure: {
    retail: azureRetail.scrape,
  },
  gcp: {
    catalog: gcpCatalog.scrape,
    machineTypes: gcpMachineTypes.scrape,
  },
  ibm: {
    kubernetes: ibmKubernetes.scrape,
    catalog: ibmCatalog.scrape,
    cos: ibmCos.scrape,
  }
};

async function run(): Promise<void> {
  const argv = await yargs
    .usage(
      'Usage: $0 --only=[aws:bulk,aws:spot,azure:retail,gcp:catalog,gcp:machineTypes,ibm:kubernetes,ibm:catalog,ibm:cos]'
    )
    .options({
      only: { type: 'string' },
    }).argv;

  const scraperConfigs: ScraperConfig[] = [];

  Object.entries(Scrapers).forEach((scraperEntry) => {
    const [vendor, vendorScrapers] = scraperEntry;
    Object.entries(vendorScrapers).forEach((vendorScraperEntry) => {
      const [source, scraperFunc] = vendorScraperEntry;

      if (
        !argv.only ||
        (argv.only && argv.only.split(',').includes(`${vendor}:${source}`))
      ) {
        scraperConfigs.push({
          vendor,
          source,
          scraperFunc,
        });
      }
    });
  });

  for (const scraperConfig of scraperConfigs) {
    config.logger.info(
      `Running update function for ${scraperConfig.vendor}:${scraperConfig.source}`
    );
    await scraperConfig.scraperFunc();
  }
}

export default {
  run,
};
