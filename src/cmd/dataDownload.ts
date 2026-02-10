import axios, { AxiosResponse } from 'axios';
import ProgressBar from 'progress';
import fs from 'fs';
import yargs from 'yargs';
import config from '../config';

async function run() {
  config.logger.error('Data download functionality has been removed.');
  config.logger.error(
    'Please use alternative methods to populate your database with pricing data.'
  );
  process.exit(1);
}

config.logger.info('Starting: downloading DB data');
run()
  .then(() => {
    config.logger.info('Completed: downloading DB data');
    process.exit(0);
  })
  .catch((err) => {
    config.logger.error(err);
    process.exit(1);
  });
