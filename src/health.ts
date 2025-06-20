import express from 'express';
import config from './config';

const router = express.Router();

router.get('/health', async (_req, res) => {
  const status = {
      status: 'N/A',
      region: config.region,
      hostname: config.hostname,
      version: config.version
  };
  try {
    const pool = await config.pg();
    await pool.query('SELECT 1');
  } catch (err) {
    config.logger.error(`Could not connect to database: ${err}`);
    status.status = 'failed';
    res.status(500).send(status);
    return;
  }
  status.status = 'success';
  res.status(200).send(status);
});

export default router;
