import createApp from './app';
import config from './config';

createApp().then((app) => {
  app.listen(config.port, () => {
    config.logger.info(`🚀  Server ready at http://0.0.0.0:${config.port}/`);
    config.logger.info(`Running version: ${config.version}`)
  });
});
