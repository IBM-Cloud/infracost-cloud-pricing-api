import express, { Application, Request, Response, NextFunction } from 'express';
import { ApolloServer, ApolloServerOptions, BaseContext } from '@apollo/server';
import { expressMiddleware } from '@as-integrations/express4';
import { ApolloServerPluginLandingPageDisabled } from '@apollo/server/plugin/disabled';
import { ApolloServerPluginCacheControl } from '@apollo/server/plugin/cacheControl';
import { GraphQLError, GraphQLFormattedError } from 'graphql';
import { makeExecutableSchema } from '@graphql-tools/schema';
import * as log4js from 'log4js';
import cors from 'cors';
import config from './config';
import ApolloLogger from './utils/apolloLogger';
import getResolvers from './resolvers';
import typeDefs from './typeDefs';
import health from './health';
import { Product } from './db/types';
import { ErrorHandler } from './utils/errorHandler';

// Initialize error handler
const errorHandler = new ErrorHandler({
  exitOnUncaught: false
});

errorHandler.init();

export type ApplicationOptions<TContext> = {
  apolloConfigOverrides?: ApolloServer;
  disableRequestLogging?: boolean;
  disableStats?: boolean;
  disableAuth?: boolean;
  logger?: log4js.Logger;
  convertProducts?(context: TContext, products: Product[]): Promise<Product[]>;
};

interface ResponseError extends Error {
  status?: number;
}

let logId: number = 0;

async function createApp<TContext>(
  opts: ApplicationOptions<TContext> = {}
): Promise<Application> {
  const app = express();

  app.get('/liveness', (req, res) => {
    res.sendStatus(200);
  });

  app.get('/readiness', (req, res) => {
    res.sendStatus(200);
  });

  const logger = opts.logger || config.logger;

  app.use(express.json());
  app.use(
    (err: ResponseError, _req: Request, res: Response, next: NextFunction) => {
      if (err instanceof SyntaxError && err.status === 400) {
        res.status(400).send({ error: 'Bad request' });
      } else {
        next();
      }
    }
  );

  if (!opts.disableRequestLogging) {
    app.use((req: Request, _res: Response, next: NextFunction) => {
      if (!['/health', '/graphql'].includes(req.path)) {
        logger.debug({ body: req.body });
      }
      next();
    });
  }

  app.use(health);

    // Big query objects with large keys or too many fields could trip this check
    app.use((req: Request, res: Response, next: NextFunction) => {
      const query = req.query.query || req.body.query || '';
      if (query.length > 2000) {
        res.status(400).json({
          status: 'error',
          message: 'Query too large'
        })
      } else {
        next();
      }
    });

  const errorFormatter = (formattedError: GraphQLFormattedError, err: unknown): GraphQLFormattedError => {
    const resp: GraphQLFormattedError = {
      message: formattedError.message
    };
    if (formattedError.extensions?.code === 'GRAPHQL_VALIDATION_FAILED') {
      throw new GraphQLError("Invalid Request", {
        extensions: {
          code: 'GRAPHQL_VALIDATION_FAILED'
        }
      })
    }
    return resp;
  };
  
  const apolloConfig: ApolloServerOptions<BaseContext> = {
    schema: makeExecutableSchema({
      typeDefs,
      resolvers: getResolvers<TContext>(opts),
    }),
    introspection: false,
    plugins: [
      ApolloServerPluginLandingPageDisabled(),
      ApolloServerPluginCacheControl({
        // Set a default maxAge for all fields that don't have explicit hints
        defaultMaxAge: 600,
      }),
    ],
    allowBatchedHttpRequests: true,
    formatError: errorFormatter,
    ...opts.apolloConfigOverrides,
  };

  const apollo = new ApolloServer(apolloConfig);
  apollo.addPlugin(new ApolloLogger(logger));
  await apollo.start();

  app.use(log4js.connectLogger(logger, {
    context: true,
    format: (req, res, format) => {
      logId++
      return format(`{id:${logId}, txid:${req.headers["x-transaction-id"]}, remoteAddr: ":remote-addr", method: :method, url: :url, statusCode: :status, content-length: :content-length, responseTime: ${res.responseTime}, user-agent: ":user-agent"}`)
    }
  }))

  app.use(
    '/graphql',
    cors<cors.CorsRequest>(),
    express.json(),
    expressMiddleware(apollo)
  );

  return app;
}

export default createApp;