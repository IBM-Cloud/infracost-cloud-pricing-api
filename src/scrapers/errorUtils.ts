import axios from 'axios';
import config from '../config';

export enum ErrorSeverity {
  FATAL = 'fatal',     // Exit immediately - auth failures, critical errors
  RETRY = 'retry',     // Retry with backoff - rate limits, server errors
  SKIP = 'skip',       // Log and continue - 404s, expected failures
}

export function classifyHttpError(error: unknown, context: string): ErrorSeverity {
  if (!axios.isAxiosError(error)) {
    config.logger.error(`Non-HTTP error in ${context}: ${error}`);
    return ErrorSeverity.SKIP;
  }
  
  const status = error.response?.status;
  
  // Authentication failures are fatal
  if (status === 401 || status === 403) {
    config.logger.error(`Authentication failed in ${context}`);
    return ErrorSeverity.FATAL;
  }
  
  // Rate limiting and server errors should retry
  if (status === 429 || (status && status >= 500)) {
    config.logger.warn(`Retryable error ${status} in ${context}`);
    return ErrorSeverity.RETRY;
  }
  
  // Network errors are fatal (can't proceed without network)
  if (['ECONNREFUSED', 'ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH'].includes(error.code || '')) {
    config.logger.error(`Network error in ${context}: ${error.code}`);
    return ErrorSeverity.FATAL;
  }
  
  // Not found is expected for some items
  if (status === 404) {
    config.logger.debug(`Not found in ${context} (expected)`);
    return ErrorSeverity.SKIP;
  }
  
  // Other errors - log and skip
  config.logger.error(`Unexpected error in ${context}: ${error.message}`);
  return ErrorSeverity.SKIP;
}

export async function retryOperation<T>(
  operation: () => Promise<T>,
  options: {
    maxRetries: number;
    shouldRetry: (error: any) => boolean;
    onRetry?: (attempt: number, delay: number) => void;
  }
): Promise<T> {
  let lastError: any;
  
  for (let attempt = 0; attempt <= options.maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      
      if (attempt === options.maxRetries || !options.shouldRetry(error)) {
        throw error;
      }
      
      const delay = Math.min(1000 * Math.pow(2, attempt), 30000);
      options.onRetry?.(attempt + 1, delay);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw lastError;
}

export function createProgressTracker(total: number, name: string) {
  let completed = 0;
  return {
    increment() {
      completed++;
      if (completed % 10 === 0 || completed === total) {
        const percent = Math.round((completed / total) * 100);
        config.logger.info(`${name} progress: ${completed}/${total} (${percent}%)`);
      }
    },
    getCompleted() {
      return completed;
    }
  };
}