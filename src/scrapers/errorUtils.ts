import axios from 'axios';
import config from '../config';

export enum ErrorSeverity {
  FATAL = 'fatal',     // Exit immediately - auth failures, critical errors
  RETRY = 'retry',     // Retry with backoff - rate limits, server errors
  SKIP = 'skip',       // Log and continue - 404s, expected failures
}

export function httpErrorSeverity(error: unknown): ErrorSeverity {
  if (!axios.isAxiosError(error)) return ErrorSeverity.SKIP;
  const status = error.response?.status;
  if (status === 401 || status === 403) return ErrorSeverity.FATAL;
  if (status === 429 || (status && status >= 500)) return ErrorSeverity.RETRY;
  if (['ECONNREFUSED', 'ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH'].includes(error.code || '')) return ErrorSeverity.FATAL;
  return ErrorSeverity.SKIP;
}

export function classifyHttpError(error: unknown, context: string): ErrorSeverity {
  const severity = httpErrorSeverity(error);

  if (!axios.isAxiosError(error)) {
    config.logger.error(`Non-HTTP error in ${context}: ${error}`);
    return severity;
  }

  const status = (error as any).response?.status;

  switch (severity) {
    case ErrorSeverity.FATAL:
      if (status === 401 || status === 403) {
        config.logger.error(`Authentication failed in ${context}`);
      } else {
        config.logger.error(`Network error in ${context}: ${(error as any).code}`);
      }
      break;
    case ErrorSeverity.RETRY:
      config.logger.warn(`Retryable error ${status} in ${context}`);
      break;
    case ErrorSeverity.SKIP:
      if (status === 404) {
        config.logger.debug(`Not found in ${context} (expected)`);
      } else {
        config.logger.error(`Unexpected error in ${context}: ${(error as any).message}`);
      }
      break;
  }

  return severity;
}

export function retryAfterDelay(error: unknown): number | null {
  if (!axios.isAxiosError(error)) return null;
  if (error.response?.status !== 429) return null;
  const header = error.response.headers?.['retry-after'];
  if (!header) return null;
  const seconds = Number(header);
  if (!Number.isNaN(seconds) && seconds > 0) return seconds * 1000;
  const date = Date.parse(header);
  if (!Number.isNaN(date)) return Math.max(0, date - Date.now());
  return null;
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
      
      // Honour Retry-After for 429s; otherwise exponential backoff starting at 5s
      const retryAfter = retryAfterDelay(error);
      const delay = retryAfter ?? Math.min(5000 * Math.pow(2, attempt), 60000);
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