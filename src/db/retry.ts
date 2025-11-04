import config from '../config';

/**
 * PostgreSQL error codes that should trigger a retry
 * Based on: https://www.postgresql.org/docs/current/errcodes-appendix.html
 */
const RETRYABLE_ERROR_CODES = new Set([
  // Connection errors
  '08000', // connection_exception
  '08003', // connection_does_not_exist
  '08006', // connection_failure
  '08001', // sqlclient_unable_to_establish_sqlconnection
  '08004', // sqlserver_rejected_establishment_of_sqlconnection
  '08007', // transaction_resolution_unknown
  '08P01', // protocol_violation
  
  // System errors
  '53000', // insufficient_resources
  '53100', // disk_full
  '53200', // out_of_memory
  '53300', // too_many_connections
  '53400', // configuration_limit_exceeded
  
  // Operator intervention
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now
  
  // Transaction errors that might be transient
  '40001', // serialization_failure
  '40P01', // deadlock_detected
]);

/**
 * Error messages that indicate retryable conditions
 */
const RETRYABLE_ERROR_MESSAGES = [
  'connection terminated',
  'connection refused',
  'connection timeout',
  'connection reset',
  'connection closed',
  'server closed the connection',
  'connection lost',
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'ENOTFOUND',
  'ENETUNREACH',
  'EHOSTUNREACH',
  'socket hang up',
  'network error',
  'Client has encountered a connection error',
];

export interface RetryOptions {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  onRetry?: (error: Error, attempt: number, delayMs: number) => void;
}

export interface RetryableError extends Error {
  code?: string;
  errno?: string;
  syscall?: string;
}

/**
 * Determines if an error is retryable based on error code or message
 */
export function isRetryableError(error: RetryableError): boolean {
  // Check PostgreSQL error codes
  if (error.code && RETRYABLE_ERROR_CODES.has(error.code)) {
    return true;
  }

  // Check error message for retryable patterns
  const errorMessage = error.message?.toLowerCase() || '';
  return RETRYABLE_ERROR_MESSAGES.some(pattern => 
    errorMessage.includes(pattern.toLowerCase())
  );
}

/**
 * Calculate delay for exponential backoff with the specified pattern:
 * 1s, 2s, 4s, 8s, 16s, 20s, 20s, 20s, 20s, 20s
 */
export function calculateBackoffDelay(attempt: number, baseDelayMs: number = 1000, maxDelayMs: number = 20000): number {
  if (attempt <= 0) return baseDelayMs;
  
  // Exponential backoff: 2^attempt * baseDelay
  const exponentialDelay = (2 ** (attempt - 1)) * baseDelayMs;
  
  // Cap at maxDelay (60 seconds)
  return Math.min(exponentialDelay, maxDelayMs);
}

/**
 * Sleep for specified milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

/**
 * Retry a database operation with exponential backoff
 * 
 * @param operation - The async function to retry
 * @param options - Retry configuration options
 * @returns The result of the operation
 * @throws The last error if all retries are exhausted
 */
export async function retryDatabaseOperation<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const {
    maxRetries = 10,
    baseDelayMs = 1000,
    maxDelayMs = 20000,
    onRetry,
  } = options;

  let lastError: Error | undefined;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      // Attempt the operation
      const result = await operation();
      
      // Log success if this was a retry
      if (attempt > 0) {
        config.logger.info({
          message: 'Database operation succeeded after retry',
          attempt,
          totalAttempts: attempt + 1,
        });
      }
      
      return result;
    } catch (error) {
      lastError = error as Error;
      const retryableError = error as RetryableError;
      
      // Check if we should retry
      const shouldRetry = isRetryableError(retryableError);
      const isLastAttempt = attempt === maxRetries;
      
      if (!shouldRetry || isLastAttempt) {
        // Log final failure
        config.logger.error({
          message: 'Database operation failed',
          error: retryableError.message,
          errorCode: retryableError.code,
          attempt: attempt + 1,
          totalAttempts: attempt + 1,
          retryable: shouldRetry,
          reason: isLastAttempt ? 'Max retries exhausted' : 'Non-retryable error',
        });
        throw error;
      }
      
      // Calculate delay for next retry
      const delayMs = calculateBackoffDelay(attempt + 1, baseDelayMs, maxDelayMs);
      
      // Log retry attempt
      config.logger.warn({
        message: 'Database operation failed, retrying',
        error: retryableError.message,
        errorCode: retryableError.code,
        attempt: attempt + 1,
        maxRetries,
        nextRetryInMs: delayMs,
        nextRetryInSeconds: (delayMs / 1000).toFixed(1),
      });
      
      // Call custom retry callback if provided
      if (onRetry) {
        onRetry(retryableError, attempt + 1, delayMs);
      }
      
      // Wait before retrying
      await sleep(delayMs);
    }
  }
  
  // This should never be reached, but TypeScript needs it
  throw lastError || new Error('Unknown error in retry logic');
}

/**
 * Wrapper for pool.query with retry logic
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
export async function queryWithRetry<T = any>(
  pool: any,
  sql: string,
  values?: any[]
): Promise<T> {
  return retryDatabaseOperation(async () => {
    if (values) {
      return pool.query(sql, values);
    }
    return pool.query(sql);
  });
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// Made with Bob
