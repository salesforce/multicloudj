package com.salesforce.multicloudj.sts.gcp;

import com.google.api.client.http.HttpResponseException;
import com.google.auth.Retryable;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.common.retries.RetryExecutor;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.function.LongSupplier;
import org.apache.http.NoHttpResponseException;

/**
 * Re-runs a token operation according to a {@link RetryConfig}. The google-auth library retries
 * token refreshes a fixed number of times internally with no way to configure it, so this applies
 * the caller's attempts, backoff and total timeout around each operation through a {@link
 * RetryExecutor}.
 *
 * <p>Only transient failures are retried: errors the auth library marks retryable, HTTP 408, 429
 * and 5xx responses, and connection-level I/O failures. Exponential delays get equal jitter so
 * clients that failed together, such as every pod hitting the same token endpoint outage, do not
 * retry in lockstep.
 */
final class GcpStsRetrier {
  private static final int MAX_CAUSE_DEPTH = 5;

  private final RetryConfig retryConfig;
  private final RetryExecutor retryExecutor;

  GcpStsRetrier(RetryConfig retryConfig) {
    this(retryConfig, new RetryExecutor(retryConfig, RetryExecutor::equalJitter));
  }

  GcpStsRetrier(RetryConfig retryConfig, RetryExecutor.Sleeper sleeper, LongSupplier clockMillis) {
    this(
        retryConfig,
        new RetryExecutor(retryConfig, sleeper, clockMillis, RetryExecutor::equalJitter));
  }

  private GcpStsRetrier(RetryConfig retryConfig, RetryExecutor retryExecutor) {
    this.retryConfig = retryConfig;
    this.retryExecutor = retryExecutor;
  }

  boolean isEnabled() {
    return retryConfig != null;
  }

  <T> T execute(RetryExecutor.Attempt<T> operation) throws IOException {
    return retryExecutor.execute(operation, GcpStsRetrier::isRetryable);
  }

  static boolean isRetryable(Throwable error) {
    Throwable current = error;
    for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
      // google-auth marks only some token endpoint statuses retryable (not 502 or 504), so a
      // non-retryable verdict falls through to classify the HTTP response it wraps.
      if (current instanceof Retryable && ((Retryable) current).isRetryable()) {
        return true;
      }
      if (current instanceof HttpResponseException) {
        int status = ((HttpResponseException) current).getStatusCode();
        return status == 408 || status == 429 || status >= 500;
      }
      if (current instanceof SocketTimeoutException
          || current instanceof SocketException
          || current instanceof NoHttpResponseException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
