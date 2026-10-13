package com.salesforce.multicloudj.sts.gcp;

import com.google.api.client.http.HttpResponseException;
import com.google.auth.Retryable;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.http.NoHttpResponseException;

/**
 * Re-runs a token operation according to a {@link RetryConfig}. The google-auth library retries
 * token refreshes a fixed number of times internally with no way to configure it, so this loop
 * applies the caller's attempts, backoff and total timeout around each operation.
 *
 * <p>Only transient failures are retried: errors the auth library marks retryable, HTTP 408, 429
 * and 5xx responses, and connection-level I/O failures. With no RetryConfig every operation runs
 * exactly once; a RetryConfig that leaves maxAttempts unset allows up to 3 attempts, and one that
 * leaves the mode unset backs off exponentially from 100 ms, doubling up to 20 s.
 *
 * <p>totalTimeout is checked between attempts: no retry starts once the elapsed time plus the next
 * delay reaches it, but an attempt already in flight is not interrupted (attemptTimeout bounds a
 * single attempt). An interrupted thread stops retrying with an {@link InterruptedIOException}
 * whose cause is the last failure.
 */
final class GcpStsRetrier {
  private static final int DEFAULT_MAX_ATTEMPTS = 3;
  private static final double DEFAULT_MULTIPLIER = 2.0;
  private static final long DEFAULT_INITIAL_DELAY_MILLIS = 100;
  private static final long DEFAULT_MAX_DELAY_MILLIS = 20_000;
  private static final int MAX_CAUSE_DEPTH = 5;

  @FunctionalInterface
  interface IoOperation<T> {
    T run() throws IOException;
  }

  @FunctionalInterface
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final RetryConfig retryConfig;
  private final Sleeper sleeper;
  private final LongSupplier nanoClock;

  GcpStsRetrier(RetryConfig retryConfig) {
    this(retryConfig, Thread::sleep, System::nanoTime);
  }

  GcpStsRetrier(RetryConfig retryConfig, Sleeper sleeper, LongSupplier nanoClock) {
    this.retryConfig = retryConfig;
    this.sleeper = sleeper;
    this.nanoClock = nanoClock;
  }

  boolean isEnabled() {
    return retryConfig != null;
  }

  <T> T execute(IoOperation<T> operation) throws IOException {
    if (retryConfig == null) {
      return operation.run();
    }
    int maxAttempts =
        retryConfig.getMaxAttempts() != null ? retryConfig.getMaxAttempts() : DEFAULT_MAX_ATTEMPTS;
    long startNanos = nanoClock.getAsLong();
    for (int attempt = 1; ; attempt++) {
      try {
        return operation.run();
      } catch (IOException e) {
        if (attempt >= maxAttempts || !isRetryable(e)) {
          throw e;
        }
        throwIfInterrupted(e);
        long delayMillis = delayBeforeRetryMillis(attempt);
        if (exceedsTotalTimeout(startNanos, delayMillis)) {
          throw e;
        }
        sleep(delayMillis, e);
      }
    }
  }

  /** Delay before the retry that follows the given failed attempt (1 for the first failure). */
  long delayBeforeRetryMillis(int failedAttempt) {
    if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      return retryConfig.getFixedDelayMillis();
    }
    boolean exponential = retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL;
    double multiplier =
        exponential && retryConfig.getMultiplier() > 0
            ? retryConfig.getMultiplier()
            : DEFAULT_MULTIPLIER;
    long initialDelay =
        exponential ? retryConfig.getInitialDelayMillis() : DEFAULT_INITIAL_DELAY_MILLIS;
    long maxDelay = exponential ? retryConfig.getMaxDelayMillis() : DEFAULT_MAX_DELAY_MILLIS;
    double delay = initialDelay * Math.pow(multiplier, failedAttempt - 1);
    return (long) Math.min(maxDelay, delay);
  }

  private static void throwIfInterrupted(IOException lastError) throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      InterruptedIOException interrupted =
          new InterruptedIOException("Interrupted before retrying");
      interrupted.initCause(lastError);
      throw interrupted;
    }
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

  private boolean exceedsTotalTimeout(long startNanos, long delayMillis) {
    if (retryConfig.getTotalTimeout() == null) {
      return false;
    }
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(nanoClock.getAsLong() - startNanos);
    return elapsedMillis + delayMillis >= retryConfig.getTotalTimeout();
  }

  private void sleep(long millis, IOException lastError) throws IOException {
    if (millis <= 0) {
      return;
    }
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      InterruptedIOException interrupted =
          new InterruptedIOException("Interrupted while waiting to retry");
      interrupted.initCause(lastError);
      throw interrupted;
    }
  }
}
