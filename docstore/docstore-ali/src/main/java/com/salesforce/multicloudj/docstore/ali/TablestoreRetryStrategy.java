package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Applies a {@link RetryConfig} to Tablestore requests. Which errors are retried comes from the
 * SDK's {@link DefaultRetryStrategy}; this strategy additionally bounds the number of attempts,
 * stops retrying after totalTimeout and replaces the pause between attempts according to the
 * configured mode. Unset maxAttempts defaults to 3 attempts; unset totalTimeout applies no total
 * time limit.
 *
 * <p>EXPONENTIAL waits a random time between half and all of {@code min(initialDelayMillis *
 * 2^(retry - 1), maxDelayMillis)} before each retry, counting the first retry as 1, so clients
 * that fail together do not retry together. The delay always doubles, so RetryConfig.multiplier is
 * not applied.
 *
 * <p>The SDK clones the configured strategy for every request, so the attempt count and deadline
 * are tracked per request. A returned pause of 0 tells the SDK not to retry.
 */
final class TablestoreRetryStrategy implements RetryStrategy {

  static final int DEFAULT_MAX_ATTEMPTS = 3;

  private final RetryConfig retryConfig;
  private final int maxAttempts;
  // Decides retryability and enforces the deadline; its own pause is used when mode is unset.
  private final DefaultRetryStrategy defaults;
  private final long deadlineMillis;
  private int retries;

  TablestoreRetryStrategy(RetryConfig retryConfig) {
    this.retryConfig = retryConfig;
    this.maxAttempts =
        retryConfig.getMaxAttempts() != null ? retryConfig.getMaxAttempts() : DEFAULT_MAX_ATTEMPTS;
    long totalTimeout = totalTimeoutMillis(retryConfig);
    this.defaults = new DefaultRetryStrategy(totalTimeout, TimeUnit.MILLISECONDS);
    this.deadlineMillis = System.currentTimeMillis() + totalTimeout;
  }

  /**
   * Returns the time after which no retry starts. The SDK narrows the time left before its deadline
   * to an int, so it is capped at Integer.MAX_VALUE ms (about 24 days). That cap also stands in for
   * an unset totalTimeout, which RetryConfig defines as no total time limit, instead of the SDK's
   * 10s default deadline.
   */
  static long totalTimeoutMillis(RetryConfig retryConfig) {
    return Math.min(
        Integer.MAX_VALUE,
        retryConfig.getTotalTimeout() != null ? retryConfig.getTotalTimeout() : Long.MAX_VALUE);
  }

  @Override
  public RetryStrategy clone() {
    return new TablestoreRetryStrategy(retryConfig);
  }

  @Override
  public int getRetries() {
    return retries;
  }

  @Override
  public long nextPause(String action, Exception ex) {
    if (retries + 1 >= maxAttempts) {
      return 0;
    }
    long sdkPause = defaults.nextPause(action, ex);
    if (sdkPause <= 0) {
      return 0;
    }
    long pause = retryConfig.getMode() == null ? sdkPause : configuredPause();
    // A retry is not started after the total timeout, so the pause is capped at the time left.
    pause = Math.min(pause, deadlineMillis - System.currentTimeMillis());
    if (pause <= 0) {
      return 0;
    }
    retries++;
    return pause;
  }

  private long configuredPause() {
    if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      return retryConfig.getFixedDelayMillis();
    }
    long ceiling =
        (long)
            Math.min(
                retryConfig.getMaxDelayMillis(),
                retryConfig.getInitialDelayMillis() * Math.pow(2, retries));
    long half = ceiling / 2;
    // At least 1 ms, since a pause of 0 would stop retrying.
    return Math.max(1, half + ThreadLocalRandom.current().nextLong(ceiling - half + 1));
  }
}
