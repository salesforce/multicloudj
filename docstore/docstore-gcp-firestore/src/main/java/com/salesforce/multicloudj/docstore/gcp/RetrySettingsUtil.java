package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.retrying.RetrySettings;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;

/** Applies a {@link RetryConfig} on top of Firestore per-method {@link RetrySettings}. */
final class RetrySettingsUtil {

  private static final double DEFAULT_MULTIPLIER = 2.0;

  private RetrySettingsUtil() {}

  /**
   * Overlays {@code retryConfig}, already checked by {@link RetryConfig#validate()}, onto {@code
   * base}. Fields not set in {@code retryConfig} keep the values from {@code base}, which carries
   * the Firestore client's per-method defaults.
   *
   * <p>{@code attemptTimeout} sets a constant per-RPC timeout and {@code totalTimeout} bounds all
   * attempts combined. FIXED mode is expressed as equal initial and max delays with a multiplier of
   * 1.0. Retry delays are jittered, so the actual wait is a random duration up to the computed
   * delay.
   *
   * <p>An unset {@code totalTimeout} means no total time limit, so retries stop only at the max
   * attempts. gax treats a zero total timeout as unset, but stops retrying altogether when both the
   * total timeout and max attempts are zero. When neither {@code retryConfig} nor {@code base} sets
   * max attempts, the base total timeout is kept so retries stay enabled and bounded.
   *
   * @throws InvalidArgumentException if {@code retryConfig} is null
   */
  static RetrySettings apply(RetrySettings base, RetryConfig retryConfig) {
    if (retryConfig == null) {
      throw new InvalidArgumentException("RetryConfig cannot be null");
    }
    RetrySettings.Builder builder = base.toBuilder();

    if (retryConfig.getMaxAttempts() != null) {
      builder.setMaxAttempts(retryConfig.getMaxAttempts());
    }

    if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
      builder
          .setInitialRetryDelayDuration(Duration.ofMillis(retryConfig.getInitialDelayMillis()))
          .setRetryDelayMultiplier(multiplier(retryConfig))
          .setMaxRetryDelayDuration(Duration.ofMillis(retryConfig.getMaxDelayMillis()));
    } else if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      Duration delay = Duration.ofMillis(retryConfig.getFixedDelayMillis());
      builder
          .setInitialRetryDelayDuration(delay)
          .setRetryDelayMultiplier(1.0)
          .setMaxRetryDelayDuration(delay);
    }

    if (retryConfig.getAttemptTimeout() != null) {
      Duration rpcTimeout = Duration.ofMillis(retryConfig.getAttemptTimeout());
      builder
          .setInitialRpcTimeoutDuration(rpcTimeout)
          .setRpcTimeoutMultiplier(1.0)
          .setMaxRpcTimeoutDuration(rpcTimeout);
    }

    if (retryConfig.getTotalTimeout() != null) {
      builder.setTotalTimeoutDuration(Duration.ofMillis(retryConfig.getTotalTimeout()));
    } else if (builder.getMaxAttempts() > 0) {
      builder.setTotalTimeoutDuration(Duration.ZERO);
    }
    return builder.build();
  }

  // An unset multiplier is 0.0 on RetryConfig, which documents 2.0 as the default.
  private static double multiplier(RetryConfig retryConfig) {
    double multiplier = retryConfig.getMultiplier();
    return multiplier == 0.0 ? DEFAULT_MULTIPLIER : multiplier;
  }
}
