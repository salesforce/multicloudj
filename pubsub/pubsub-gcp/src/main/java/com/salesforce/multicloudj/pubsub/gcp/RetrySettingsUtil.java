package com.salesforce.multicloudj.pubsub.gcp;

import com.google.api.gax.retrying.RetrySettings;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;

/** Applies a {@link RetryConfig} on top of GCP Pub/Sub per-method {@link RetrySettings}. */
public final class RetrySettingsUtil {

  private static final double DEFAULT_MULTIPLIER = 2.0;

  private RetrySettingsUtil() {}

  /**
   * Overlays {@code retryConfig} onto {@code base}. Fields not set in {@code retryConfig} keep the
   * values from {@code base}, which carries the Pub/Sub client's per-method defaults.
   *
   * <p>{@code attemptTimeout} sets a constant per-RPC timeout and {@code totalTimeout} bounds all
   * attempts combined. FIXED mode is expressed as equal initial and max delays with a multiplier of
   * 1.0. Retry delays are jittered, so the actual wait is a random duration up to the computed
   * delay. When {@code totalTimeout} is unset, the base total timeout still bounds every attempt,
   * including one with a longer {@code attemptTimeout}.
   *
   * @throws InvalidArgumentException if {@code retryConfig} is null or has invalid values
   */
  public static RetrySettings apply(RetrySettings base, RetryConfig retryConfig) {
    validate(retryConfig);
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
    }
    return builder.build();
  }

  /**
   * Validates {@code retryConfig} without building settings.
   *
   * @throws InvalidArgumentException if {@code retryConfig} is null or has invalid values
   */
  public static void validate(RetryConfig retryConfig) {
    if (retryConfig == null) {
      throw new InvalidArgumentException("RetryConfig cannot be null");
    }
    if (retryConfig.getMaxAttempts() != null) {
      requirePositive("maxAttempts", retryConfig.getMaxAttempts());
    }
    // Backoff fields only take effect for a mode, so setting them without one is a mistake rather
    // than a request for the Pub/Sub default backoff.
    if (retryConfig.getMode() == null
        && (retryConfig.getInitialDelayMillis() != 0
            || retryConfig.getMaxDelayMillis() != 0
            || retryConfig.getFixedDelayMillis() != 0
            || retryConfig.getMultiplier() != 0.0)) {
      throw new InvalidArgumentException(
          "RetryConfig.mode must be set when initialDelayMillis, maxDelayMillis,"
              + " fixedDelayMillis or multiplier is set");
    }
    if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
      requirePositive("initialDelayMillis", retryConfig.getInitialDelayMillis());
      requirePositive("maxDelayMillis", retryConfig.getMaxDelayMillis());
      if (retryConfig.getMaxDelayMillis() < retryConfig.getInitialDelayMillis()) {
        throw new InvalidArgumentException(
            "RetryConfig.maxDelayMillis must not be less than initialDelayMillis, got: "
                + retryConfig.getMaxDelayMillis()
                + " < "
                + retryConfig.getInitialDelayMillis());
      }
      multiplier(retryConfig);
    } else if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      requirePositive("fixedDelayMillis", retryConfig.getFixedDelayMillis());
    }
    if (retryConfig.getAttemptTimeout() != null) {
      requirePositive("attemptTimeout", retryConfig.getAttemptTimeout());
    }
    if (retryConfig.getTotalTimeout() != null) {
      requirePositive("totalTimeout", retryConfig.getTotalTimeout());
    }
  }

  // An unset multiplier is 0.0 on RetryConfig; treat it as 2.0 because RetrySettings rejects
  // multipliers below 1.0. NaN and infinity are rejected explicitly because RetrySettings accepts
  // them and then retries with no delay.
  private static double multiplier(RetryConfig retryConfig) {
    double multiplier = retryConfig.getMultiplier();
    if (multiplier == 0.0) {
      return DEFAULT_MULTIPLIER;
    }
    if (!Double.isFinite(multiplier) || multiplier < 1.0) {
      throw new InvalidArgumentException(
          "RetryConfig.multiplier must be at least 1.0, got: " + multiplier);
    }
    return multiplier;
  }

  private static void requirePositive(String field, long value) {
    if (value <= 0) {
      throw new InvalidArgumentException(
          "RetryConfig." + field + " must be greater than 0, got: " + value);
    }
  }
}
