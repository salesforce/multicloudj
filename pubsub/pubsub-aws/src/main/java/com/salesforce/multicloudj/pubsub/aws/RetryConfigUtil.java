package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;

/** Converts a {@link RetryConfig} into AWS SDK client override configuration. */
public final class RetryConfigUtil {

  private RetryConfigUtil() {}

  /**
   * Builds the client override configuration carrying the retry strategy and API call timeouts.
   *
   * <p>{@code attemptTimeout} maps to the per-attempt API call timeout and {@code totalTimeout} to
   * the overall API call timeout. See {@link #toRetryStrategy} for how backoff fields are applied.
   *
   * @throws InvalidArgumentException if {@code retryConfig} is null or has invalid values
   */
  public static ClientOverrideConfiguration toClientOverrideConfiguration(RetryConfig retryConfig) {
    ClientOverrideConfiguration.Builder builder =
        ClientOverrideConfiguration.builder().retryStrategy(toRetryStrategy(retryConfig));
    if (retryConfig.getAttemptTimeout() != null) {
      requirePositive("attemptTimeout", retryConfig.getAttemptTimeout());
      builder.apiCallAttemptTimeout(Duration.ofMillis(retryConfig.getAttemptTimeout()));
    }
    if (retryConfig.getTotalTimeout() != null) {
      requirePositive("totalTimeout", retryConfig.getTotalTimeout());
      builder.apiCallTimeout(Duration.ofMillis(retryConfig.getTotalTimeout()));
    }
    return builder.build();
  }

  /**
   * Converts a {@link RetryConfig} into an AWS SDK {@link RetryStrategy}. Unset fields keep the AWS
   * SDK standard retry strategy defaults.
   *
   * <p>{@code multiplier} is not supported and is ignored. {@code EXPONENTIAL} maps to {@link
   * BackoffStrategy#exponentialDelay}, which accepts only a base and a maximum delay and always
   * doubles the delay between attempts: the delay before attempt {@code n} is {@code
   * min(initialDelayMillis * 2^(n-2), maxDelayMillis)}. That strategy also applies full jitter, so
   * the actual wait is a random duration between zero and that value.
   *
   * @throws InvalidArgumentException if {@code retryConfig} is null or has invalid values
   */
  public static RetryStrategy toRetryStrategy(RetryConfig retryConfig) {
    if (retryConfig == null) {
      throw new InvalidArgumentException("RetryConfig cannot be null");
    }
    StandardRetryStrategy.Builder strategyBuilder = StandardRetryStrategy.builder();

    if (retryConfig.getMaxAttempts() != null) {
      requirePositive("maxAttempts", retryConfig.getMaxAttempts());
      strategyBuilder.maxAttempts(retryConfig.getMaxAttempts());
    }

    if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
      requirePositive("initialDelayMillis", retryConfig.getInitialDelayMillis());
      requirePositive("maxDelayMillis", retryConfig.getMaxDelayMillis());
      strategyBuilder.backoffStrategy(
          BackoffStrategy.exponentialDelay(
              Duration.ofMillis(retryConfig.getInitialDelayMillis()),
              Duration.ofMillis(retryConfig.getMaxDelayMillis())));
    } else if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      requirePositive("fixedDelayMillis", retryConfig.getFixedDelayMillis());
      strategyBuilder.backoffStrategy(
          BackoffStrategy.fixedDelay(Duration.ofMillis(retryConfig.getFixedDelayMillis())));
    }
    return strategyBuilder.build();
  }

  private static void requirePositive(String field, long value) {
    if (value <= 0) {
      throw new InvalidArgumentException(
          "RetryConfig." + field + " must be greater than 0, got: " + value);
    }
  }
}
