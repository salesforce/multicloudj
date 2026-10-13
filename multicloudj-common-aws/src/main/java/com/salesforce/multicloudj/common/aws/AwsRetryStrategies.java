package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;

/** Translates a {@link RetryConfig} into AWS SDK client retry and timeout settings. */
public final class AwsRetryStrategies {

  private AwsRetryStrategies() {}

  /**
   * Applies the retry strategy and the per-attempt and total API call timeouts from the
   * RetryConfig to an AWS SDK client override configuration. Unset timeouts keep the SDK defaults.
   *
   * @param config the override configuration builder of the AWS SDK client
   * @param retryConfig the retry configuration to apply
   * @throws InvalidArgumentException if retryConfig is null
   */
  public static void applyRetryConfig(
      ClientOverrideConfiguration.Builder config, RetryConfig retryConfig) {
    config.retryStrategy(toRetryStrategy(retryConfig));
    if (retryConfig.getAttemptTimeout() != null) {
      config.apiCallAttemptTimeout(Duration.ofMillis(retryConfig.getAttemptTimeout()));
    }
    if (retryConfig.getTotalTimeout() != null) {
      config.apiCallTimeout(Duration.ofMillis(retryConfig.getTotalTimeout()));
    }
  }

  /**
   * Builds a StandardRetryStrategy from a RetryConfig already checked by {@link
   * RetryConfig#validate()}. Unset fields keep the SDK defaults. The SDK's exponential backoff
   * always doubles, so RetryConfig.multiplier is not applied.
   *
   * @param retryConfig the retry configuration to convert
   * @return the AWS SDK retry strategy
   * @throws InvalidArgumentException if retryConfig is null
   */
  public static RetryStrategy toRetryStrategy(RetryConfig retryConfig) {
    if (retryConfig == null) {
      throw new InvalidArgumentException("RetryConfig cannot be null");
    }
    StandardRetryStrategy.Builder strategy = StandardRetryStrategy.builder();
    if (retryConfig.getMaxAttempts() != null) {
      strategy.maxAttempts(retryConfig.getMaxAttempts());
    }
    BackoffStrategy backoff = null;
    if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
      backoff =
          BackoffStrategy.exponentialDelay(
              Duration.ofMillis(retryConfig.getInitialDelayMillis()),
              Duration.ofMillis(retryConfig.getMaxDelayMillis()));
    } else if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      backoff = BackoffStrategy.fixedDelay(Duration.ofMillis(retryConfig.getFixedDelayMillis()));
    }
    if (backoff != null) {
      // Throttled responses use a separate backoff strategy that must be set explicitly.
      strategy.backoffStrategy(backoff).throttlingBackoffStrategy(backoff);
    }
    return strategy.build();
  }
}
