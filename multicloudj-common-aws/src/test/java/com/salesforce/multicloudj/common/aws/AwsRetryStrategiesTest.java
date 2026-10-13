package com.salesforce.multicloudj.common.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.AcquireInitialTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;

class AwsRetryStrategiesTest {

  private static ClientOverrideConfiguration apply(RetryConfig retryConfig) {
    ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();
    AwsRetryStrategies.applyRetryConfig(builder, retryConfig);
    return builder.build();
  }

  /** Returns the delay before the first retry of a throttled failure. */
  private static Duration firstThrottledRetryDelay(RetryStrategy strategy) {
    RetryStrategy throttled =
        strategy.toBuilder().retryOnException(t -> true).treatAsThrottling(t -> true).build();
    RetryToken token =
        throttled.acquireInitialToken(AcquireInitialTokenRequest.create("scope")).token();
    RefreshRetryTokenResponse refreshed =
        throttled.refreshRetryToken(
            RefreshRetryTokenRequest.builder()
                .token(token)
                .failure(new RuntimeException("throttled"))
                .build());
    return refreshed.delay();
  }

  @Test
  void applyRetryConfig_setsStrategyAndTimeouts() {
    RetryConfig retryConfig =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(5)
            .initialDelayMillis(100)
            .maxDelayMillis(2000)
            .attemptTimeout(1500L)
            .totalTimeout(10000L)
            .build();

    ClientOverrideConfiguration config = apply(retryConfig);

    assertEquals(5, config.retryStrategy().orElseThrow().maxAttempts());
    assertEquals(Optional.of(Duration.ofMillis(1500)), config.apiCallAttemptTimeout());
    assertEquals(Optional.of(Duration.ofMillis(10000)), config.apiCallTimeout());
  }

  @Test
  void applyRetryConfig_leavesUnsetTimeoutsEmpty() {
    ClientOverrideConfiguration config =
        apply(RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(200).build());

    assertTrue(config.retryStrategy().isPresent());
    assertFalse(config.apiCallAttemptTimeout().isPresent());
    assertFalse(config.apiCallTimeout().isPresent());
  }

  @Test
  void toRetryStrategy_keepsSdkDefaultAttemptsWhenUnset() {
    RetryStrategy defaults = StandardRetryStrategy.builder().build();

    RetryStrategy strategy = AwsRetryStrategies.toRetryStrategy(RetryConfig.builder().build());

    assertEquals(defaults.maxAttempts(), strategy.maxAttempts());
  }

  @Test
  void toRetryStrategy_appliesFixedBackoffToThrottledRetries() {
    RetryStrategy strategy =
        AwsRetryStrategies.toRetryStrategy(
            RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .maxAttempts(3)
                .fixedDelayMillis(7)
                .build());

    Duration delay = firstThrottledRetryDelay(strategy);

    // The fixed backoff is jittered within [1ms, configured delay]; the SDK's default throttling
    // backoff would wait hundreds of milliseconds.
    assertTrue(delay.toMillis() <= 7, "delay was " + delay);
  }

  @Test
  void toRetryStrategy_appliesExponentialBackoffToThrottledRetries() {
    RetryStrategy strategy =
        AwsRetryStrategies.toRetryStrategy(
            RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(3)
                .initialDelayMillis(5)
                .maxDelayMillis(7)
                .build());

    Duration delay = firstThrottledRetryDelay(strategy);

    assertTrue(delay.toMillis() <= 7, "delay was " + delay);
  }

  @Test
  void toRetryStrategy_rejectsNullConfig() {
    InvalidArgumentException e =
        assertThrows(
            InvalidArgumentException.class, () -> AwsRetryStrategies.toRetryStrategy(null));
    assertEquals("RetryConfig cannot be null", e.getMessage());
  }
}
