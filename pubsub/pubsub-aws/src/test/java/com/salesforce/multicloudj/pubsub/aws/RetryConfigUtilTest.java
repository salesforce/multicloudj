package com.salesforce.multicloudj.pubsub.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;

public class RetryConfigUtilTest {

  @Test
  void testExponentialModeSetsMaxAttempts() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(4)
            .initialDelayMillis(100L)
            .maxDelayMillis(5000L)
            .build();

    RetryStrategy strategy = RetryConfigUtil.toRetryStrategy(config);

    assertEquals(4, strategy.maxAttempts());
  }

  @Test
  void testFixedModeSetsMaxAttempts() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(2)
            .fixedDelayMillis(500L)
            .build();

    RetryStrategy strategy = RetryConfigUtil.toRetryStrategy(config);

    assertEquals(2, strategy.maxAttempts());
  }

  @Test
  void testUnsetMaxAttemptsKeepsSdkDefault() {
    RetryConfig config =
        RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(500L).build();

    RetryStrategy strategy = RetryConfigUtil.toRetryStrategy(config);

    assertEquals(StandardRetryStrategy.builder().build().maxAttempts(), strategy.maxAttempts());
  }

  @Test
  void testTimeoutsMapToApiCallTimeouts() {
    RetryConfig config =
        RetryConfig.builder().maxAttempts(3).attemptTimeout(2000L).totalTimeout(10000L).build();

    ClientOverrideConfiguration override = RetryConfigUtil.toClientOverrideConfiguration(config);

    assertEquals(Duration.ofMillis(2000L), override.apiCallAttemptTimeout().orElseThrow());
    assertEquals(Duration.ofMillis(10000L), override.apiCallTimeout().orElseThrow());
    assertEquals(3, override.retryStrategy().orElseThrow().maxAttempts());
  }

  @Test
  void testUnsetTimeoutsAreNotOverridden() {
    RetryConfig config = RetryConfig.builder().maxAttempts(3).build();

    ClientOverrideConfiguration override = RetryConfigUtil.toClientOverrideConfiguration(config);

    assertFalse(override.apiCallAttemptTimeout().isPresent());
    assertFalse(override.apiCallTimeout().isPresent());
    assertTrue(override.retryStrategy().isPresent());
  }

  @Test
  void testTimeoutOnlyConfigKeepsSdkRetryStrategy() {
    RetryConfig config = RetryConfig.builder().attemptTimeout(2000L).build();

    ClientOverrideConfiguration override = RetryConfigUtil.toClientOverrideConfiguration(config);

    assertEquals(Duration.ofMillis(2000L), override.apiCallAttemptTimeout().orElseThrow());
    assertFalse(override.retryStrategy().isPresent());
  }

  @Test
  void testEmptyConfigOverridesNothing() {
    ClientOverrideConfiguration override =
        RetryConfigUtil.toClientOverrideConfiguration(RetryConfig.builder().build());

    assertFalse(override.retryStrategy().isPresent());
    assertFalse(override.apiCallAttemptTimeout().isPresent());
    assertFalse(override.apiCallTimeout().isPresent());
  }

  @Test
  void testModeOnlyConfigOverridesRetryStrategy() {
    RetryConfig config =
        RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(100L).build();

    ClientOverrideConfiguration override = RetryConfigUtil.toClientOverrideConfiguration(config);

    assertTrue(override.retryStrategy().isPresent());
  }

  @Test
  void testNullConfigThrows() {
    InvalidArgumentException exception =
        assertThrows(InvalidArgumentException.class, () -> RetryConfigUtil.toRetryStrategy(null));
    assertEquals("RetryConfig cannot be null", exception.getMessage());

    exception =
        assertThrows(
            InvalidArgumentException.class,
            () -> RetryConfigUtil.toClientOverrideConfiguration(null));
    assertEquals("RetryConfig cannot be null", exception.getMessage());
  }

  @Test
  void testMaxDelayBelowInitialDelayThrows() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(1000L)
            .maxDelayMillis(500L)
            .build();

    InvalidArgumentException exception =
        assertThrows(InvalidArgumentException.class, () -> RetryConfigUtil.toRetryStrategy(config));
    assertEquals(
        "RetryConfig.maxDelayMillis must not be less than initialDelayMillis, got: 500 < 1000",
        exception.getMessage());
  }

  @Test
  void testNonPositiveMaxAttemptsThrows() {
    RetryConfig config = RetryConfig.builder().maxAttempts(0).build();

    InvalidArgumentException exception =
        assertThrows(InvalidArgumentException.class, () -> RetryConfigUtil.toRetryStrategy(config));
    assertEquals("RetryConfig.maxAttempts must be greater than 0, got: 0", exception.getMessage());
  }

  @Test
  void testExponentialModeWithoutDelaysThrows() {
    RetryConfig config =
        RetryConfig.builder().mode(RetryConfig.Mode.EXPONENTIAL).maxDelayMillis(5000L).build();

    InvalidArgumentException exception =
        assertThrows(InvalidArgumentException.class, () -> RetryConfigUtil.toRetryStrategy(config));
    assertEquals(
        "RetryConfig.initialDelayMillis must be greater than 0, got: 0", exception.getMessage());
  }

  @Test
  void testFixedModeWithoutDelayThrows() {
    RetryConfig config = RetryConfig.builder().mode(RetryConfig.Mode.FIXED).build();

    InvalidArgumentException exception =
        assertThrows(InvalidArgumentException.class, () -> RetryConfigUtil.toRetryStrategy(config));
    assertEquals(
        "RetryConfig.fixedDelayMillis must be greater than 0, got: 0", exception.getMessage());
  }

  @Test
  void testNonPositiveTimeoutThrows() {
    RetryConfig config = RetryConfig.builder().totalTimeout(-1L).build();

    InvalidArgumentException exception =
        assertThrows(
            InvalidArgumentException.class,
            () -> RetryConfigUtil.toClientOverrideConfiguration(config));
    assertEquals(
        "RetryConfig.totalTimeout must be greater than 0, got: -1", exception.getMessage());
  }
}
