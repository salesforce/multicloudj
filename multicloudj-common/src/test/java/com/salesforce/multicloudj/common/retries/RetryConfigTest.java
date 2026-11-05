package com.salesforce.multicloudj.common.retries;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;

class RetryConfigTest {

  @Test
  void testExponentialModeBuilder() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(5)
            .initialDelayMillis(100L)
            .multiplier(2.0)
            .maxDelayMillis(5000L)
            .totalTimeout(30000L)
            .build();

    assertEquals(RetryConfig.Mode.EXPONENTIAL, config.getMode());
    assertEquals(5, config.getMaxAttempts());
    assertEquals(100L, config.getInitialDelayMillis());
    assertEquals(2.0, config.getMultiplier());
    assertEquals(5000L, config.getMaxDelayMillis());
    assertEquals(30000L, config.getTotalTimeout());
  }

  @Test
  void testFixedModeBuilder() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(1000L)
            .totalTimeout(10000L)
            .build();

    assertEquals(RetryConfig.Mode.FIXED, config.getMode());
    assertEquals(3, config.getMaxAttempts());
    assertEquals(1000L, config.getFixedDelayMillis());
    assertEquals(10000L, config.getTotalTimeout());
  }

  @Test
  void testBuilderWithoutTotalTimeout() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(3)
            .initialDelayMillis(50L)
            .multiplier(1.5)
            .maxDelayMillis(2000L)
            .build();

    assertEquals(RetryConfig.Mode.EXPONENTIAL, config.getMode());
    assertEquals(3, config.getMaxAttempts());
    assertEquals(50L, config.getInitialDelayMillis());
    assertEquals(1.5, config.getMultiplier());
    assertEquals(2000L, config.getMaxDelayMillis());
    assertNull(config.getTotalTimeout());
  }

  @Test
  void testMinimalExponentialConfig() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(1)
            .initialDelayMillis(10L)
            .multiplier(1.0)
            .maxDelayMillis(10L)
            .build();

    assertNotNull(config);
    assertEquals(RetryConfig.Mode.EXPONENTIAL, config.getMode());
    assertEquals(1, config.getMaxAttempts());
    assertEquals(10L, config.getInitialDelayMillis());
    assertEquals(1.0, config.getMultiplier());
    assertEquals(10L, config.getMaxDelayMillis());
  }

  @Test
  void testMinimalFixedConfig() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(1)
            .fixedDelayMillis(100L)
            .build();

    assertNotNull(config);
    assertEquals(RetryConfig.Mode.FIXED, config.getMode());
    assertEquals(1, config.getMaxAttempts());
    assertEquals(100L, config.getFixedDelayMillis());
  }

  @Test
  void testModeEnum() {
    assertEquals(2, RetryConfig.Mode.values().length);
    assertEquals(RetryConfig.Mode.EXPONENTIAL, RetryConfig.Mode.valueOf("EXPONENTIAL"));
    assertEquals(RetryConfig.Mode.FIXED, RetryConfig.Mode.valueOf("FIXED"));
  }

  private static void assertInvalid(RetryConfig config, String message) {
    InvalidArgumentException e = assertThrows(InvalidArgumentException.class, config::validate);
    assertEquals(message, e.getMessage());
  }

  @Test
  void validate_acceptsValidConfigs() {
    assertDoesNotThrow(() -> RetryConfig.builder().build().validate());
    assertDoesNotThrow(
        () ->
            RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(1)
                .initialDelayMillis(100L)
                .maxDelayMillis(100L)
                .multiplier(1.0)
                .attemptTimeout(1L)
                .totalTimeout(1L)
                .build()
                .validate());
    assertDoesNotThrow(
        () ->
            RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .fixedDelayMillis(50L)
                .build()
                .validate());
  }

  @Test
  void validate_rejectsNonPositiveMaxAttempts() {
    assertInvalid(
        RetryConfig.builder().maxAttempts(0).build(),
        "RetryConfig.maxAttempts must be greater than 0, got: 0");
    assertInvalid(
        RetryConfig.builder().maxAttempts(-1).build(),
        "RetryConfig.maxAttempts must be greater than 0, got: -1");
  }

  @Test
  void validate_rejectsNonPositiveExponentialInitialDelay() {
    assertInvalid(
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(0L)
            .maxDelayMillis(5000L)
            .build(),
        "RetryConfig.initialDelayMillis must be greater than 0 for EXPONENTIAL mode, got: 0");
  }

  @Test
  void validate_rejectsNonPositiveExponentialMaxDelay() {
    assertInvalid(
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(100L)
            .maxDelayMillis(0L)
            .build(),
        "RetryConfig.maxDelayMillis must be greater than 0 for EXPONENTIAL mode, got: 0");
  }

  @Test
  void validate_rejectsExponentialMultiplierBelowOne() {
    assertInvalid(
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(100L)
            .maxDelayMillis(5000L)
            .multiplier(0.5)
            .build(),
        "RetryConfig.multiplier must be at least 1 for EXPONENTIAL mode, got: 0.5");
  }

  @Test
  void validate_rejectsNonFiniteExponentialMultiplier() {
    for (double multiplier :
        new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
      assertInvalid(
          RetryConfig.builder()
              .mode(RetryConfig.Mode.EXPONENTIAL)
              .initialDelayMillis(100L)
              .maxDelayMillis(5000L)
              .multiplier(multiplier)
              .build(),
          "RetryConfig.multiplier must be at least 1 for EXPONENTIAL mode, got: " + multiplier);
    }
  }

  @Test
  void validate_rejectsExponentialMaxDelayBelowInitialDelay() {
    assertInvalid(
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(500L)
            .maxDelayMillis(100L)
            .build(),
        "RetryConfig.maxDelayMillis must not be less than initialDelayMillis for EXPONENTIAL"
            + " mode, got maxDelayMillis: 100, initialDelayMillis: 500");
  }

  @Test
  void validate_rejectsNonPositiveFixedDelay() {
    assertInvalid(
        RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(0L).build(),
        "RetryConfig.fixedDelayMillis must be greater than 0 for FIXED mode, got: 0");
  }

  @Test
  void validate_rejectsNonPositiveTimeouts() {
    assertInvalid(
        RetryConfig.builder().attemptTimeout(0L).build(),
        "RetryConfig.attemptTimeout must be greater than 0, got: 0");
    assertInvalid(
        RetryConfig.builder().totalTimeout(-1L).build(),
        "RetryConfig.totalTimeout must be greater than 0, got: -1");
  }
}
