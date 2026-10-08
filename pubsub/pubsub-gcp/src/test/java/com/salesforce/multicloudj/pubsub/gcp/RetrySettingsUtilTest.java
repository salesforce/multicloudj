package com.salesforce.multicloudj.pubsub.gcp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.gax.retrying.RetrySettings;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class RetrySettingsUtilTest {

  private static final RetrySettings BASE =
      RetrySettings.newBuilder()
          .setInitialRetryDelayDuration(Duration.ofMillis(100))
          .setRetryDelayMultiplier(4.0)
          .setMaxRetryDelayDuration(Duration.ofSeconds(60))
          .setInitialRpcTimeoutDuration(Duration.ofSeconds(5))
          .setRpcTimeoutMultiplier(1.5)
          .setMaxRpcTimeoutDuration(Duration.ofSeconds(60))
          .setTotalTimeoutDuration(Duration.ofSeconds(60))
          .setMaxAttempts(0)
          .build();

  @Test
  void testExponentialModeOverridesBackoff() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .maxAttempts(5)
            .initialDelayMillis(200L)
            .multiplier(3.0)
            .maxDelayMillis(8000L)
            .build();

    RetrySettings settings = RetrySettingsUtil.apply(BASE, config);

    assertEquals(5, settings.getMaxAttempts());
    assertEquals(Duration.ofMillis(200), settings.getInitialRetryDelayDuration());
    assertEquals(3.0, settings.getRetryDelayMultiplier());
    assertEquals(Duration.ofMillis(8000), settings.getMaxRetryDelayDuration());
  }

  @Test
  void testExponentialModeDefaultsMultiplierToTwo() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(200L)
            .maxDelayMillis(8000L)
            .build();

    RetrySettings settings = RetrySettingsUtil.apply(BASE, config);

    assertEquals(2.0, settings.getRetryDelayMultiplier());
  }

  @Test
  void testFixedModeUsesConstantDelay() {
    RetryConfig config =
        RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(500L).build();

    RetrySettings settings = RetrySettingsUtil.apply(BASE, config);

    assertEquals(Duration.ofMillis(500), settings.getInitialRetryDelayDuration());
    assertEquals(Duration.ofMillis(500), settings.getMaxRetryDelayDuration());
    assertEquals(1.0, settings.getRetryDelayMultiplier());
  }

  @Test
  void testUnsetFieldsKeepBaseValues() {
    RetryConfig config = RetryConfig.builder().totalTimeout(2000L).build();

    RetrySettings settings = RetrySettingsUtil.apply(BASE, config);

    assertEquals(Duration.ofMillis(2000), settings.getTotalTimeoutDuration());
    assertEquals(0, settings.getMaxAttempts());
    assertEquals(Duration.ofMillis(100), settings.getInitialRetryDelayDuration());
    assertEquals(4.0, settings.getRetryDelayMultiplier());
    assertEquals(Duration.ofSeconds(60), settings.getMaxRetryDelayDuration());
    assertEquals(Duration.ofSeconds(5), settings.getInitialRpcTimeoutDuration());
    assertEquals(1.5, settings.getRpcTimeoutMultiplier());
    assertEquals(Duration.ofSeconds(60), settings.getMaxRpcTimeoutDuration());
  }

  @Test
  void testAttemptTimeoutSetsConstantRpcTimeout() {
    RetryConfig config = RetryConfig.builder().attemptTimeout(3000L).build();

    RetrySettings settings = RetrySettingsUtil.apply(BASE, config);

    assertEquals(Duration.ofMillis(3000), settings.getInitialRpcTimeoutDuration());
    assertEquals(Duration.ofMillis(3000), settings.getMaxRpcTimeoutDuration());
    assertEquals(1.0, settings.getRpcTimeoutMultiplier());
    assertEquals(Duration.ofSeconds(60), settings.getTotalTimeoutDuration());
  }

  @Test
  void testNullConfigRejected() {
    assertThrows(InvalidArgumentException.class, () -> RetrySettingsUtil.apply(BASE, null));
    assertThrows(InvalidArgumentException.class, () -> RetrySettingsUtil.validate(null));
  }

  @Test
  void testMultiplierBelowOneRejected() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.EXPONENTIAL)
            .initialDelayMillis(100L)
            .multiplier(0.5)
            .maxDelayMillis(1000L)
            .build();

    InvalidArgumentException e =
        assertThrows(InvalidArgumentException.class, () -> RetrySettingsUtil.validate(config));
    assertTrue(e.getMessage().contains("multiplier"));
  }

  @Test
  void testExponentialModeRequiresDelays() {
    RetryConfig config =
        RetryConfig.builder().mode(RetryConfig.Mode.EXPONENTIAL).maxDelayMillis(1000L).build();

    InvalidArgumentException e =
        assertThrows(InvalidArgumentException.class, () -> RetrySettingsUtil.validate(config));
    assertTrue(e.getMessage().contains("initialDelayMillis"));
  }

  @Test
  void testFixedModeRequiresDelay() {
    RetryConfig config = RetryConfig.builder().mode(RetryConfig.Mode.FIXED).build();

    InvalidArgumentException e =
        assertThrows(InvalidArgumentException.class, () -> RetrySettingsUtil.validate(config));
    assertTrue(e.getMessage().contains("fixedDelayMillis"));
  }

  @Test
  void testNonPositiveValuesRejected() {
    assertThrows(
        InvalidArgumentException.class,
        () -> RetrySettingsUtil.validate(RetryConfig.builder().maxAttempts(0).build()));
    assertThrows(
        InvalidArgumentException.class,
        () -> RetrySettingsUtil.validate(RetryConfig.builder().attemptTimeout(0L).build()));
    assertThrows(
        InvalidArgumentException.class,
        () -> RetrySettingsUtil.validate(RetryConfig.builder().totalTimeout(-1L).build()));
  }

  @Test
  void testValidConfigAccepted() {
    RetryConfig config =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(100L)
            .attemptTimeout(1000L)
            .totalTimeout(5000L)
            .build();

    assertDoesNotThrow(() -> RetrySettingsUtil.validate(config));
  }
}
