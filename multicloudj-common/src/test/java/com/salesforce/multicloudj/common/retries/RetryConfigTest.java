package com.salesforce.multicloudj.common.retries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class RetryConfigTest {

    @Test
    void testExponentialModeBuilder() {
        RetryConfig config = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(5)
                .initialDelayMillis(100L)
                .multiplier(2.0)
                .maxDelayMillis(5000L)
                .totalTimeoutMillis(30000L)
                .build();

        assertEquals(RetryConfig.Mode.EXPONENTIAL, config.getMode());
        assertEquals(5, config.getMaxAttempts());
        assertEquals(100L, config.getInitialDelayMillis());
        assertEquals(2.0, config.getMultiplier());
        assertEquals(5000L, config.getMaxDelayMillis());
        assertEquals(30000L, config.getTotalTimeoutMillis());
    }

    @Test
    void testFixedModeBuilder() {
        RetryConfig config = RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .maxAttempts(3)
                .fixedDelayMillis(1000L)
                .totalTimeoutMillis(10000L)
                .build();

        assertEquals(RetryConfig.Mode.FIXED, config.getMode());
        assertEquals(3, config.getMaxAttempts());
        assertEquals(1000L, config.getFixedDelayMillis());
        assertEquals(10000L, config.getTotalTimeoutMillis());
    }

    @Test
    void testBuilderWithoutTotalTimeout() {
        RetryConfig config = RetryConfig.builder()
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
        assertNull(config.getTotalTimeoutMillis());
    }

    @Test
    void testMinimalExponentialConfig() {
        RetryConfig config = RetryConfig.builder()
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
        RetryConfig config = RetryConfig.builder()
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
}
