package com.salesforce.multicloudj.common.retries;

import lombok.Builder;
import lombok.Getter;

/**
 * Cloud-agnostic retry configuration for MultiCloudJ services.
 *
 * <p>This configuration provides a unified retry strategy that can be applied across all supported
 * cloud providers and services.
 * It abstracts provider-specific retry mechanisms into a common model that can be translated to
 * native retry configurations for each cloud provider.
 *
 * <p>Supports two retry modes:
 * <ul>
 *   <li><b>EXPONENTIAL</b>: Delay between retries grows exponentially using the formula:
 *       {@code delay = min(maxDelayMillis, initialDelayMillis * multiplier^(attempt-1))}</li>
 *   <li><b>FIXED</b>: Constant delay between retries using {@code fixedDelayMillis}</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Exponential backoff with 3 retries
 * RetryConfig exponentialConfig = RetryConfig.builder()
 *     .mode(RetryConfig.Mode.EXPONENTIAL)
 *     .maxAttempts(3)
 *     .initialDelayMillis(100L)
 *     .multiplier(2.0)
 *     .maxDelayMillis(5000L)
 *     .totalTimeoutMillis(30000L)
 *     .build();
 *
 * // Fixed delay with 5 retries
 * RetryConfig fixedConfig = RetryConfig.builder()
 *     .mode(RetryConfig.Mode.FIXED)
 *     .maxAttempts(5)
 *     .fixedDelayMillis(1000L)
 *     .build();
 * }</pre>
 */
@Getter
@Builder
public final class RetryConfig {
    /**
     * Retry mode determining the delay calculation strategy between retry attempts.
     */
    public enum Mode {
        /**
         * Exponential backoff mode where delay grows exponentially between retries.
         * Uses formula: {@code delay = min(maxDelayMillis, initialDelayMillis * multiplier^(attempt-1))}
         *
         * <p>Requires: {@code initialDelayMillis}, {@code multiplier}, {@code maxDelayMillis}
         */
        EXPONENTIAL,

        /**
         * Fixed delay mode where the same delay is used between all retry attempts.
         *
         * <p>Requires: {@code fixedDelayMillis}
         */
        FIXED,
    }

    /**
     * The retry mode determining delay calculation strategy.
     */
    private final Mode mode;

    /**
     * Maximum number of attempts including the initial request.
     * For example, {@code maxAttempts = 3} means 1 initial attempt + 2 retries.
     */
    private final int maxAttempts;

    /**
     * Initial delay in milliseconds before the first retry (EXPONENTIAL mode only).
     * This is the base delay that gets multiplied by {@code multiplier^(attempt-1)}.
     */
    private final long initialDelayMillis;

    /**
     * Multiplier for exponential backoff (EXPONENTIAL mode only).
     * Each retry delay is calculated as: {@code initialDelayMillis * multiplier^(attempt-1)}.
     * Common values: 2.0 for doubling delay,
     * Default will be 2.0 if you don't specific it. AWS is always 2.0
     */
    private final double multiplier;

    /**
     * Maximum delay cap in milliseconds (EXPONENTIAL mode only).
     * Prevents delay from growing indefinitely by capping it at this value.
     */
    private final long maxDelayMillis;

    /**
     * Fixed delay in milliseconds between retries (FIXED mode only).
     * The same delay is used for all retry attempts.
     */
    private final long fixedDelayMillis;

    /**
     * Optional total timeout in milliseconds for all retry attempts combined.
     * If set, the retry logic will stop retrying once this timeout is exceeded,
     * even if {@code maxAttempts} has not been reached.
     */
    private final Long totalTimeoutMillis;
}
