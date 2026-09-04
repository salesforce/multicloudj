package com.salesforce.multicloudj.common.exceptions;

/**
 * Thrown when a call is short-circuited because the circuit breaker is open. The failing dependency
 * is being given time to recover, so the call was rejected without being attempted.
 *
 * <p>This exception is non-retryable: an open breaker means an immediate retry would also be
 * rejected. Callers should back off and retry only after the breaker's wait duration has elapsed.
 */
public class CircuitBreakerOpenException extends SubstrateSdkException {

  private static final boolean DEFAULT_RETRYABLE = false;

  public CircuitBreakerOpenException() {
    super(DEFAULT_RETRYABLE);
  }

  public CircuitBreakerOpenException(String message, Throwable cause) {
    super(message, cause, DEFAULT_RETRYABLE);
  }

  public CircuitBreakerOpenException(String message) {
    super(message, DEFAULT_RETRYABLE);
  }

  public CircuitBreakerOpenException(Throwable cause) {
    super(cause, DEFAULT_RETRYABLE);
  }
}
