package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class CircuitBreakerOpenExceptionTest {

  @Test
  public void testDefaultConstructor() {
    CircuitBreakerOpenException exception = new CircuitBreakerOpenException();
    assertNull(exception.getMessage());
    assertNull(exception.getCause());
    assertFalse(exception.isRetryable());
  }

  @Test
  public void testConstructorWithMessage() {
    String message = "Circuit breaker is open";
    CircuitBreakerOpenException exception = new CircuitBreakerOpenException(message);
    assertEquals(message, exception.getMessage());
    assertNull(exception.getCause());
    assertFalse(exception.isRetryable());
  }

  @Test
  public void testConstructorWithCause() {
    Throwable cause = new Throwable("Cause");
    CircuitBreakerOpenException exception = new CircuitBreakerOpenException(cause);
    assertEquals(cause, exception.getCause());
    assertEquals("java.lang.Throwable: Cause", exception.getMessage());
    assertFalse(exception.isRetryable());
  }

  @Test
  public void testConstructorWithMessageAndCause() {
    String message = "Circuit breaker is open";
    Throwable cause = new Throwable("Cause");
    CircuitBreakerOpenException exception = new CircuitBreakerOpenException(message, cause);
    assertEquals(message, exception.getMessage());
    assertEquals(cause, exception.getCause());
    assertFalse(exception.isRetryable());
  }
}
