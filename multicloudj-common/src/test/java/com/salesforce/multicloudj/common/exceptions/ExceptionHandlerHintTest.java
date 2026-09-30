package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Verifies retryability propagation through {@link ExceptionHandler#build}. */
class ExceptionHandlerHintTest {

  @Test
  void hintTrueOverridesNonRetryableTypeDefault() {
    Throwable cause = new Throwable("cause");
    SubstrateSdkException result =
        ExceptionHandler.build(InvalidArgumentException.class, cause, true);
    assertInstanceOf(InvalidArgumentException.class, result);
    assertTrue(result.isRetryable());
    assertEquals(cause, result.getCause());
  }

  @Test
  void hintFalseOverridesRetryableTypeDefault() {
    Throwable cause = new Throwable("cause");
    SubstrateSdkException result =
        ExceptionHandler.build(ResourceExhaustedException.class, cause, false);
    assertInstanceOf(ResourceExhaustedException.class, result);
    assertFalse(result.isRetryable());
  }

  @Test
  void nullHintFallsBackToTypeDefault() {
    SubstrateSdkException nonRetryable =
        ExceptionHandler.build(InvalidArgumentException.class, new Throwable(), null);
    assertFalse(nonRetryable.isRetryable());

    SubstrateSdkException retryable =
        ExceptionHandler.build(ResourceExhaustedException.class, new Throwable(), null);
    assertTrue(retryable.isRetryable());
  }

  @Test
  void unknownExceptionFallbackAppliesHint() {
    SubstrateSdkException result = ExceptionHandler.build(null, new Throwable(), false);
    assertInstanceOf(UnknownException.class, result);
    assertFalse(result.isRetryable());
  }

  @Test
  void unknownExceptionFallbackDefaultsToRetryable() {
    SubstrateSdkException result = ExceptionHandler.build(null, new Throwable());
    assertInstanceOf(UnknownException.class, result);
    assertTrue(result.isRetryable());
  }

  @Test
  void existingTypedExceptionIsReturnedUnchanged() {
    ResourceNotFoundException original = new ResourceNotFoundException("not found");
    SubstrateSdkException result =
        ExceptionHandler.build(ResourceNotFoundException.class, original, true);
    assertSame(original, result);
    assertFalse(result.isRetryable());
  }

  @Test
  void backwardCompatibleTwoArgOverloadStillWorks() {
    SubstrateSdkException result =
        ExceptionHandler.build(ResourceExhaustedException.class, new Throwable());
    assertInstanceOf(ResourceExhaustedException.class, result);
    assertTrue(result.isRetryable());
  }
}
