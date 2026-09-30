package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Verifies the per-subclass default retryability. */
public class RetryableDefaultsTest {

  @Test
  public void retryableTypes_defaultToRetryable() {
    assertTrue(new ResourceExhaustedException().isRetryable());
    assertTrue(new ResourceExhaustedException("msg").isRetryable());
    assertTrue(new ResourceExhaustedException(new Throwable()).isRetryable());
    assertTrue(new DeadlineExceededException().isRetryable());
    assertTrue(new UnknownException().isRetryable());
  }

  @Test
  public void nonRetryableTypes_defaultToNonRetryable() {
    assertFalse(new InvalidArgumentException().isRetryable());
    assertFalse(new UnAuthorizedException().isRetryable());
    assertFalse(new ResourceNotFoundException().isRetryable());
    assertFalse(new ResourceAlreadyExistsException().isRetryable());
    assertFalse(new ResourceConflictException().isRetryable());
    assertFalse(new FailedPreconditionException().isRetryable());
    assertFalse(new TransactionFailedException().isRetryable());
    assertFalse(new UnSupportedOperationException().isRetryable());
  }

  @Test
  public void baseClass_defaultsToNonRetryable() {
    assertFalse(new SubstrateSdkException().isRetryable());
    assertFalse(new SubstrateSdkException("msg").isRetryable());
    assertFalse(new SubstrateSdkException(new Throwable()).isRetryable());
  }

  @Test
  public void explicitOverride_takesPrecedenceOverDefault() {
    InvalidArgumentException retryableInvalidArg =
        new InvalidArgumentException(new Throwable(), true);
    assertTrue(retryableInvalidArg.isRetryable());

    ResourceExhaustedException nonRetryableExhausted =
        new ResourceExhaustedException(new Throwable(), false);
    assertFalse(nonRetryableExhausted.isRetryable());
  }
}
