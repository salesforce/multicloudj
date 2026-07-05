package com.salesforce.multicloudj.common.exceptions;

/**
 * Builds {@link SubstrateSdkException} instances for provider implementations.
 *
 * <p>Providers convert a native {@link Throwable} into a typed {@link Class}{@code <? extends
 * SubstrateSdkException>} (typically via their per-service {@code ErrorCodeMapping}) and then call
 * {@link #build(Class, Throwable, Boolean)} to produce a ready-to-throw exception with the correct
 * retryability classification.
 */
public final class ExceptionHandler {

  private ExceptionHandler() {}

  /**
   * Constructs a {@link SubstrateSdkException} of the given subclass wrapping {@code cause}.
   *
   * <p>Retryability resolution:
   *
   * <ul>
   *   <li>If {@code retryableHint} is {@code null}, the exception type's default retryability is
   *       used (e.g. {@code ResourceExhaustedException} → retryable, {@code
   *       InvalidArgumentException} → non-retryable).
   *   <li>If {@code retryableHint} is non-null, it overrides the default. Use this when the
   *       provider SDK has an authoritative signal (throttling, 5xx, etc.).
   * </ul>
   *
   * <p>If {@code cause} is already a {@link SubstrateSdkException}, it is returned unchanged so the
   * concrete subtype and retryable flag with which it was originally constructed are preserved.
   *
   * @param exceptionClass the typed subclass to instantiate; {@code null} or unknown subclasses are
   *     mapped to {@link UnknownException}.
   * @param cause the original throwable from the provider SDK; may be {@code null}.
   * @param retryableHint provider-supplied retryability override; {@code null} for type default.
   * @return a built {@link SubstrateSdkException}.
   */
  public static SubstrateSdkException build(
      Class<? extends SubstrateSdkException> exceptionClass,
      Throwable cause,
      Boolean retryableHint) {
    if (cause instanceof SubstrateSdkException) {
      return (SubstrateSdkException) cause;
    }
    if (exceptionClass == null) {
      return newUnknown(cause, retryableHint);
    }
    if (ResourceAlreadyExistsException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new ResourceAlreadyExistsException(cause)
          : new ResourceAlreadyExistsException(cause, retryableHint);
    }
    if (ResourceNotFoundException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new ResourceNotFoundException(cause)
          : new ResourceNotFoundException(cause, retryableHint);
    }
    if (ResourceConflictException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new ResourceConflictException(cause)
          : new ResourceConflictException(cause, retryableHint);
    }
    if (UnAuthorizedException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new UnAuthorizedException(cause)
          : new UnAuthorizedException(cause, retryableHint);
    }
    if (ResourceExhaustedException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new ResourceExhaustedException(cause)
          : new ResourceExhaustedException(cause, retryableHint);
    }
    if (InvalidArgumentException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new InvalidArgumentException(cause)
          : new InvalidArgumentException(cause, retryableHint);
    }
    if (FailedPreconditionException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new FailedPreconditionException(cause)
          : new FailedPreconditionException(cause, retryableHint);
    }
    if (TransactionFailedException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new TransactionFailedException(cause)
          : new TransactionFailedException(cause, retryableHint);
    }
    if (DeadlineExceededException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new DeadlineExceededException(cause)
          : new DeadlineExceededException(cause, retryableHint);
    }
    if (UnSupportedOperationException.class.isAssignableFrom(exceptionClass)) {
      return retryableHint == null
          ? new UnSupportedOperationException(cause)
          : new UnSupportedOperationException(cause, retryableHint);
    }
    if (UnknownException.class.isAssignableFrom(exceptionClass)) {
      return newUnknown(cause, retryableHint);
    }
    return newUnknown(cause, retryableHint);
  }

  /**
   * Convenience overload that lets the type's default retryability apply.
   *
   * @see #build(Class, Throwable, Boolean)
   */
  public static SubstrateSdkException build(
      Class<? extends SubstrateSdkException> exceptionClass, Throwable cause) {
    return build(exceptionClass, cause, null);
  }

  private static UnknownException newUnknown(Throwable cause, Boolean retryableHint) {
    return retryableHint == null
        ? new UnknownException(cause)
        : new UnknownException(cause, retryableHint);
  }
}
