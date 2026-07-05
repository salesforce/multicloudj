package com.salesforce.multicloudj.common.service;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;

/**
 * Base interface for services backed by substrate-specific implementations. This provides a minimal
 * set of methods such as exception translation.
 */
public interface SdkService {

  /**
   * Retrieves the unique identifier for this service implementation.
   *
   * @return A String representing the provider's ID such as aws.
   */
  String getProviderId();

  /**
   * Maps a provider-specific {@link Throwable} to a fully-built {@link SubstrateSdkException} the
   * caller can throw directly. The returned exception carries both the typed mapping (e.g. {@code
   * ResourceNotFoundException} for HTTP 404) and an authoritative retryable flag derived from the
   * native cloud SDK signal (e.g. AWS throttling, GCP {@code ApiException.isRetryable()}, status
   * codes), or the type's default retryability when the SDK exposes no native signal.
   *
   * <p>Callers throw the result; they do not inspect or transform it further:
   *
   * <pre>{@code
   * try {
   *   ...
   * } catch (Throwable t) {
   *   throw provider.mapException(t);
   * }
   * }</pre>
   *
   * <p><b>Implementation contract:</b> implementations should compute a typed exception class
   * (typically via their per-service {@code ErrorCodeMapping}) and a retryable hint, then return
   * {@code ExceptionHandler.build(exceptionClass, t, retryableHint)}. Implementations do
   * <i>not</i> need to short-circuit when {@code t} is already a {@link SubstrateSdkException}:
   * {@code ExceptionHandler.build} preserves an already-mapped exception's concrete subtype and
   * retryable flag.
   *
   * @param t the original throwable from the provider SDK
   * @return a built {@link SubstrateSdkException} ready to throw
   */
  SubstrateSdkException mapException(Throwable t);
}
