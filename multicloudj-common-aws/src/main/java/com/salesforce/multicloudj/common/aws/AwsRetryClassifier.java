package com.salesforce.multicloudj.common.aws;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;

/**
 * Classifies AWS SDK exceptions as retryable using the SDK's own native signals.
 *
 * <p>Returns {@code null} when the exception cannot be classified, in which case the multicloudj
 * exception type's default retryability applies.
 *
 * <p>Signals consulted, in order:
 *
 * <ol>
 *   <li>{@link RetryableException} — explicit marker the AWS SDK uses to flag retryable failures.
 *   <li>{@link AwsServiceException#isThrottlingException()} — throttling is always retryable.
 *   <li>{@link AwsServiceException#statusCode()} — 5xx responses are retryable; 408 (Request
 *       Timeout) is retryable; 4xx is non-retryable.
 *   <li>{@link SdkClientException} without a status code (e.g. connection reset, DNS) — retryable.
 * </ol>
 */
public final class AwsRetryClassifier {

  private AwsRetryClassifier() {}

  /**
   * Walks the cause chain (bounded depth) for the first throwable we can classify.
   *
   * @param t the original throwable from the AWS SDK
   * @return {@code true}/{@code false} when classifiable, otherwise {@code null}
   */
  public static Boolean classify(Throwable t) {
    Throwable current = t;
    int depth = 0;
    while (current != null && depth < 5) {
      Boolean result = classifyOne(current);
      if (result != null) {
        return result;
      }
      current = current.getCause();
      depth++;
    }
    return null;
  }

  private static Boolean classifyOne(Throwable t) {
    if (t instanceof RetryableException) {
      return true;
    }
    if (t instanceof AwsServiceException) {
      AwsServiceException ase = (AwsServiceException) t;
      if (ase.isThrottlingException()) {
        return true;
      }
      int status = ase.statusCode();
      if (status >= 500 && status <= 599) {
        return true;
      }
      if (status == 408) {
        return true;
      }
      if (status >= 400 && status <= 499) {
        return false;
      }
    }
    if (t instanceof SdkClientException) {
      // Client-side failures from the AWS SDK without a server status (network, DNS, IO) are
      // generally transient.
      return true;
    }
    // Generic SdkException with no further info — no opinion.
    return null;
  }
}
