package com.salesforce.multicloudj.common.ali;

/**
 * Retryability helpers for Alibaba Cloud SDK exceptions.
 *
 * <p>Unlike GCP ({@code BaseServiceException}) or AWS ({@code AwsServiceException}), Alibaba's
 * various per-service SDKs (OSS v2, Tablestore, HBR/Tea, aliyuncs-core) do NOT share a common
 * exception base class — every SDK has its own exception types with different shapes. There is no
 * generic {@code classify(Throwable)} method that can dispatch over them.
 *
 * <p>Instead, each provider's {@code mapException} extracts the typed exception it cares about and
 * calls one of the helpers below directly. This keeps each service module's classpath tight (no
 * cross-pollination of OSS v2 / Tablestore / Tea jars) and keeps this common module free of any
 * service-specific knowledge.
 */
public final class AliRetryClassifier {

  private AliRetryClassifier() {}

  /**
   * Classifies an HTTP status code as retryable.
   *
   * @param httpStatus HTTP status code from a service exception
   * @return {@code true} for 5xx and the retryable 4xx codes (408 Request Timeout, 429 Too Many
   *     Requests); {@code false} for other 4xx; {@code null} otherwise
   */
  public static Boolean classifyByStatusCode(int httpStatus) {
    if (httpStatus >= 500 && httpStatus <= 599) {
      return true;
    }
    if (httpStatus == 408 || httpStatus == 429) {
      return true;
    }
    if (httpStatus >= 400 && httpStatus <= 499) {
      return false;
    }
    return null;
  }

  /**
   * Convenience overload accepting a nullable {@link Integer}. Returns {@code null} when {@code
   * httpStatus} is null (e.g., {@code TeaException.getStatusCode()} returning a missing status).
   */
  public static Boolean classifyByStatusCode(Integer httpStatus) {
    return httpStatus == null ? null : classifyByStatusCode(httpStatus.intValue());
  }
}
