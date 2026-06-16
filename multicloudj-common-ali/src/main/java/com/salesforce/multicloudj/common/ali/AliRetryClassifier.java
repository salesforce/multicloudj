package com.salesforce.multicloudj.common.ali;

import java.util.Set;

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
 * cross-pollination of OSS v2 / Tablestore / Tea jars) while still delegating the policy
 * (status-code interpretation, throttling-code recognition) to a single source of truth.
 */
public final class AliRetryClassifier {

  /** Tablestore error codes that indicate throttling and are always retryable. */
  private static final Set<String> RETRYABLE_TABLESTORE_ERROR_CODES =
      Set.of(
          "OTSServerBusy",
          "OTSPartitionUnavailable",
          "OTSTimeout",
          "OTSServerUnavailable",
          "OTSInternalServerError",
          "OTSQuotaExhausted",
          "OTSRequestTimeout");

  private AliRetryClassifier() {}

  /**
   * Classifies an HTTP status code as retryable.
   *
   * @param httpStatus HTTP status code from a service exception
   * @return {@code true} for 5xx and 408, {@code false} for other 4xx, {@code null} otherwise
   */
  public static Boolean classifyByStatusCode(int httpStatus) {
    if (httpStatus >= 500 && httpStatus <= 599) {
      return true;
    }
    if (httpStatus == 408) {
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

  /**
   * @param errorCode error code from a Tablestore exception
   * @return {@code true} if the error code identifies a known retryable throttling/transient
   *     failure, otherwise {@code null}
   */
  public static Boolean classifyByThrottlingErrorCode(String errorCode) {
    if (errorCode == null) {
      return null;
    }
    if (RETRYABLE_TABLESTORE_ERROR_CODES.contains(errorCode)) {
      return true;
    }
    return null;
  }
}
