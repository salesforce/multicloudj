package com.salesforce.multicloudj.common.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.BaseServiceException;

/**
 * Classifies GCP SDK exceptions as retryable using the SDK's own native signals.
 *
 * <p>Returns {@code null} when the exception cannot be classified, in which case the multicloudj
 * exception type's default retryability applies.
 *
 * <p>Signals consulted:
 *
 * <ul>
 *   <li>{@link ApiException#isRetryable()} — gax encodes Google's retry guidance per status code.
 *       Used by gRPC- and HTTP-JSON-based service clients (Firestore, PubSub, IAM, KMS, Spanner,
 *       Artifact Registry, etc.).
 *   <li>{@link BaseServiceException#isRetryable()} — common parent of every {@code
 *       google-cloud-*} service exception (Storage, BigQuery, Bigtable, etc.). Encodes the same
 *       retry guidance for clients built on the older {@code BaseService} stack.
 * </ul>
 */
public final class GcpRetryClassifier {

  private GcpRetryClassifier() {}

  /**
   * Walks the cause chain (bounded depth) for the first throwable we can classify.
   *
   * @param t the original throwable from the GCP SDK
   * @return {@code true}/{@code false} when classifiable, otherwise {@code null}
   */
  public static Boolean classify(Throwable t) {
    Throwable current = t;
    int depth = 0;
    while (current != null && depth < 5) {
      if (current instanceof ApiException) {
        return ((ApiException) current).isRetryable();
      }
      if (current instanceof BaseServiceException) {
        return ((BaseServiceException) current).isRetryable();
      }
      current = current.getCause();
      depth++;
    }
    return null;
  }
}
