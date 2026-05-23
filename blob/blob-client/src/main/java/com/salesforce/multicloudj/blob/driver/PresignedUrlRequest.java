package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.observability.OperationContext;
import java.time.Duration;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

/**
 * This object contains the information necessary to generate Presigned URLs for upload/download
 * requests
 */
@Builder
@Getter
public class PresignedUrlRequest {

  /** The key/name of the blob to access */
  private final String key;

  /** The duration for which this presigned URL will be valid once it's created */
  private final Duration duration;

  /** The type of presignedUrl operation. Currently limited to upload/download */
  private final PresignedOperation type;

  /** Optional: Specify the metadata to be used in a presignedUrl upload */
  private final Map<String, String> metadata;

  /** Optional: Specify the tags to be used in a presignedUrl upload */
  private final Map<String, String> tags;

  /** Optional: Specify the KMS key ID to be used for encryption in a presignedUrl upload */
  private final String kmsKeyId;

  /**
   * Optional: Specify the Content-Disposition header to override in a presigned download URL. 
   */
  private final String contentDisposition;

  /**
   * (Optional) Per-call observability context carrying the correlation ID. If null or if its
   * correlation ID is missing, the SDK auto-generates a UUID for log/trace correlation.
   */
  private final OperationContext operationContext;
}
