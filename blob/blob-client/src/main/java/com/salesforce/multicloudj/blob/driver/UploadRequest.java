package com.salesforce.multicloudj.blob.driver;

import static java.util.Collections.unmodifiableMap;

import java.util.Collections;
import java.util.Map;
import lombok.Getter;

/** Wrapper object for upload data */
@Getter
public class UploadRequest {

  /** (Required parameter) The name of the blob */
  private final String key;

  /**
   * (Optional parameter) The length of the content in bytes. Note that specifying the contentLength
   * can dramatically improve upload efficiency because the substrate SDKs do not need to buffer the
   * contents and calculate it themselves.
   */
  private final long contentLength;

  /**
   * (Optional parameter) The map of metadataName to metadataValue to be associated with the blob
   */
  private final Map<String, String> metadata;

  /** (Optional parameter) The map of tagName to tagValue to be associated with the blob */
  private final Map<String, String> tags;

  /**
   * (Optional parameter) The storage class for the blob (e.g., STANDARD, NEARLINE, COLDLINE,
   * ARCHIVE for GCP)
   */
  private final String storageClass;

  /** (Optional parameter) The KMS key ID or ARN to use for server-side encryption */
  private final String kmsKeyId;

  /**
   * Set the serviceSideEncryption Header but don't set the kmsKeyId. When false and kmsKeyId is
   * null, no SSE headers are sent (bucket default applies). This option will trigger the use of the
   * cloud provider managed key
   */
  private final boolean useKmsManagedKey;

  /**
   * (Optional parameter) The base64-encoded checksum value for upload validation.
   */
  private final String checksumValue;

  /**
   * (Optional parameter) The checksum algorithm used for the checksumValue.
   * Defaults to CRC32C when checksumValue is set but no algorithm is specified.
   */
  private final ChecksumMethod checksumAlgorithm;

  /** (Optional parameter) Object lock configuration for WORM protection. */
  private final ObjectLockConfiguration objectLock;

  /**
   * (Optional parameter) The content type of the blob (e.g., "application/octet-stream",
   * "application/x-directory", "text/plain")
   */
  private final String contentType;

  private UploadRequest(Builder builder) {
    this.key = builder.key;
    this.contentLength = builder.contentLength;
    this.metadata = builder.metadata;
    this.tags = builder.tags;
    this.storageClass = builder.storageClass;
    this.kmsKeyId = builder.kmsKeyId;
    this.useKmsManagedKey = builder.useKmsManagedKey;
    this.objectLock = builder.objectLock;
    this.checksumValue = builder.checksumValue;
    this.checksumAlgorithm = builder.checksumAlgorithm != null
        ? builder.checksumAlgorithm
        : (builder.checksumValue != null ? ChecksumMethod.CRC32C : null);
    this.contentType = builder.contentType;
  }

  public Map<String, String> getMetadata() {
    return metadata == null ? Map.of() : unmodifiableMap(metadata);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String key;
    private long contentLength;
    private Map<String, String> metadata = Collections.emptyMap();
    private Map<String, String> tags = Collections.emptyMap();
    private String storageClass;
    private String kmsKeyId;
    private boolean useKmsManagedKey;
    private ObjectLockConfiguration objectLock;
    private String checksumValue;
    private ChecksumMethod checksumAlgorithm;
    private String contentType;

    public Builder withKey(String key) {
      this.key = key;
      return this;
    }

    public Builder withContentLength(long contentLength) {
      this.contentLength = contentLength;
      return this;
    }

    public Builder withMetadata(Map<String, String> metadata) {
      this.metadata = unmodifiableMap(metadata);
      return this;
    }

    public Builder withTags(Map<String, String> tags) {
      this.tags = unmodifiableMap(tags);
      return this;
    }

    public Builder withStorageClass(String storageClass) {
      this.storageClass = storageClass;
      return this;
    }

    public Builder withKmsKeyId(String kmsKeyId) {
      this.kmsKeyId = kmsKeyId;
      return this;
    }

    /** See {@link UploadRequest#useKmsManagedKey}. */
    public Builder withUseKmsManagedKey(boolean useKmsManagedKey) {
      this.useKmsManagedKey = useKmsManagedKey;
      return this;
    }

    public Builder withObjectLock(ObjectLockConfiguration objectLock) {
      this.objectLock = objectLock;
      return this;
    }

    public Builder withChecksumValue(String checksumValue) {
      this.checksumValue = checksumValue;
      return this;
    }

    public Builder withChecksumAlgorithm(ChecksumMethod checksumAlgorithm) {
      this.checksumAlgorithm = checksumAlgorithm;
      return this;
    }

    public Builder withContentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public UploadRequest build() {
      return new UploadRequest(this);
    }
  }
}
