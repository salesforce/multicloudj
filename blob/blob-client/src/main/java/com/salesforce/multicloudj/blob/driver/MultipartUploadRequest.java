package com.salesforce.multicloudj.blob.driver;

import static java.util.Collections.unmodifiableMap;

import java.util.Collections;
import java.util.Map;
import lombok.Getter;

/** A request object for initiating a multipartUpload request */
@Getter
public class MultipartUploadRequest {

  private final String key;
  private Map<String, String> metadata;
  private final Map<String, String> tags;
  private final String kmsKeyId;
  private final boolean useKmsManagedKey;
  private final boolean checksumEnabled;
  private final ChecksumAlgorithm checksumAlgorithm;
  private final String contentType;

  private MultipartUploadRequest(final Builder builder) {
    this.key = builder.key;
    this.metadata = builder.metadata;
    this.tags = builder.tags;
    this.kmsKeyId = builder.kmsKeyId;
    this.useKmsManagedKey = builder.useKmsManagedKey;
    this.checksumAlgorithm = builder.checksumAlgorithm != null
        ? builder.checksumAlgorithm
        : (builder.checksumEnabled ? ChecksumAlgorithm.CRC32C : null);
    this.checksumEnabled = this.checksumAlgorithm != null;
    this.contentType = builder.contentType;
  }

  public Map<String, String> getMetadata() {
    return metadata == null ? Map.of() : unmodifiableMap(metadata);
  }

  public Map<String, String> getTags() {
    return tags == null ? Map.of() : unmodifiableMap(tags);
  }

  public static class Builder {
    private String key;
    private Map<String, String> metadata = Collections.emptyMap();
    private Map<String, String> tags = Collections.emptyMap();
    private String kmsKeyId;
    private boolean useKmsManagedKey;
    private boolean checksumEnabled;
    private ChecksumAlgorithm checksumAlgorithm;
    private String contentType;

    public Builder withKey(String key) {
      this.key = key;
      return this;
    }

    public Builder withMetadata(final Map<String, String> metadata) {
      this.metadata = unmodifiableMap(metadata);
      return this;
    }

    public Builder withTags(final Map<String, String> tags) {
      this.tags = unmodifiableMap(tags);
      return this;
    }

    public Builder withKmsKeyId(String kmsKeyId) {
      this.kmsKeyId = kmsKeyId;
      return this;
    }

    public Builder withUseKmsManagedKey(boolean useKmsManagedKey) {
      this.useKmsManagedKey = useKmsManagedKey;
      return this;
    }

    public Builder withChecksumEnabled(boolean checksumEnabled) {
      this.checksumEnabled = checksumEnabled;
      return this;
    }

    public Builder withChecksumAlgorithm(ChecksumAlgorithm checksumAlgorithm) {
      this.checksumAlgorithm = checksumAlgorithm;
      return this;
    }

    public Builder withContentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public MultipartUploadRequest build() {
      return new MultipartUploadRequest(this);
    }
  }
}
