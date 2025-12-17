package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Wrapper object for upload data
 */
@Getter
public class UploadRequest {

    /**
     * (Required parameter) The name of the blob
     */
    private final String key;
    /**
     * (Optional parameter) The length of the content in bytes.
     * Note that specifying the contentLength can dramatically improve upload efficiency
     * because the substrate SDKs do not need to buffer the contents and calculate it themselves.
     */
    private final long contentLength;
    /**
     * (Optional parameter) The map of metadataName to metadataValue to be associated with the blob
     */
    private final Map<String, String> metadata;
    /**
     * (Optional parameter) The map of tagName to tagValue to be associated with the blob
     */
    private final Map<String, String> tags;
    /**
     * (Optional parameter) The storage class for the blob (e.g., STANDARD, NEARLINE, COLDLINE, ARCHIVE for GCP)
     */
    private final String storageClass;
    /**
     * (Optional parameter) The KMS key ID or ARN to use for server-side encryption
     */
    private final String kmsKeyId;

    private UploadRequest(Builder builder) {
        this.key = builder.key;
        this.contentLength = builder.contentLength;
        this.metadata = builder.metadata;
        this.tags = builder.tags;
        this.storageClass = builder.storageClass;
        this.kmsKeyId = builder.kmsKeyId;
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

        public UploadRequest build() {
            return new UploadRequest(this);
        }
    }
}
