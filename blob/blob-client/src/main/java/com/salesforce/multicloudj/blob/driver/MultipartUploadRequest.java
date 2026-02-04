package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * A request object for initiating a multipartUpload request
 */
@Getter
public class MultipartUploadRequest {

    private final String key;
    private Map<String, String> metadata;
    private final Map<String, String> tags;
    private final String kmsKeyId;
    /**
     * (Optional, AWS) When true, request SSE with AWS managed key (aws/s3). See UploadRequest Javadoc for useKmsManagedKey.
     */
    private final boolean useKmsManagedKey;

    private MultipartUploadRequest(final Builder builder){
        this.key = builder.key;
        this.metadata = builder.metadata;
        this.tags = builder.tags;
        this.kmsKeyId = builder.kmsKeyId;
        this.useKmsManagedKey = builder.useKmsManagedKey;
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

        public MultipartUploadRequest build() {
            return new MultipartUploadRequest(this);
        }
    }
}
