package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * A request object for initiating a multipartUpload request
 */
@Getter
public class MultipartUploadRequest {

    private final String key;
    private Map<String, String> metadata;

    private MultipartUploadRequest(final Builder builder){
        this.key = builder.key;
        this.metadata = builder.metadata;
    }

    public static class Builder {
        private String key;
        private Map<String, String> metadata;

        public MultipartUploadRequest.Builder withKey(String key) {
            this.key = key;
            return this;
        }

        public MultipartUploadRequest.Builder withMetadata(final Map<String, String> metadata) {
            this.metadata = unmodifiableMap(metadata);
            return this;
        }

        public MultipartUploadRequest build() {
            return new MultipartUploadRequest(this);
        }
    }
}
