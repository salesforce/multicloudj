package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * Wrapper object for download data
 */
@Getter
public class DownloadRequest {

    private final String key;
    private final String versionId;

    private DownloadRequest(Builder builder) {
        this.key = builder.key;
        this.versionId = builder.versionId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String key;
        private String versionId;

        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        public Builder withVersionId(String versionId) {
            this.versionId = versionId;
            return this;
        }

        public DownloadRequest build() {
            return new DownloadRequest(this);
        }
    }
}
