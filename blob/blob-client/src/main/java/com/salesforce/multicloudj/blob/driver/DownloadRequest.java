package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * Wrapper object for download data
 */
@Getter
public class DownloadRequest {

    private final String key;
    private final String versionId;
    private final Long start;
    private final Long end;

    private DownloadRequest(Builder builder) {
        this.key = builder.key;
        this.versionId = builder.versionId;
        this.start = builder.start;
        this.end = builder.end;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String key;
        private String versionId;
        private Long start;
        private Long end;

        /**
         * Specifies the key of the Blob to download.
         */
        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        /**
         * Specifies the versionId of the blob to download. This field is optional and only used if your bucket
         * has versioning enabled. This value should be null unless you're targeting a specific key/version blob.
         * If this value is set to null for a versioned bucket, it'll download the latest version of the blob.
         */
        public Builder withVersionId(String versionId) {
            this.versionId = versionId;
            return this;
        }

        /**
         * Specifies the byte range to read from the blob. Inclusive of both start and end bytes.
         * Only specify the byte range if you want to download a specific byte range. If this is not specified
         * it will download the entire blob by default.
         * <pre>
         * Reading the first 500 bytes            - createRangeString(0, 500)
         * Reading a middle 500 bytes             - createRangeString(123, 623)
         * Reading the last 500 bytes             - createRangeString(null, 500)
         * Reading everything but first 500 bytes - createRangeString(500, null)
         * </pre>
         */
        public Builder withRange(Long start, Long end) {
            this.start = start;
            this.end = end;
            return this;
        }

        public DownloadRequest build() {
            return new DownloadRequest(this);
        }
    }
}
