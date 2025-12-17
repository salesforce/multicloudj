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
    private final String kmsKeyId;

    private DownloadRequest(Builder builder) {
        this.key = builder.key;
        this.versionId = builder.versionId;
        this.start = builder.start;
        this.end = builder.end;
        this.kmsKeyId = builder.kmsKeyId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String key;
        private String versionId;
        private Long start;
        private Long end;
        private String kmsKeyId;

        /**
         * Specifies the key of the Blob to download.
         */
        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        /**
         * (Optional) Specifies the versionId of the blob to download.
         *
         * <p>For buckets without versioning enabled:</p>
         * <ul>
         *     <li>This field has no purpose for non-versioned buckets. Leave it null</li>
         *     <li>Note: Some substrates do return a value for this field, and it can be used in requests,
         *         but it doesn't do anything</li>
         * </ul>
         *
         * <p>For buckets with versioning enabled:</p>
         * <ul>
         *     <li>This field is optional</li>
         *     <li>If you set the value to null then it will target the latest version of the blob</li>
         *     <li>If you set the value to a specific versionId, then it will target that version of the blob</li>
         *     <li>If you use an invalid versionId it will not be able to find your blob</li>
         * </ul>
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

        /**
         * (Optional) Specifies the KMS key ID or ARN to use for decrypting the blob.
         * This is only needed if the blob was encrypted with a customer-managed KMS key.
         */
        public Builder withKmsKeyId(String kmsKeyId) {
            this.kmsKeyId = kmsKeyId;
            return this;
        }

        public DownloadRequest build() {
            return new DownloadRequest(this);
        }
    }
}
