package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import java.util.Objects;

/**
 * Blob info data object
 */
public class BlobInfo {

    private String key;
    private long objectSize;
    private Instant lastModified;

    private BlobInfo(Builder builder) {
        this.key = builder.key;
        this.objectSize = builder.objectSize;
        this.lastModified = builder.lastModified;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BlobInfo blobInfo = (BlobInfo) obj;
        return objectSize == blobInfo.objectSize && Objects.equals(key, blobInfo.key) && Objects.equals(lastModified, blobInfo.lastModified);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, objectSize, lastModified);
    }

    public String getKey() {
        return key;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public Instant getLastModified() {
        return lastModified;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String key;
        private long objectSize;
        private Instant lastModified;

        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        public Builder withObjectSize(long objectSize) {
            this.objectSize = objectSize;
            return this;
        }

        public Builder withLastModified(Instant lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        public BlobInfo build() {
            return new BlobInfo(this);
        }
    }
}
