package com.salesforce.multicloudj.blob.driver;

import java.util.Objects;

/**
 * Blob info data object
 */
public class BlobInfo {

    private String key;
    private long objectSize;

    private BlobInfo(Builder builder) {
        this.key = builder.key;
        this.objectSize = builder.objectSize;
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
        return objectSize == blobInfo.objectSize && Objects.equals(key, blobInfo.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, objectSize);
    }

    public String getKey() {
        return key;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String key;
        private long objectSize;

        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        public Builder withObjectSize(long objectSize) {
            this.objectSize = objectSize;
            return this;
        }

        public BlobInfo build() {
            return new BlobInfo(this);
        }
    }
}
