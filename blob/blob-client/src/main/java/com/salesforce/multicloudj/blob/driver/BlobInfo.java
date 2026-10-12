package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import java.util.Objects;

/** Blob info data object */
public class BlobInfo {

  private String key;
  private long objectSize;
  private Instant lastModified;
  private boolean commonPrefix;

  private BlobInfo(Builder builder) {
    this.key = builder.key;
    this.objectSize = builder.objectSize;
    this.lastModified = builder.lastModified;
    this.commonPrefix = builder.commonPrefix;
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
    return objectSize == blobInfo.objectSize
        && commonPrefix == blobInfo.commonPrefix
        && Objects.equals(key, blobInfo.key)
        && Objects.equals(lastModified, blobInfo.lastModified);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, objectSize, lastModified, commonPrefix);
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

  /** Returns whether this entry represents a common prefix instead of an object. */
  public boolean isCommonPrefix() {
    return commonPrefix;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String key;
    private long objectSize;
    private Instant lastModified;
    private boolean commonPrefix;

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

    public Builder withCommonPrefix(boolean commonPrefix) {
      this.commonPrefix = commonPrefix;
      return this;
    }

    public BlobInfo build() {
      return new BlobInfo(this);
    }
  }
}
