package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import java.util.Objects;

/** Blob info data object */
public class BlobInfo {

  private String key;
  private long objectSize;
  private Instant lastModified;
  private String versionId;
  private Boolean isLatest;

  private BlobInfo(Builder builder) {
    this.key = builder.key;
    this.objectSize = builder.objectSize;
    this.lastModified = builder.lastModified;
    this.versionId = builder.versionId;
    this.isLatest = builder.isLatest;
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
        && Objects.equals(key, blobInfo.key)
        && Objects.equals(lastModified, blobInfo.lastModified)
        && Objects.equals(versionId, blobInfo.versionId)
        && Objects.equals(isLatest, blobInfo.isLatest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, objectSize, lastModified, versionId, isLatest);
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

  public String getVersionId() {
    return versionId;
  }

  public Boolean getIsLatest() {
    return isLatest;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String key;
    private long objectSize;
    private Instant lastModified;
    private String versionId;
    private Boolean isLatest;

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

    public Builder withVersionId(String versionId) {
      this.versionId = versionId;
      return this;
    }

    public Builder withIsLatest(Boolean isLatest) {
      this.isLatest = isLatest;
      return this;
    }

    public BlobInfo build() {
      return new BlobInfo(this);
    }
  }
}
