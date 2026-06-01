package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsRequest;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsResult;
import com.aliyun.sdk.service.oss2.models.ObjectVersion;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Iterator object to retrieve BlobMetadata versions for an exact key. */
public class BlobMetadataIterator implements Iterator<BlobMetadata> {

  private final String key;
  private final Iterator<ListObjectVersionsResult> responseIterator;
  private Iterator<ObjectVersion> current = List.<ObjectVersion>of().iterator();
  private BlobMetadata next;

  public BlobMetadataIterator(OSSClient ossClient, String bucket, String key) {
    this.key = key;
    this.responseIterator =
        ossClient
            .listObjectVersionsPaginator(
                ListObjectVersionsRequest.newBuilder().bucket(bucket).prefix(key).build())
            .iterator();
  }

  @Override
  public boolean hasNext() {
    if (next != null) {
      return true;
    }

    while (true) {
      while (current.hasNext()) {
        ObjectVersion version = current.next();
        if (!key.equals(version.key())) {
          continue;
        }

        next =
            BlobMetadata.builder()
                .key(version.key())
                .versionId(version.versionId())
                .eTag(version.eTag())
                .objectSize(version.size() != null ? version.size() : 0L)
                .lastModified(version.lastModified())
                .build();
        return true;
      }

      if (!responseIterator.hasNext()) {
        return false;
      }
      current = responseIterator.next().versions().iterator();
    }
  }

  @Override
  public BlobMetadata next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    BlobMetadata result = next;
    next = null;
    return result;
  }
}
