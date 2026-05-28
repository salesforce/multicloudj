package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

/** Iterator object to retrieve BlobMetadata versions for an exact key. */
public class BlobMetadataIterator implements Iterator<BlobMetadata> {

  private final String key;
  private final Iterator<ListObjectVersionsResponse> responseIterator;
  private Iterator<ObjectVersion> current = List.<ObjectVersion>of().iterator();
  private BlobMetadata next;

  public BlobMetadataIterator(S3Client s3Client, String bucket, String key) {
    this.key = key;
    this.responseIterator =
        s3Client
            .listObjectVersionsPaginator(
                ListObjectVersionsRequest.builder().bucket(bucket).prefix(key).build())
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
        // S3's prefix filter returns keys that START with the prefix, not exact matches.
        if (!key.equals(version.key())) {
          continue;
        }

        next =
            BlobMetadata.builder()
                .key(version.key())
                .versionId(version.versionId())
                .eTag(version.eTag())
                .objectSize(version.size())
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
