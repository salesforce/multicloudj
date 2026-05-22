package com.salesforce.multicloudj.blob.aws;

import static java.util.stream.Collectors.toList;

import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.ListBlobVersionsRequest;
import com.salesforce.multicloudj.common.Constants;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;

/** Iterator that lazily fetches object versions for a specific key, excluding delete markers. */
public class BlobVersionIterator implements Iterator<BlobInfo> {

  private final S3Client s3Client;
  private final String bucket;
  private final ListBlobVersionsRequest listRequest;
  private List<BlobInfo> currentBatch;
  private String nextKeyMarker;
  private String nextVersionIdMarker;
  private boolean hasMorePages;
  private int currentIndex;

  public BlobVersionIterator(
      S3Client s3Client, String bucket, ListBlobVersionsRequest listRequest) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.listRequest = listRequest;
    this.hasMorePages = true;
    this.currentBatch = nextBatch();
    this.currentIndex = 0;
  }

  private List<BlobInfo> nextBatch() {
    ListObjectVersionsRequest.Builder requestBuilder =
        ListObjectVersionsRequest.builder()
            .bucket(bucket)
            .prefix(listRequest.getKey())
            .maxKeys(Constants.LIST_BATCH_SIZE);

    if (nextKeyMarker != null) {
      requestBuilder.keyMarker(nextKeyMarker);
    }
    if (nextVersionIdMarker != null) {
      requestBuilder.versionIdMarker(nextVersionIdMarker);
    }

    ListObjectVersionsResponse response =
        s3Client.listObjectVersions(requestBuilder.build());

    nextKeyMarker = response.nextKeyMarker();
    nextVersionIdMarker = response.nextVersionIdMarker();
    hasMorePages = Boolean.TRUE.equals(response.isTruncated());

    return response.versions().stream()
        .filter(v -> v.key().equals(listRequest.getKey()))
        .map(
            v ->
                BlobInfo.builder()
                    .withKey(v.key())
                    .withVersionId(v.versionId())
                    .withIsLatest(v.isLatest())
                    .withObjectSize(v.size())
                    .withLastModified(v.lastModified())
                    .build())
        .collect(toList());
  }

  @Override
  public boolean hasNext() {
    if (currentIndex < currentBatch.size()) {
      return true;
    }

    if (hasMorePages) {
      currentBatch = nextBatch();
      currentIndex = 0;
      return !currentBatch.isEmpty();
    }

    return false;
  }

  @Override
  public BlobInfo next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return currentBatch.get(currentIndex++);
  }
}
