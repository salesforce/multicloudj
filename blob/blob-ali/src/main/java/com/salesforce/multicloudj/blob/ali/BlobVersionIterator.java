package com.salesforce.multicloudj.blob.ali;

import static java.util.stream.Collectors.toList;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListVersionsRequest;
import com.aliyun.oss.model.OSSVersionSummary;
import com.aliyun.oss.model.VersionListing;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.ListBlobVersionsRequest;
import com.salesforce.multicloudj.common.Constants;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Iterator that lazily fetches object versions for a specific key, excluding delete markers. */
public class BlobVersionIterator implements Iterator<BlobInfo> {

  private final OSS ossClient;
  private final String bucket;
  private final ListBlobVersionsRequest listRequest;
  private List<BlobInfo> currentBatch;
  private String nextKeyMarker;
  private String nextVersionIdMarker;
  private boolean hasMorePages;
  private int currentIndex;

  public BlobVersionIterator(OSS ossClient, String bucket, ListBlobVersionsRequest listRequest) {
    this.ossClient = ossClient;
    this.bucket = bucket;
    this.listRequest = listRequest;
    this.hasMorePages = true;
    this.currentBatch = nextBatch();
    this.currentIndex = 0;
  }

  private List<BlobInfo> nextBatch() {
    ListVersionsRequest request = new ListVersionsRequest();
    request.setBucketName(bucket);
    request.setPrefix(listRequest.getKey());
    request.setMaxResults(Constants.LIST_BATCH_SIZE);

    if (nextKeyMarker != null) {
      request.setKeyMarker(nextKeyMarker);
    }
    if (nextVersionIdMarker != null) {
      request.setVersionIdMarker(nextVersionIdMarker);
    }

    VersionListing response = ossClient.listVersions(request);

    nextKeyMarker = response.getNextKeyMarker();
    nextVersionIdMarker = response.getNextVersionIdMarker();
    hasMorePages = response.isTruncated();

    return response.getVersionSummaries().stream()
        .filter(v -> !v.isDeleteMarker())
        .filter(v -> v.getKey().equals(listRequest.getKey()))
        .map(
            v ->
                BlobInfo.builder()
                    .withKey(v.getKey())
                    .withVersionId(v.getVersionId())
                    .withIsLatest(v.isLatest())
                    .withObjectSize(v.getSize())
                    .withLastModified(
                        v.getLastModified() != null ? v.getLastModified().toInstant() : null)
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
