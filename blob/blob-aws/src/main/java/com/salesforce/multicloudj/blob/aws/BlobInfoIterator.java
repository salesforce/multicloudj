package com.salesforce.multicloudj.blob.aws;

import static java.util.stream.Collectors.toList;

import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.common.Constants;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

/** Iterator object to retrieve BlobInfo list */
public class BlobInfoIterator implements Iterator<BlobInfo> {

  private final S3Client s3Client;
  private final String bucket;
  private List<BlobInfo> currentBatch;
  private String nextContinuationToken;
  private int currentIndex;
  private final ListBlobsRequest listRequest;

  public BlobInfoIterator(S3Client s3Client, String bucket, ListBlobsRequest listRequest) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.listRequest = listRequest;
    this.currentBatch = nextBatch();
    this.currentIndex = 0;
  }

  private List<BlobInfo> nextBatch() {
    ListObjectsV2Request.Builder requestBuilder =
        ListObjectsV2Request.builder().bucket(bucket).maxKeys(Constants.LIST_BATCH_SIZE);

    if (listRequest != null && StringUtils.isNotEmpty(listRequest.getPrefix())) {
      requestBuilder.prefix(listRequest.getPrefix());
    }

    if (listRequest != null && StringUtils.isNotEmpty(listRequest.getDelimiter())) {
      requestBuilder.delimiter(listRequest.getDelimiter());
    }

    if (nextContinuationToken != null) {
      requestBuilder.continuationToken(nextContinuationToken);
    }

    ListObjectsV2Request request = requestBuilder.build();
    ListObjectsV2Response response = s3Client.listObjectsV2(request);

    nextContinuationToken = response.nextContinuationToken();

    List<BlobInfo> blobs = response.contents().stream()
        .map(
            s3Obj ->
                new BlobInfo.Builder()
                    .withKey(s3Obj.key())
                    .withObjectSize(s3Obj.size())
                    .withLastModified(s3Obj.lastModified())
                    .build())
        .collect(toList());
    if (!includesCommonPrefixes()) {
      return blobs;
    }

    TreeSet<String> commonPrefixes = new TreeSet<>();
    response.commonPrefixes().forEach(prefix -> commonPrefixes.add(prefix.prefix()));
    commonPrefixes.forEach(
        prefix -> blobs.add(new BlobInfo.Builder().withKey(prefix).withCommonPrefix(true).build()));
    blobs.sort(Comparator.comparing(BlobInfo::getKey).thenComparing(BlobInfo::isCommonPrefix));
    return blobs;
  }

  private boolean includesCommonPrefixes() {
    return listRequest != null
        && listRequest.isIncludeCommonPrefixes()
        && StringUtils.isNotEmpty(listRequest.getDelimiter());
  }

  @Override
  public boolean hasNext() {
    if (currentIndex < currentBatch.size()) {
      return true;
    }

    while (nextContinuationToken != null) {
      currentBatch = nextBatch();
      currentIndex = 0;
      if (!currentBatch.isEmpty()) {
        return true;
      }
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
