package com.salesforce.multicloudj.blob.ali;

import static java.util.stream.Collectors.toList;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Result;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;

/** Iterator object to retrieve BlobInfo list */
public class BlobInfoIterator implements Iterator<BlobInfo> {

  private final OSSClient ossClient;
  private final AliTransformer transformer;
  private List<BlobInfo> currentBatch;
  private String nextContinuationToken;
  private int currentIndex;
  private final ListBlobsRequest listRequest;

  public BlobInfoIterator(
      OSSClient ossClient, AliTransformer transformer, ListBlobsRequest listRequest) {
    this.ossClient = ossClient;
    this.transformer = transformer;
    this.listRequest = listRequest;
    this.currentBatch = nextBatch();
    this.currentIndex = 0;
  }

  private List<BlobInfo> nextBatch() {
    ListObjectsV2Request request =
        transformer.toListObjectsRequest(listRequest, nextContinuationToken);
    ListObjectsV2Result result =
        ossClient.listObjectsV2(request, OperationOptions.defaults());
    nextContinuationToken = result.nextContinuationToken();

    List<BlobInfo> blobs = result.contents().stream()
        .map(
            objSum ->
                new BlobInfo.Builder()
                    .withKey(objSum.key())
                    .withObjectSize(objSum.size() != null ? objSum.size() : 0L)
                    .withLastModified(objSum.lastModified())
                    .build())
        .collect(toList());
    if (!includesCommonPrefixes() || result.commonPrefixes() == null) {
      return blobs;
    }

    TreeSet<String> commonPrefixes = new TreeSet<>();
    result.commonPrefixes().forEach(prefix -> commonPrefixes.add(prefix.prefix()));
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
