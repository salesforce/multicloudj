package com.salesforce.multicloudj.blob.ali;

import static java.util.stream.Collectors.toList;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Result;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Iterator object to retrieve BlobInfo list */
public class BlobInfoIterator implements Iterator<BlobInfo> {

  private final OSSClient ossV2Client;
  private final AliTransformer transformer;
  private List<BlobInfo> currentBatch;
  private String nextContinuationToken;
  private int currentIndex;
  private final ListBlobsRequest listRequest;

  public BlobInfoIterator(
      OSSClient ossV2Client, AliTransformer transformer, ListBlobsRequest listRequest) {
    this.ossV2Client = ossV2Client;
    this.transformer = transformer;
    this.listRequest = listRequest;
    this.currentBatch = nextBatch();
    this.currentIndex = 0;
  }

  private List<BlobInfo> nextBatch() {
    ListObjectsV2Request request =
        transformer.toV2ListObjectsRequest(listRequest, nextContinuationToken);
    ListObjectsV2Result result =
        ossV2Client.listObjectsV2(request, OperationOptions.defaults());
    nextContinuationToken = result.nextContinuationToken();

    return result.contents().stream()
        .map(
            objSum ->
                new BlobInfo.Builder()
                    .withKey(objSum.key())
                    .withObjectSize(objSum.size() != null ? objSum.size() : 0L)
                    .build())
        .collect(toList());
  }

  @Override
  public boolean hasNext() {
    if (currentIndex < currentBatch.size()) {
      return true;
    }

    if (nextContinuationToken != null) {
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
