package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectListing;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.stream.Collectors.toList;

/**
 * Iterator object to retrieve BlobInfo list
 */
public class BlobInfoIterator implements Iterator<BlobInfo> {

    private final OSS ossClient;
    private final String bucket;
    private List<BlobInfo> currentBatch;
    private String nextMarker;
    private int currentIndex;
    private final ListBlobsRequest listRequest;

    public BlobInfoIterator(OSS ossClient, String bucket, ListBlobsRequest listRequest) {
        this.ossClient = ossClient;
        this.bucket = bucket;
        this.listRequest = listRequest;
        this.currentBatch = nextBatch();
        this.currentIndex = 0;
    }

    private List<BlobInfo> nextBatch() {
        ListObjectsRequest request = new ListObjectsRequest(bucket).withMarker(nextMarker);

        if (listRequest != null && StringUtils.isNotEmpty(listRequest.getPrefix())) {
            request.withPrefix(listRequest.getPrefix());
        }

        if (listRequest != null && StringUtils.isNotEmpty(listRequest.getDelimiter())) {
            request.withDelimiter(listRequest.getDelimiter());
        }

        ObjectListing response = ossClient.listObjects(request);
        nextMarker = response.getNextMarker();

        return response.getObjectSummaries().stream()
                .map(objSum -> new BlobInfo.Builder()
                        .withKey(objSum.getKey())
                        .withObjectSize(objSum.getSize())
                        .build())
                .collect(toList());
    }

    @Override
    public boolean hasNext() {
        if (currentIndex < currentBatch.size()) {
            return true;
        }

        if (nextMarker != null) {
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
