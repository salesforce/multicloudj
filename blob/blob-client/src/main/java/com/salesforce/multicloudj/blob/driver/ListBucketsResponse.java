package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class ListBucketsResponse {
    private final List<BucketInfo> bucketInfoList;
}
