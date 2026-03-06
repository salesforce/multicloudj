package com.salesforce.multicloudj.blob.driver;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ListBucketsResponse {
  private final List<BucketInfo> bucketInfoList;
}
