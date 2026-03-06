package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class BucketInfo {
  // Bucket name
  private final String name;

  // Created date.
  private final Instant creationDate;

  // Region
  private final String region;
}
