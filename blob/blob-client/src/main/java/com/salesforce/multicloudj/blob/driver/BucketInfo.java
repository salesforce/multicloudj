package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;

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
