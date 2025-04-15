package com.salesforce.multicloudj.blob.aws;

/**
 * Super-simple class to provide instances of AwsTransformer that are specific to a given bucket.
 * This helps remove the need to validate that the buckets line up between the client and their transformer.
 */
public class AwsTransformerSupplier {

    /**
     * Produces a {@link AwsTransformer} specific to the supplied bucket
     * @param bucket the bucket to assign the transformer to.
     * @return the bucket-specific transformer
     */
    public AwsTransformer get(String bucket) {
        return new AwsTransformer(bucket);
    }
}
