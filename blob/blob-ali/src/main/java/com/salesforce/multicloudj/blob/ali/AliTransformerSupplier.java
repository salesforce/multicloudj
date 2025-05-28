package com.salesforce.multicloudj.blob.ali;

/**
 * Super-simple class to provide instances of AliTransformer that are specific to a given bucket.
 * This helps remove the need to validate that the buckets line up between the client and their transformer.
 */
public class AliTransformerSupplier {

    /**
     * Produces a {@link AliTransformer} specific to the supplied bucket
     * @param bucket the bucket to assign the transformer to.
     * @return the bucket-specific transformer
     */
    public AliTransformer get(String bucket) {
        return new AliTransformer(bucket);
    }
}
