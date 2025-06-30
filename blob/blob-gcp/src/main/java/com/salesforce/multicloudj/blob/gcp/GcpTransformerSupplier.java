package com.salesforce.multicloudj.blob.gcp;

/**
 * Super-simple class to provide instances of GcpTransformer that are specific to a given bucket.
 * This helps remove the need to validate that the buckets line up between the client and their transformer.
 */
public class GcpTransformerSupplier {

    /**
     * Produces a {@link GcpTransformer} specific to the supplied bucket
     * @param bucket the bucket to assign the transformer to.
     * @return the bucket-specific transformer
     */
    public GcpTransformer get(String bucket) {
        return new GcpTransformer(bucket);
    }
}
