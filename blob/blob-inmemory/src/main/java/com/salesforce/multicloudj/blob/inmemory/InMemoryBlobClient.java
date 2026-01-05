package com.salesforce.multicloudj.blob.inmemory;

import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.BucketInfo;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * An implementation of the {@link AbstractBlobClient} for InMemory storage.
 * InMemoryBlobClient is service client for interacting with InMemory Blob Storage.
 *
 * <p>This class provides methods to interact with in-memory storage for testing purposes.
 *
 * @see AbstractBlobClient
 */
public class InMemoryBlobClient extends AbstractBlobClient<InMemoryBlobClient> {

    private static final String PROVIDER_ID = "memory";
    // Package-private so InMemoryBlobStore can access it
    static final Map<String, BucketMetadata> BUCKETS = new ConcurrentHashMap<>();

    /**
     * Lists all the buckets for this in-memory storage.
     *
     * @return a {@link ListBucketsResponse} containing a list of {@link BucketInfo} objects representing the buckets.
     */
    @Override
    protected ListBucketsResponse doListBuckets() {
        List<BucketInfo> bucketInfoList = BUCKETS.entrySet().stream()
                .map(entry -> BucketInfo.builder()
                        .name(entry.getKey())
                        .region(region)
                        .creationDate(entry.getValue().getCreationDate())
                        .build())
                .collect(Collectors.toList());

        return ListBucketsResponse.builder()
                .bucketInfoList(bucketInfoList)
                .build();
    }

    /**
     * Creates a new bucket with the specified name.
     *
     * @param bucketName The name of the bucket to create
     */
    @Override
    protected void doCreateBucket(String bucketName) {
        BUCKETS.putIfAbsent(bucketName, new BucketMetadata(Instant.now()));
    }

    /**
     * Constructs an instance of {@link InMemoryBlobClient} using the provided builder.
     *
     * @param builder the builder used to configure this client.
     */
    public InMemoryBlobClient(Builder builder) {
        super(builder);
    }

    /**
     * Returns a new instance of {@link Builder}.
     *
     * @return a new instance of {@link Builder}.
     */
    @Override
    public Builder builder() {
        return new Builder();
    }

    /**
     * Returns the appropriate exception class based on the given throwable.
     */
    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        }
        return UnknownException.class;
    }

    /**
     * Closes the client and releases any resources.
     */
    @Override
    public void close() {
        // Nothing to close for in-memory implementation
    }

    // Inner class for bucket metadata
    private static class BucketMetadata {
        private final Instant creationDate;

        public BucketMetadata(Instant creationDate) {
            this.creationDate = creationDate;
        }

        public Instant getCreationDate() {
            return creationDate;
        }
    }

    // Builder
    public static class Builder extends AbstractBlobClient.Builder<InMemoryBlobClient> {

        public Builder() {
            providerId(PROVIDER_ID);
        }

        @Override
        public InMemoryBlobClient build() {
            return new InMemoryBlobClient(this);
        }
    }

    // Public method to clear buckets for testing
    public static void clearBuckets() {
        BUCKETS.clear();
    }
}
