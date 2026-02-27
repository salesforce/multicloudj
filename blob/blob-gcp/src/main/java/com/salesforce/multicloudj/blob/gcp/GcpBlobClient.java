package com.salesforce.multicloudj.blob.gcp;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.auto.service.AutoService;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.BucketInfo;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of the {@link AbstractBlobClient} for GCP Cloud Storage.
 * GcpBlobClient is service client for interacting with GCP Cloud Blob Storage.
 *
 * <p>This class provides methods to interact with GCP resources using GCP SDK for Java to interact
 * with the Cloud Storage service.
 *
 * @see AbstractBlobClient
 */
@AutoService(AbstractBlobClient.class)
public class GcpBlobClient extends AbstractBlobClient<GcpBlobClient> {
    private final Storage storage;

    /**
     * Lists all the buckets for this authenticated account.
     *
     * @return a {@link ListBucketsResponse} containing a list of {@link BucketInfo} objects representing the buckets
     * for this account.
     */
    @Override
    protected ListBucketsResponse doListBuckets() {
        List<BucketInfo> bucketInfoList = new ArrayList<>();

        for (Bucket bucket : storage.list().iterateAll()) {
            BucketInfo bucketInfo = BucketInfo.builder()
                    .name(bucket.getName())
                    .region(bucket.getLocation())
                    .creationDate(Instant.ofEpochMilli(bucket.getCreateTimeOffsetDateTime().toInstant().toEpochMilli()))
                    .build();
            bucketInfoList.add(bucketInfo);
        }

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
        storage.create(Bucket.newBuilder(bucketName).build());
    }

    /**
     * Constructs an instance of {@link GcpBlobClient} using the provided builder and Storage client.
     *
     * @param builder  the builder used to configure this client.
     */
    public GcpBlobClient(Builder builder) {
        this(builder, buildStorage(builder));
    }

    /**
     * Constructs an instance of {@link GcpBlobClient} using the provided builder and Storage client.
     *
     * @param builder  the builder used to configure this client.
     * @param storage the Storage client used to communicate with the Cloud Storage service.
     */
    public GcpBlobClient(Builder builder, Storage storage) {
        super(builder);
        this.storage = storage;
    }

    private static Storage buildStorage(Builder builder) {
        StorageOptions.Builder storageBuilder = StorageOptions.newBuilder();

        Credentials credentials = GcpCredentialsProvider.getCredentials(builder.getCredentialsOverrider());
        if (credentials != null) {
            storageBuilder.setCredentials(credentials);
        }

        if (builder.getEndpoint() != null) {
            storageBuilder.setHost(builder.getEndpoint().toString());
        }

        if (builder.getProxyEndpoint() != null) {
            org.apache.http.HttpHost proxy = new org.apache.http.HttpHost(
                    builder.getProxyEndpoint().getHost(),
                    builder.getProxyEndpoint().getPort()
            );
            org.apache.http.impl.client.HttpClientBuilder httpClientBuilder = org.apache.http.impl.client.HttpClientBuilder.create()
                    .setProxy(proxy);
            storageBuilder.setTransportOptions(HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> new ApacheHttpTransport(httpClientBuilder.build()))
                    .build());
        }

        if (builder.getRetryConfig() != null) {
            GcpTransformer transformer = new GcpTransformer(null);
            storageBuilder.setRetrySettings(transformer.toGcpRetrySettings(builder.getRetryConfig()));
        }

        return storageBuilder.build().getService();
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
        } else if (t instanceof ApiException) {
            ApiException exception = (ApiException) t;
            StatusCode statusCode = exception.getStatusCode();
            return CommonErrorCodeMapping.getException(statusCode.getCode());
        } else if (t instanceof StorageException) {
            return CommonErrorCodeMapping.getException(((StorageException) t).getCode());
        } else if(t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }

    /**
     * Closes the underlying GCP Storage client and releases any resources.
     */
    @Override
    public void close() {
        try {
            if (storage != null) {
                storage.close();
            }
        } catch (Exception e) {
            throw new SubstrateSdkException("Failed to close GCP Storage client", e);
        }
    }

    public static class Builder extends AbstractBlobClient.Builder<GcpBlobClient> {

        public Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

        @Override
        public GcpBlobClient build() {
            return new GcpBlobClient(this);
        }

        public GcpBlobClient build(Storage storage) {
            return new GcpBlobClient(this, storage);
        }
    }
}
