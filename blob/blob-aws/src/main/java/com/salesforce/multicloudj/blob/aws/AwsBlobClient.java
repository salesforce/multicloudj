package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.BucketInfo;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.time.Duration;
import java.util.stream.Collectors;

/**
 * An implementation of the {@link AbstractBlobClient} for AWS S3.
 * AwsBlobClient is service client for interacting with AWS Cloud Blob Storage.
 *
 * <p>This class provides methods to interact with AWS resources using AWS SDK for Java to interact
 * with the S3 service.
 *
 * @see AbstractBlobClient
 */
public class AwsBlobClient extends AbstractBlobClient<AwsBlobClient> {
    private final S3Client s3Client;

    /**
     * Lists all the buckets in the current region for this authenticated account.
     *
     * @return a {@link ListBucketsResponse} containing a list of {@link BucketInfo} objects representing the buckets in
     * the current region for this account.
     */
    @Override
    protected ListBucketsResponse doListBuckets() {
        software.amazon.awssdk.services.s3.model.ListBucketsResponse response = s3Client.listBuckets();


        return ListBucketsResponse.builder().bucketInfoList(response.buckets().stream().map(bucket -> BucketInfo.builder()
                .region(bucket.bucketRegion())
                .name(bucket.name())
                .creationDate(bucket.creationDate())
                .build()).collect(Collectors.toList())).build();
    }

    /**
     * Creates a new bucket with the specified name.
     *
     * @param bucketName The name of the bucket to create
     */
    @Override
    protected void doCreateBucket(String bucketName) {
        s3Client.createBucket(builder -> builder.bucket(bucketName));
    }

    /**
     * Constructs an instance of {@link AwsBlobClient} using the provided builder and S3 client.
     *
     * @param builder  the builder used to configure this client.
     */
    public AwsBlobClient(Builder builder) {
        this(builder, buildS3Client(builder));
    }

    /**
     * Constructs an instance of {@link AwsBlobClient} using the provided builder and S3 client.
     *
     * @param builder  the builder used to configure this client.
     * @param s3Client the S3 client used to communicate with the S3 service.
     */
    public AwsBlobClient(Builder builder, S3Client s3Client) {
        super(builder);
        this.s3Client = s3Client;
    }

    private static S3Client buildS3Client(Builder builder) {
        Region regionObj = Region.of(builder.getRegion());
        S3ClientBuilder b = S3Client.builder();
        b.region(regionObj);

        AwsCredentialsProvider credentialsProvider = CredentialsProvider.getCredentialsProvider(builder.getCredentialsOverrider(), regionObj);
        if (credentialsProvider != null) {
            b.credentialsProvider(credentialsProvider);
        }
        if (builder.getEndpoint() != null) {
            b.endpointOverride(builder.getEndpoint());
        }
        if (builder.getProxyEndpoint() != null) {
            ProxyConfiguration proxyConfig = ProxyConfiguration.builder()
                    .endpoint(builder.getProxyEndpoint())
                    .build();
            b.httpClient(ApacheHttpClient.builder()
                    .proxyConfiguration(proxyConfig)
                    .build());
        }
        if (builder.getRetryConfig() != null) {
            // Create a temporary transformer instance for retry strategy conversion
            AwsTransformer transformer = new AwsTransformer(null);
            b.overrideConfiguration(config -> {
                config.retryStrategy(transformer.toAwsRetryStrategy(builder.getRetryConfig()));
                // Set API call timeouts if provided
                if (builder.getRetryConfig().getAttemptTimeout() != null) {
                    config.apiCallAttemptTimeout(Duration.ofMillis(builder.getRetryConfig().getAttemptTimeout()));
                }
                if (builder.getRetryConfig().getTotalTimeout() != null) {
                    config.apiCallTimeout(Duration.ofMillis(builder.getRetryConfig().getTotalTimeout()));
                }
            });
        }

        return b.build();
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
        } else if (t instanceof AwsServiceException) {
            String errorCode = ((AwsServiceException) t).awsErrorDetails().errorCode();
            return ErrorCodeMapping.getException(errorCode);
        } else if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }


    public static class Builder extends AbstractBlobClient.Builder<AwsBlobClient> {

        public Builder() {
            providerId(AwsConstants.PROVIDER_ID);
        }

        @Override
        public AwsBlobClient build() {
            return new AwsBlobClient(this);
        }

        public AwsBlobClient build(S3Client s3Client) {
            return new AwsBlobClient(this, s3Client);
        }
    }
}
