package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.comm.SignVersion;
import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.BucketInfo;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.provider.Provider;

import java.util.stream.Collectors;

/**
 * An implementation of the {@link AbstractBlobClient} for Ali OSS
 * AliBlobClient is service client for interacting with Alibaba Cloud Blob Storage.
 *
 * <p>This class provides methods to access Ali resources using Alibaba OSS JDK to interact
 * with the OSS service.
 */
public class AliBlobClient extends AbstractBlobClient<AliBlobClient> {
    private OSS ossClient;

    /**
     * Constructs an {@link AliBlobClient} using the provided builder.
     *
     * @param builder the builder to use to construct the AliBlobClient
     */
    protected AliBlobClient(Builder builder) {
        super(builder);

        String endpoint = "https://oss-" + region + ".aliyuncs.com";
        if (builder.getEndpoint() != null) {
            endpoint = builder.getEndpoint().getHost();
        }
        OSSClientBuilder.OSSClientBuilderImpl ossClientBuilderImpl = OSSClientBuilder.create().endpoint(endpoint);
        ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
        clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
        if (builder.getProxyEndpoint() != null) {
            clientBuilderConfiguration.setProxyHost(builder.getProxyEndpoint().getHost());
            clientBuilderConfiguration.setProxyPort(builder.getProxyEndpoint().getPort());
        }
        CredentialsProvider credentialsProvider = OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, region);
        if (credentialsProvider != null) {
            ossClientBuilderImpl.credentialsProvider(credentialsProvider);
        }
        ossClientBuilderImpl.clientConfiguration(clientBuilderConfiguration);
        ossClient = ossClientBuilderImpl.build();
    }

    /**
     * Lists all buckets in the Alibaba Cloud Blob Storage account associated with this client.
     *
     * @return a {@link ListBucketsResponse} containing a list of all buckets int the current region for this account
     */
    @Override
    protected ListBucketsResponse doListBuckets() {
        return ListBucketsResponse.builder().bucketInfoList(ossClient.listBuckets().stream().map(bucket -> BucketInfo.builder()
                .name(bucket.getName())
                .region(bucket.getRegion())
                .creationDate(bucket.getCreationDate().toInstant())
                .build()).collect(Collectors.toList())).build();
    }

    /**
     * Returns a {@link Provider.Builder} for creating a Provider for this class.
     *
     * @return a {@link Provider.Builder} for creating a Provider for this class
     */
    @Override
    public Provider.Builder builder() {
        return new AliBlobStore.Builder();
    }

    /**
     * Returns the appropriate SubstrateSdkException subclass for the given Throwable.
     */
    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        } else if (t instanceof ServiceException) {
            String errorCode = ((ServiceException) t).getErrorCode();
            return ErrorCodeMapping.getException(errorCode);
        } else if (t instanceof ClientException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }

    public static class Builder extends AbstractBlobClient.Builder<AliBlobClient> {

        public Builder() {
            providerId(AliConstants.PROVIDER_ID);
        }

        @Override
        public AliBlobClient build() {
            return new AliBlobClient(this);
        }
    }
}
