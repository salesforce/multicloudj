package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.models.ListBucketsRequest;
import com.aliyun.sdk.service.oss2.models.ListBucketsResult;
import com.aliyun.sdk.service.oss2.models.PutBucketRequest;
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
 * An implementation of the {@link AbstractBlobClient} for Ali OSS AliBlobClient is service client
 * for interacting with Alibaba Cloud Blob Storage.
 *
 * <p>This class provides methods to access Ali resources using Alibaba OSS JDK to interact with the
 * OSS service.
 */
public class AliBlobClient extends AbstractBlobClient<AliBlobClient> {
  private OSS ossClient;
  private OSSClient ossV2Client;

  /**
   * Constructs an {@link AliBlobClient} using the provided builder.
   *
   * @param builder the builder to use to construct the AliBlobClient
   */
  protected AliBlobClient(Builder builder) {
    super(builder);

    String endpoint = "https://oss-" + region + ".aliyuncs.com";
    if (builder.getEndpoint() != null) {
      endpoint = builder.getEndpoint().toString();
    }
    OSSClientBuilder.OSSClientBuilderImpl ossClientBuilderImpl =
        OSSClientBuilder.create().region(region).endpoint(endpoint);
    ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
    clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
    if (builder.getProxyEndpoint() != null) {
      clientBuilderConfiguration.setProxyHost(builder.getProxyEndpoint().getHost());
      clientBuilderConfiguration.setProxyPort(builder.getProxyEndpoint().getPort());
    }
    CredentialsProvider credentialsProvider =
        OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, region);
    if (credentialsProvider != null) {
      ossClientBuilderImpl.credentialsProvider(credentialsProvider);
    }
    ossClientBuilderImpl.clientConfiguration(clientBuilderConfiguration);
    ossClient = ossClientBuilderImpl.build();
    ossV2Client = buildOSSV2Client(builder);
  }

  /**
   * Constructs an {@link AliBlobClient} using the provided builder and pre-built OSS clients.
   *
   * @param builder the builder to use to construct the AliBlobClient
   * @param ossClient the pre-built v1 OSS client to use
   * @param ossV2Client the pre-built v2 OSS client to use
   */
  protected AliBlobClient(Builder builder, OSS ossClient, OSSClient ossV2Client) {
    super(builder);
    this.ossClient = ossClient;
    this.ossV2Client = ossV2Client;
  }

  /**
   * Lists all buckets in the Alibaba Cloud Blob Storage account associated with this client.
   *
   * @return a {@link ListBucketsResponse} containing a list of all buckets in the current region
   *     for this account
   */
  @Override
  protected ListBucketsResponse doListBuckets() {
    ListBucketsRequest request = ListBucketsRequest.newBuilder().build();
    ListBucketsResult result = ossV2Client.listBuckets(request, OperationOptions.defaults());
    return ListBucketsResponse.builder()
        .bucketInfoList(
            result.buckets().stream()
                .map(
                    bucket ->
                        BucketInfo.builder()
                            .name(bucket.name())
                            .region(bucket.region())
                            .creationDate(bucket.creationDate())
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  /**
   * Creates a new bucket with the specified name.
   *
   * @param bucketName The name of the bucket to create
   */
  @Override
  protected void doCreateBucket(String bucketName) {
    PutBucketRequest request = PutBucketRequest.newBuilder()
        .bucket(bucketName)
        .build();
    ossV2Client.putBucket(request, OperationOptions.defaults());
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

  /** Returns the appropriate SubstrateSdkException subclass for the given Throwable. */
  @Override
  public Class<? extends SubstrateSdkException> getException(Throwable t) {
    if (t instanceof SubstrateSdkException) {
      return (Class<? extends SubstrateSdkException>) t.getClass();
    } else if (t instanceof ServiceException) {
      String errorCode = ((ServiceException) t).getErrorCode();
      return ErrorCodeMapping.getException(errorCode);
    } else if (t instanceof ClientException || t instanceof IllegalArgumentException) {
      return InvalidArgumentException.class;
    }
    return UnknownException.class;
  }

  /** Closes the underlying OSS clients and releases any resources. */
  @Override
  public void close() {
    if (ossClient != null) {
      ossClient.shutdown();
    }
    if (ossV2Client != null) {
      try {
        ossV2Client.close();
      } catch (Exception e) {
        throw new SubstrateSdkException("Failed to close Ali OSS v2 client", e);
      }
    }
  }

  private static OSSClient buildOSSV2Client(Builder builder) {
    com.aliyun.sdk.service.oss2.credentials.CredentialsProvider v2Creds =
        OSSCredentialsProvider.getV2CredentialsProvider(builder.getCredentialsOverrider());
    if (v2Creds == null) {
      return null;
    }

    var v2Builder = OSSClient.newBuilder()
        .region(builder.getRegion())
        .credentialsProvider(v2Creds);

    if (builder.getEndpoint() != null) {
      v2Builder.endpoint(builder.getEndpoint().toString());
    }
    if (builder.getProxyEndpoint() != null) {
      v2Builder.proxyHost(
          builder.getProxyEndpoint().getHost()
              + ":" + builder.getProxyEndpoint().getPort());
    }

    return v2Builder.build();
  }

  public static class Builder extends AbstractBlobClient.Builder<AliBlobClient> {

    public Builder() {
      providerId(AliConstants.PROVIDER_ID);
    }

    @Override
    public AliBlobClient build() {
      return new AliBlobClient(this);
    }

    public AliBlobClient build(OSS ossClient, OSSClient ossV2Client) {
      return new AliBlobClient(this, ossClient, ossV2Client);
    }
  }
}
