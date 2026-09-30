package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.models.ListBucketsRequest;
import com.aliyun.sdk.service.oss2.models.ListBucketsResult;
import com.aliyun.sdk.service.oss2.models.PutBucketRequest;
import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.BucketInfo;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import java.util.stream.Collectors;

/**
 * An implementation of the {@link AbstractBlobClient} for Ali OSS AliBlobClient is service client
 * for interacting with Alibaba Cloud Blob Storage.
 *
 * <p>This class provides methods to access Ali resources using Alibaba OSS JDK to interact with the
 * OSS service.
 */
public class AliBlobClient extends AbstractBlobClient<AliBlobClient> implements AliSdkService {
  private OSSClient ossClient;

  /**
   * Constructs an {@link AliBlobClient} using the provided builder.
   *
   * @param builder the builder to use to construct the AliBlobClient
   */
  protected AliBlobClient(Builder builder) {
    super(builder);
    ossClient = buildOSSClient(builder);
  }

  /**
   * Constructs an {@link AliBlobClient} using the provided builder and a pre-built OSS client.
   *
   * @param builder the builder to use to construct the AliBlobClient
   * @param ossClient the pre-built OSS client to use
   */
  protected AliBlobClient(Builder builder, OSSClient ossClient) {
    super(builder);
    this.ossClient = ossClient;
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
    ListBucketsResult result = ossClient.listBuckets(request, OperationOptions.defaults());
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
    ossClient.putBucket(request, OperationOptions.defaults());
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

  /** Closes the underlying OSS client and releases any resources. */
  @Override
  public void close() {
    if (ossClient != null) {
      try {
        ossClient.close();
      } catch (Exception e) {
        throw new SubstrateSdkException("Failed to close Ali OSS client", e);
      }
    }
  }

  private static OSSClient buildOSSClient(Builder builder) {
    CredentialsProvider creds =
        OssCredentialsProvider.getCredentialsProvider(
            builder.getCredentialsOverrider(), builder.getRegion());
    if (creds == null) {
      return null;
    }

    var clientBuilder = OSSClient.newBuilder()
        .region(builder.getRegion())
        .credentialsProvider(creds);

    if (builder.getEndpoint() != null) {
      clientBuilder.endpoint(builder.getEndpoint().toString());
    }
    if (builder.getProxyEndpoint() != null) {
      clientBuilder.proxyHost(
          builder.getProxyEndpoint().getHost()
              + ":" + builder.getProxyEndpoint().getPort());
    }

    return clientBuilder.build();
  }

  public static class Builder extends AbstractBlobClient.Builder<AliBlobClient> {

    public Builder() {
      providerId(AliConstants.PROVIDER_ID);
    }

    @Override
    public AliBlobClient build() {
      return new AliBlobClient(this);
    }

    public AliBlobClient build(OSSClient ossClient) {
      return new AliBlobClient(this, ossClient);
    }
  }
}
