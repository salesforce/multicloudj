package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.BlobSpanNames;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.observability.MultiCloudJLogger;
import com.salesforce.multicloudj.common.observability.TracingPolicy;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;
import java.util.Map;

/**
 * Entry point for Client code to interact with the Blob service. This is a Service client, which
 * can be used to interact with the blob service, unlike {@link BucketClient} that interacts with
 * buckets.
 *
 * <p>This class serves the purpose of providing common (i.e. substrate-agnostic) service
 * functionality.
 */
public class BlobClient implements AutoCloseable {

  private static final String SDK_SERVICE = "blob";

  protected AbstractBlobClient<?> blobClient;
  protected final MultiCloudJLogger multiCloudJLogger;

  protected BlobClient(AbstractBlobClient<?> blobClient) {
    this(blobClient, null);
  }

  protected BlobClient(AbstractBlobClient<?> blobClient, TracingPolicy tracingPolicy) {
    this.blobClient = blobClient;
    this.multiCloudJLogger =
        new MultiCloudJLogger(
            tracingPolicy, SDK_SERVICE, blobClient != null ? blobClient.getProviderId() : null);
  }

  public static BlobClientBuilder builder(String providerId) {
    return new BlobClientBuilder(providerId);
  }

  /** Lists the buckets associated with the authenticated account in the substrate. */
  public ListBucketsResponse listBuckets() {
    return multiCloudJLogger.traceOperation(
        BlobSpanNames.LIST_BUCKETS,
        null,
        null,
        ctx -> {
          try {
            return blobClient.listBuckets();
          } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobClient.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
          }
        });
  }

  /**
   * Creates a new bucket with the specified name.
   *
   * @param bucketName The name of the bucket to create
   */
  public void createBucket(String bucketName) {
    multiCloudJLogger.traceVoidOperation(
        BlobSpanNames.CREATE_BUCKET,
        bucketName != null ? Map.of("bucket", bucketName) : null,
        null,
        ctx -> {
          try {
            blobClient.createBucket(bucketName);
          } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobClient.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
          }
        });
  }

  /** Closes the underlying blob client and releases any resources. */
  @Override
  public void close() throws Exception {
    if (blobClient != null) {
      blobClient.close();
    }
  }

  public static class BlobClientBuilder {

    private final AbstractBlobClient.Builder<?> blobClientBuilder;

    public BlobClientBuilder(String providerId) {
      this.blobClientBuilder = ProviderSupplier.findBlobClientProviderBuilder(providerId);
    }

    /**
     * Method to supply region
     *
     * @param region Region
     * @return An instance of self
     */
    public BlobClientBuilder withRegion(String region) {
      this.blobClientBuilder.withRegion(region);
      return this;
    }

    /**
     * Method to supply an endpoint override
     *
     * @param endpoint The endpoint override
     * @return An instance of self
     */
    public BlobClientBuilder withEndpoint(URI endpoint) {
      this.blobClientBuilder.withEndpoint(endpoint);
      return this;
    }

    /**
     * Method to supply a proxy endpoint override
     *
     * @param proxyEndpoint The proxy endpoint override
     * @return An instance of self
     */
    public BlobClientBuilder withProxyEndpoint(URI proxyEndpoint) {
      this.blobClientBuilder.withProxyEndpoint(proxyEndpoint);
      return this;
    }

    /**
     * Method to supply credentialsOverrider
     *
     * @param credentialsOverrider CredentialsOverrider
     * @return An instance of self
     */
    public BlobClientBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
      this.blobClientBuilder.withCredentialsOverrider(credentialsOverrider);
      return this;
    }

    /**
     * Method to supply retry configuration
     *
     * @param retryConfig The retry configuration to use for retrying failed requests
     * @return An instance of self
     */
    public BlobClientBuilder withRetryConfig(RetryConfig retryConfig) {
      this.blobClientBuilder.withRetryConfig(retryConfig);
      return this;
    }

    /**
     * Method to supply the per-client tracing policy. Default is {@link TracingPolicy#DISABLED}.
     *
     * @param tracingPolicy the tracing policy
     * @return An instance of self
     */
    public BlobClientBuilder withTracingPolicy(TracingPolicy tracingPolicy) {
      this.blobClientBuilder.withTracingPolicy(tracingPolicy);
      return this;
    }

    /**
     * Builds and returns an instance of BlobClient.
     *
     * @return An instance of BlobClient.
     */
    public BlobClient build() {
      return new BlobClient(blobClientBuilder.build(), blobClientBuilder.getTracingPolicy());
    }
  }
}
