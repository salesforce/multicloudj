package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.observability.MultiCloudJLogger;
import com.salesforce.multicloudj.common.observability.TracingPolicy;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.util.Map;
import lombok.Getter;

/**
 * Base class for substrate-specific implementations. This class serves the purpose of providing
 * common (i.e. substrate-agnostic) functionality.
 */
public abstract class AbstractBlobClient<T extends AbstractBlobClient<T>>
    implements Provider, SdkService, AutoCloseable {

  private static final String SDK_SERVICE = "blob";

  private final String providerId;
  @Getter protected final String region;

  protected final CredentialsOverrider credentialsOverrider;
  protected final MultiCloudJLogger multiCloudJLogger;

  protected AbstractBlobClient(Builder<T> builder) {
    this(
        builder.getProviderId(),
        builder.getRegion(),
        builder.getCredentialsOverrider(),
        builder.getTracingPolicy());
  }

  public AbstractBlobClient(String providerId, String region, CredentialsOverrider credentials) {
    this(providerId, region, credentials, null);
  }

  public AbstractBlobClient(
      String providerId,
      String region,
      CredentialsOverrider credentials,
      TracingPolicy tracingPolicy) {
    this.providerId = providerId;
    this.region = region;
    this.credentialsOverrider = credentials;
    this.multiCloudJLogger = new MultiCloudJLogger(tracingPolicy, SDK_SERVICE, providerId);
  }

  @Override
  public String getProviderId() {
    return providerId;
  }

  /** Passes the call to the substrate-specific listBuckets methods */
  public ListBucketsResponse listBuckets() {
    return multiCloudJLogger.traceOperation("blob.listBuckets", null, null, ctx -> doListBuckets());
  }

  /**
   * Creates a new bucket with the specified name
   *
   * @param bucketName The name of the bucket to create
   */
  public void createBucket(String bucketName) {
    multiCloudJLogger.traceVoidOperation(
        "blob.createBucket",
        bucketName != null ? Map.of("bucket", bucketName) : null,
        null,
        ctx -> doCreateBucket(bucketName));
  }

  protected abstract ListBucketsResponse doListBuckets();

  protected abstract void doCreateBucket(String bucketName);

  public abstract static class Builder<T extends AbstractBlobClient<T>> extends BlobBuilder<T>
      implements Provider.Builder {

    @Override
    public Builder<T> providerId(String providerId) {
      super.providerId(providerId);
      return this;
    }
  }
}
