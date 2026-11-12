package com.salesforce.multicloudj.blob.gcp.async;

import com.google.cloud.storage.Storage;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.blob.async.driver.BlobStoreAsyncBridge;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.gcp.GcpBlobStore;
import com.salesforce.multicloudj.blob.gcp.GcpTransformerSupplier;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;

/** GCP implementation of AsyncBlobStore */
public class GcpAsyncBlobStore extends BlobStoreAsyncBridge implements AsyncBlobStore {

  private final Storage storage;
  private final GcpTransformerSupplier transformerSupplier;
  private final boolean useTransferListener;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Creates a new async wrapper around the provided BlobStore.
   *
   * @param blobStore the synchronous blob store to wrap
   * @param executorService the executor service to use for async operations. If this value is null
   *     then this will use the ForkJoinPool.commonPool()
   * @param storage the GCP Storage client for directory operations
   * @param transformerSupplier the transformer supplier for GCP operations
   */
  public GcpAsyncBlobStore(
      AbstractBlobStore blobStore,
      ExecutorService executorService,
      Storage storage,
      GcpTransformerSupplier transformerSupplier) {
    this(blobStore, executorService, storage, transformerSupplier, null);
  }

  public GcpAsyncBlobStore(
      AbstractBlobStore blobStore,
      ExecutorService executorService,
      Storage storage,
      GcpTransformerSupplier transformerSupplier,
      Boolean useTransferListener) {
    super(blobStore, executorService);
    this.storage = storage;
    this.transformerSupplier = transformerSupplier;
    this.useTransferListener = Boolean.TRUE.equals(useTransferListener);
  }

  /**
   * Closes the wrapped blob store, which releases its TransferManager workers, and then the
   * directly supplied Storage client. The Storage client may be the same instance the wrapped store
   * just closed; Storage.close() is safe to repeat. Both are attempted even if the first fails; the
   * first failure is thrown with later ones suppressed.
   * The executor service is owned by the caller (or is the common pool) and is left running.
   * Subsequent calls are no-ops.
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    Exception failure = null;
    try {
      super.close();
    } catch (Exception e) {
      failure = e;
    }
    if (storage != null) {
      try {
        storage.close();
      } catch (Exception e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure instanceof SubstrateSdkException) {
      throw (SubstrateSdkException) failure;
    }
    if (failure != null) {
      throw new SubstrateSdkException("Failed to close GCP async blob store", failure);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  @Getter
  public static class Builder extends AsyncBlobStoreProvider.Builder {

    private GcpBlobStore gcpBlobStore;
    private Storage storage;
    private GcpTransformerSupplier transformerSupplier = new GcpTransformerSupplier();

    public Builder() {
      providerId(GcpConstants.PROVIDER_ID);
    }

    public Builder withGcpBlobStore(GcpBlobStore gcpBlobStore) {
      this.gcpBlobStore = gcpBlobStore;
      return this;
    }

    public Builder withStorage(Storage storage) {
      this.storage = storage;
      return this;
    }

    public Builder withTransformerSupplier(GcpTransformerSupplier transformerSupplier) {
      this.transformerSupplier = transformerSupplier;
      return this;
    }

    @Override
    public GcpAsyncBlobStore build() {
      GcpBlobStore blobStore = gcpBlobStore;
      if (blobStore == null) {
        blobStore =
            new GcpBlobStore.Builder()
                .copyFrom(this)
                .withStorage(storage)
                .withTransformerSupplier(transformerSupplier)
                .build();
      }
      return new GcpAsyncBlobStore(
          blobStore,
          getExecutorService(),
          storage,
          transformerSupplier,
          getUseTransferListener());
    }
  }
}
