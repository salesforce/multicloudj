package com.salesforce.multicloudj.blob.gcp.async;

import com.google.cloud.storage.Storage;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.blob.async.driver.BlobStoreAsyncBridge;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.gcp.GcpBlobStore;
import com.salesforce.multicloudj.blob.gcp.GcpTransformerSupplier;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import lombok.Getter;

import java.util.concurrent.ExecutorService;

/**
 * GCP implementation of AsyncBlobStore
 */
public class GcpAsyncBlobStore extends BlobStoreAsyncBridge implements AsyncBlobStore {

    /**
     * Creates a new async wrapper around the provided BlobStore.
     *
     * @param blobStore       the synchronous blob store to wrap
     * @param executorService the executor service to use for async operations. If this value is
     *                        null then this will use the ForkJoinPool.commonPool()
     */
    public GcpAsyncBlobStore(AbstractBlobStore<?> blobStore, ExecutorService executorService) {
        super(blobStore, executorService);
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
            GcpBlobStore blobStore = getGcpBlobStore();
            if(blobStore == null) {
                blobStore = new GcpBlobStore.Builder()
                        .withStorage(getStorage())
                        .withTransformerSupplier(getTransformerSupplier())
                        .withBucket(getBucket())
                        .withCredentialsOverrider(getCredentialsOverrider())
                        .withEndpoint(getEndpoint())
                        .withIdleConnectionTimeout(getIdleConnectionTimeout())
                        .withMaxConnections(getMaxConnections())
                        .withProperties(getProperties())
                        .withProxyEndpoint(getProxyEndpoint())
                        .withRegion(getRegion())
                        .withSocketTimeout(getSocketTimeout())
                        .withValidator(getValidator())
                        .build();
            }
            return new GcpAsyncBlobStore(blobStore, getExecutorService());
        }
    }
}
