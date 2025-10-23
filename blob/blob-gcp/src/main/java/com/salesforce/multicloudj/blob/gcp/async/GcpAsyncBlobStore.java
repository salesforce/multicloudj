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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.FailedBlobDownload;
import com.salesforce.multicloudj.blob.driver.FailedBlobUpload;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * GCP implementation of AsyncBlobStore
 */
public class GcpAsyncBlobStore extends BlobStoreAsyncBridge implements AsyncBlobStore {

    private final Storage storage;
    private final GcpTransformerSupplier transformerSupplier;

    /**
     * Creates a new async wrapper around the provided BlobStore.
     *
     * @param blobStore       the synchronous blob store to wrap
     * @param executorService the executor service to use for async operations. If this value is
     *                        null then this will use the ForkJoinPool.commonPool()
     * @param storage         the GCP Storage client for directory operations
     * @param transformerSupplier the transformer supplier for GCP operations
     */
    public GcpAsyncBlobStore(AbstractBlobStore<?> blobStore, ExecutorService executorService,
                             Storage storage, GcpTransformerSupplier transformerSupplier) {
        super(blobStore, executorService);
        this.storage = storage;
        this.transformerSupplier = transformerSupplier;
    }

    public static GcpAsyncBlobStore.Builder builder() {
        return new GcpAsyncBlobStore.Builder();
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
            return new GcpAsyncBlobStore(blobStore, getExecutorService(), storage, transformerSupplier);
        }
    }

}