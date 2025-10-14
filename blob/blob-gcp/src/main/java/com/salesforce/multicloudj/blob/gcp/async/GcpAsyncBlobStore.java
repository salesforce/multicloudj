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

    /**
     * Maximum number of objects that can be deleted in a single batch operation.
     * GCP supports up to 1000 objects per batch delete.
     */
    private static final int MAX_OBJECTS_PER_BATCH_DELETE = 1000;
    private static final int MAX_CONCURRENT_UPLOADS = 10;

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

    @Override
    public CompletableFuture<DirectoryUploadResponse> uploadDirectory(DirectoryUploadRequest directoryUploadRequest) {
        // Override the bridge implementation with optimized GCP directory upload
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path sourceDir = Paths.get(directoryUploadRequest.getLocalSourceDirectory());
                var transformer = transformerSupplier.get(getBucket());
                List<Path> filePaths = transformer.toFilePaths(directoryUploadRequest);
                List<FailedBlobUpload> failedUploads = new ArrayList<>();

                // Upload files in parallel using thread pool
                ExecutorService executor = null;
                List<CompletableFuture<Void>> uploadFutures = new ArrayList<>();

                if (!filePaths.isEmpty()) {
                    executor = Executors.newFixedThreadPool(
                            Math.min(filePaths.size(), MAX_CONCURRENT_UPLOADS));
                }

                for (Path filePath : filePaths) {
                    CompletableFuture<Void> uploadFuture = CompletableFuture.runAsync(() -> {
                        try {
                            // Generate blob key
                            String blobKey = transformer.toBlobKey(sourceDir, filePath, directoryUploadRequest.getPrefix());

                            // Upload file to GCS
                            BlobId blobId = BlobId.of(getBucket(), blobKey);
                            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                            storage.createFrom(blobInfo, filePath);
                        } catch (Exception e) {
                            synchronized (failedUploads) {
                                failedUploads.add(FailedBlobUpload.builder()
                                        .source(filePath)
                                        .exception(e)
                                        .build());
                            }
                        }
                    }, executor);
                    uploadFutures.add(uploadFuture);
                }

                // Wait for all uploads to complete
                CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0])).join();

                // Shutdown executor if it was created
                if (executor != null) {
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                }

                return DirectoryUploadResponse.builder()
                        .failedTransfers(failedUploads)
                        .build();

            } catch (Exception e) {
                throw new RuntimeException("Failed to upload directory", e);
            }
        }, getExecutorService());
    }

    @Override
    public CompletableFuture<DirectoryDownloadResponse> downloadDirectory(DirectoryDownloadRequest directoryDownloadRequest) {
        // Override the bridge implementation with optimized GCP directory download
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path targetDir = Paths.get(directoryDownloadRequest.getLocalDestinationDirectory());
                var transformer = transformerSupplier.get(getBucket());
                List<FailedBlobDownload> failedDownloads = new ArrayList<>();

                // Create target directory if it doesn't exist
                Files.createDirectories(targetDir);

                // List all blobs with the given prefix
                final String prefix = directoryDownloadRequest.getPrefixToDownload() != null && !directoryDownloadRequest.getPrefixToDownload().endsWith("/")
                        ? directoryDownloadRequest.getPrefixToDownload() + "/"
                        : directoryDownloadRequest.getPrefixToDownload();

                Storage.BlobListOption[] options = prefix != null ?
                        new Storage.BlobListOption[]{
                                Storage.BlobListOption.prefix(prefix)
                        } : new Storage.BlobListOption[0];

                List<Blob> blobs = new ArrayList<>();
                for (Blob blob : storage.list(getBucket(), options).getValues()) {
                    blobs.add(blob);
                }

                // Download files in parallel using thread pool
                ExecutorService executor = null;
                List<CompletableFuture<Void>> downloadFutures = new ArrayList<>();

                if (!blobs.isEmpty()) {
                    executor = Executors.newFixedThreadPool(
                            Math.min(blobs.size(), 10));
                }

                for (Blob blob : blobs) {
                    CompletableFuture<Void> downloadFuture = CompletableFuture.runAsync(() -> {
                        try {
                            // Skip if it's a directory marker (ends with /)
                            if (blob.getName().endsWith("/")) {
                                return;
                            }

                            // Calculate local file path
                            String relativePath = prefix != null ?
                                    blob.getName().substring(prefix.length()) : blob.getName();
                            Path localFilePath = targetDir.resolve(relativePath);

                            // Create parent directories
                            Files.createDirectories(localFilePath.getParent());

                            // Download blob to local file
                            blob.downloadTo(localFilePath);

                        } catch (Exception e) {
                            synchronized (failedDownloads) {
                                failedDownloads.add(FailedBlobDownload.builder()
                                        .destination(Paths.get(directoryDownloadRequest.getLocalDestinationDirectory(),
                                                prefix != null ? blob.getName().substring(prefix.length()) : blob.getName()))
                                        .exception(e)
                                        .build());
                            }
                        }
                    }, executor);
                    downloadFutures.add(downloadFuture);
                }

                // Wait for all downloads to complete
                CompletableFuture.allOf(downloadFutures.toArray(new CompletableFuture[0])).join();

                // Shutdown executor if it was created
                if (executor != null) {
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                }

                return DirectoryDownloadResponse.builder()
                        .failedTransfers(failedDownloads)
                        .build();

            } catch (Exception e) {
                throw new RuntimeException("Failed to download directory", e);
            }
        }, getExecutorService());
    }

    @Override
    public CompletableFuture<Void> deleteDirectory(String prefix) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        var transformer = transformerSupplier.get(getBucket());

        // List all blobs with the given prefix and delete them in batches
        Storage.BlobListOption[] options = prefix != null ?
                new Storage.BlobListOption[]{
                        Storage.BlobListOption.prefix(prefix)
                } : new Storage.BlobListOption[0];

        List<Blob> blobs = new ArrayList<>();
        for (Blob blob : storage.list(getBucket(), options).getValues()) {
            blobs.add(blob);
        }

        // Convert GCP Blob objects to DriverBlobInfo objects for partitioning
        var blobInfos = new ArrayList<com.salesforce.multicloudj.blob.driver.BlobInfo>();
        for (Blob blob : blobs) {
            blobInfos.add(com.salesforce.multicloudj.blob.driver.BlobInfo.builder()
                    .withKey(blob.getName())
                    .withObjectSize(blob.getSize())
                    .build());
        }

        // Partition the blobs into smaller chunks for batch deletion
        var partitionedBlobLists = transformer.partitionList(blobInfos, MAX_OBJECTS_PER_BATCH_DELETE);

        // Delete each partition
        for (var blobList : partitionedBlobLists) {
            List<BlobId> blobIds = blobList.stream()
                    .map(blobInfo -> BlobId.of(getBucket(), blobInfo.getKey()))
                    .collect(Collectors.toList());

            CompletableFuture<Void> deleteFuture = CompletableFuture.runAsync(() -> {
                try {
                    storage.delete(blobIds);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to delete blobs", e);
                }
            }, getExecutorService());

            futures.add(deleteFuture);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

}
