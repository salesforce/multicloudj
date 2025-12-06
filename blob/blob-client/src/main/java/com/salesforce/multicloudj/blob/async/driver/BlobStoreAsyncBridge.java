package com.salesforce.multicloudj.blob.async.driver;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryDownloadResponse;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsBatch;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import lombok.Getter;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

/**
 * An async wrapper around AbstractBlobStore that implements AsyncBlobStore.
 * This wrapper takes an AbstractBlobStore and makes it asynchronous by executing
 * operations on a provided ExecutorService.
 */
public class BlobStoreAsyncBridge implements AsyncBlobStore {

    @Getter
    private final AbstractBlobStore blobStore;

    @Getter
    private final ExecutorService executorService;

    /**
     * Creates a new async wrapper around the provided BlobStore.
     * Note: This is only a stop-gap for vendors that don't currently provide async client support.
     *       Once all the substrates have async clients the plan is to deprecate and remove this class.
     *
     * @param blobStore the synchronous blob store to wrap
     * @param executorService the executor service to use for async operations. If this value is
     *                        null then this will use the ForkJoinPool.commonPool()
     */
    public BlobStoreAsyncBridge(AbstractBlobStore blobStore, ExecutorService executorService) {
        this.blobStore = blobStore;
        this.executorService = executorService==null ? ForkJoinPool.commonPool() : executorService;
    }

    @Override
    public String getProviderId() {
        return blobStore.getProviderId();
    }

    @Override
    public String getBucket() {
        return blobStore.getBucket();
    }

    @Override
    public String getRegion() {
        return blobStore.getRegion();
    }

    @Override
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, InputStream inputStream) {
        return CompletableFuture.supplyAsync(() -> blobStore.upload(uploadRequest, inputStream), executorService);
    }

    @Override
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, byte[] content) {
        return CompletableFuture.supplyAsync(() -> blobStore.upload(uploadRequest, content), executorService);
    }

    @Override
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, File file) {
        return CompletableFuture.supplyAsync(() -> blobStore.upload(uploadRequest, file), executorService);
    }

    @Override
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, Path path) {
        return CompletableFuture.supplyAsync(() -> blobStore.upload(uploadRequest, path), executorService);
    }

    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, OutputStream outputStream) {
        return CompletableFuture.supplyAsync(() -> blobStore.download(downloadRequest, outputStream), executorService);
    }

    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, ByteArray byteArray) {
        return CompletableFuture.supplyAsync(() -> blobStore.download(downloadRequest, byteArray), executorService);
    }

    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, File file) {
        return CompletableFuture.supplyAsync(() -> blobStore.download(downloadRequest, file), executorService);
    }

    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, Path path) {
        return CompletableFuture.supplyAsync(() -> blobStore.download(downloadRequest, path), executorService);
    }

    @Override
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest) {
        return CompletableFuture.supplyAsync(() -> blobStore.download(downloadRequest), executorService);
    }

    @Override
    public CompletableFuture<Void> delete(String key, String versionId) {
        return CompletableFuture.runAsync(() -> blobStore.delete(key, versionId), executorService);
    }

    @Override
    public CompletableFuture<Void> delete(Collection<BlobIdentifier> objects) {
        return CompletableFuture.runAsync(() -> blobStore.delete(objects), executorService);
    }

    @Override
    public CompletableFuture<CopyResponse> copy(CopyRequest request) {
        return CompletableFuture.supplyAsync(() -> blobStore.copy(request), executorService);
    }

    @Override
    public CompletableFuture<BlobMetadata> getMetadata(String key, String versionId) {
        return CompletableFuture.supplyAsync(() -> blobStore.getMetadata(key, versionId), executorService);
    }

    @Override
    public CompletableFuture<Void> list(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
        return CompletableFuture.runAsync(() -> {
            Iterator<BlobInfo> iterator = blobStore.list(request);
            List<BlobInfo> currentBatch = new ArrayList<>();
            while (iterator.hasNext()) {
                currentBatch.add(iterator.next());
                if (currentBatch.size() >= 100) {
                    consumer.accept(new ListBlobsBatch(currentBatch, List.of()));
                    currentBatch = new ArrayList<>();
                }
            }
            
            // Process any remaining items
            if (!currentBatch.isEmpty()) {
                consumer.accept(new ListBlobsBatch(currentBatch, List.of()));
            }
        }, executorService);
    }

    @Override
    public CompletableFuture<ListBlobsPageResponse> listPage(ListBlobsPageRequest request) {
        return CompletableFuture.supplyAsync(() -> blobStore.listPage(request), executorService);
    }

    @Override
    public CompletableFuture<MultipartUpload> initiateMultipartUpload(MultipartUploadRequest request) {
        return CompletableFuture.supplyAsync(() -> blobStore.initiateMultipartUpload(request), executorService);
    }

    @Override
    public CompletableFuture<UploadPartResponse> uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        return CompletableFuture.supplyAsync(() -> blobStore.uploadMultipartPart(mpu, mpp), executorService);
    }

    @Override
    public CompletableFuture<MultipartUploadResponse> completeMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        return CompletableFuture.supplyAsync(() -> blobStore.completeMultipartUpload(mpu, parts), executorService);
    }

    @Override
    public CompletableFuture<List<UploadPartResponse>> listMultipartUpload(MultipartUpload mpu) {
        return CompletableFuture.supplyAsync(() -> blobStore.listMultipartUpload(mpu), executorService);
    }

    @Override
    public CompletableFuture<Void> abortMultipartUpload(MultipartUpload mpu) {
        return CompletableFuture.runAsync(() -> blobStore.abortMultipartUpload(mpu), executorService);
    }

    @Override
    public CompletableFuture<Map<String, String>> getTags(String key) {
        return CompletableFuture.supplyAsync(() -> blobStore.getTags(key), executorService);
    }

    @Override
    public CompletableFuture<Void> setTags(String key, Map<String, String> tags) {
        return CompletableFuture.runAsync(() -> blobStore.setTags(key, tags), executorService);
    }

    @Override
    public CompletableFuture<URL> generatePresignedUrl(PresignedUrlRequest request) {
        return CompletableFuture.supplyAsync(() -> blobStore.generatePresignedUrl(request), executorService);
    }

    @Override
    public CompletableFuture<Boolean> doesObjectExist(String key, String versionId) {
        return CompletableFuture.supplyAsync(() -> blobStore.doesObjectExist(key, versionId), executorService);
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return blobStore.getException(t);
    }

    @Override
    public CompletableFuture<DirectoryDownloadResponse> downloadDirectory(DirectoryDownloadRequest directoryDownloadRequest){
        return CompletableFuture.supplyAsync(() -> blobStore.downloadDirectory(directoryDownloadRequest), executorService);
    }

    @Override
    public CompletableFuture<DirectoryUploadResponse> uploadDirectory(DirectoryUploadRequest directoryUploadRequest) {
        return CompletableFuture.supplyAsync(() -> blobStore.uploadDirectory(directoryUploadRequest), executorService);
    }

    @Override
    public CompletableFuture<Void> deleteDirectory(String prefix) {
        return CompletableFuture.runAsync(() -> blobStore.deleteDirectory(prefix), executorService);
    }

    /**
     * Closes the wrapped blob store and releases any resources.
     */
    @Override
    public void close() throws Exception {
        if (blobStore != null) {
            blobStore.close();
        }
    }
} 