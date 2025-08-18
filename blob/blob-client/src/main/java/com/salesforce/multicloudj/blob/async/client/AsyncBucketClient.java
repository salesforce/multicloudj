package com.salesforce.multicloudj.blob.async.client;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.blob.driver.BlobClientBuilder;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
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
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Entry point for async Client code to interact with the Blob storage.
 */
public class AsyncBucketClient {

    protected AsyncBlobStore blobStore;

    protected AsyncBucketClient(AsyncBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    public static Builder builder(String providerId) {
        return new Builder(providerId);
    }

    protected <T> T handleException(Throwable ex) {
        Class<? extends SubstrateSdkException> exceptionClass = blobStore.getException(ex);
        ExceptionHandler.handleAndPropagate(exceptionClass, ex);
        return null;
    }

    /**
     * Uploads the Blob content to substrate-specific Blob storage
     * Note: Specifying the contentLength in the UploadRequest can dramatically improve upload efficiency
     * because the substrate SDKs do not need to buffer the contents and calculate it themselves.
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param inputStream The input stream that contains the blob content
     * @return Returns an UploadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, InputStream inputStream) {
        return blobStore
                .upload(uploadRequest, inputStream)
                .exceptionally(this::handleException);
    }

    /**
     * Uploads the Blob content to substrate-specific Blob storage
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param content The byte array that contains the blob content
     * @return Returns an UploadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, byte[] content) {
        return blobStore
                .upload(uploadRequest, content)
                .exceptionally(this::handleException);
    }

    /**
     * Uploads the Blob content to substrate-specific Blob storage
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param file The File that contains the blob content
     * @return Returns an UploadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, File file) {
        return blobStore
                .upload(uploadRequest, file)
                .exceptionally(this::handleException);
    }

    /**
     * Uploads the Blob content to substrate-specific Blob storage
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param path The Path that contains the blob content
     * @return Returns an UploadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, Path path) {
        return blobStore
                .upload(uploadRequest, path)
                .exceptionally(this::handleException);
    }

    /**
     * Downloads the Blob content from substrate-specific Blob storage
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param outputStream The output stream that the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, OutputStream outputStream) {
        return blobStore
                .download(downloadRequest, outputStream)
                .exceptionally(this::handleException);
    }

    /**
     * Downloads the Blob content from substrate-specific Blob storage
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param byteArray The byte array that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, ByteArray byteArray) {
        return blobStore
                .download(downloadRequest, byteArray)
                .exceptionally(this::handleException);
    }

    /**
     * Downloads the Blob content from substrate-specific Blob storage.
     * Throws an exception if the file already exists.
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param file The File the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the file already exists.
     */
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, File file) {
        return blobStore
                .download(downloadRequest, file)
                .exceptionally(this::handleException);
    }

    /**
     * Downloads the Blob content from substrate-specific Blob storage.
     * Throws an exception if a file already exists at the path location.
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param path The Path that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if a file already exists at the path location.
     */
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, Path path) {
        return blobStore
                .download(downloadRequest, path)
                .exceptionally(this::handleException);
    }

    /**
     * Downloads the Blob content and returns an InputStream for reading the content
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @return Returns a DownloadResponse object that contains metadata about the blob and an InputStream for reading the content
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest) {
        return blobStore
                .download(downloadRequest)
                .exceptionally(this::handleException);
    }
    /**
     * Deletes a single Blob from substrate-specific Blob storage
     *
     * @param key Object name of the Blob
     * @param versionId The versionId of the blob
     * @return a completable future
     * @throws SubstrateSdkException Thrown if the operation fails. Will not throw an exception if the blob does not exist.
     */
    public CompletableFuture<Void> delete(String key, String versionId) {
        return blobStore
                .delete(key, versionId)
                .exceptionally(this::handleException);
    }

    /**
     * Deletes a collection of Blobs from a substrate-specific Blob storage.
     *
     * @param objects A collection of blob identifiers to delete
     * @return a completable future
     * @throws SubstrateSdkException Thrown if the operation fails. Will not throw an exception if a blob in the list does not exist.
     */
    public CompletableFuture<Void> delete(Collection<BlobIdentifier> objects) {
        return blobStore
                .delete(objects)
                .exceptionally(this::handleException);
    }

    /**
     * Copies the Blob to other bucket
     *
     * @param request request describing copy operation inputs
     * @return a completable future of CopyResponse of the copied blob
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<CopyResponse> copy(CopyRequest request) {
        return blobStore
                .copy(request)
                .exceptionally(this::handleException);
    }

    /**
     * Retrieves the metadata of the Blob
     *
     * @param key Name of the Blob, whose metadata is to be retrieved
     * @param versionId The versionId of the blob. This field is optional and only used if your bucket
     *                  has versioning enabled. This value should be null unless you're targeting a
     *                  specific key/version blob.
     * @return Metadata of the Blob
     * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the blob does not exist.
     */
    public CompletableFuture<BlobMetadata> getMetadata(String key, String versionId) {
        return blobStore
                .getMetadata(key, versionId)
                .exceptionally(this::handleException);
    }

    /**
     * Retrieves the list of Blob in the bucket
     *
     * @return future that will complete when all blobs have been read
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<Void> list(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer) {
        return blobStore
                .list(request, consumer)
                .exceptionally(this::handleException);
    }

    /**
     * Retrieves a single page of blobs from the bucket with pagination support
     *
     * @param request The pagination request containing filters, pagination token, and max results
     * @return ListBlobsPageResponse containing the blobs, truncation status, and next page token
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<ListBlobsPageResponse> listPage(ListBlobsPageRequest request) {
        return blobStore
                .listPage(request)
                .exceptionally(this::handleException);
    }

    /**
     * Initiates a multipartUpload for a Blob
     *
     * @param request Contains information about the blob to upload
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<MultipartUpload> initiateMultipartUpload(MultipartUploadRequest request) {
        return blobStore
                .initiateMultipartUpload(request)
                .exceptionally(this::handleException);
    }

    /**
     * Uploads a part of the multipartUpload
     *
     * @param mpu The multipartUpload to use
     * @param mpp The multipartPart data
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<UploadPartResponse> uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        return blobStore
                .uploadMultipartPart(mpu, mpp)
                .exceptionally(this::handleException);
    }

    /**
     * Completes a multipartUpload
     *
     * @param mpu The multipartUpload to use
     * @param parts A list of the parts contained in the multipartUpload
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<MultipartUploadResponse> completeMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        return blobStore
                .completeMultipartUpload(mpu, parts)
                .exceptionally(this::handleException);
    }

    /**
     * Returns a list of all uploaded parts for the given MultipartUpload
     *
     * @param mpu The multipartUpload to query against
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<List<UploadPartResponse>> listMultipartUpload(MultipartUpload mpu) {
        return blobStore
                .listMultipartUpload(mpu)
                .exceptionally(this::handleException);
    }

    /**
     * Aborts a multipartUpload
     * @param mpu The multipartUpload to abort
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<Void> abortMultipartUpload(MultipartUpload mpu) {
        return blobStore
                .abortMultipartUpload(mpu)
                .exceptionally(this::handleException);
    }

    /**
     * Returns a map of all the tags associated with the blob
     * @param key Name of the blob whose tags are to be retrieved
     * @return The blob's tags
     * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the blob does not exist.
     */
    public CompletableFuture<Map<String, String>> getTags(String key) {
        return blobStore
                .getTags(key)
                .exceptionally(this::handleException);
    }

    /**
     * Sets tags on a blob
     * @param key Name of the blob to set tags on
     * @param tags The tags to set
     * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the blob does not exist.
     */
    public CompletableFuture<Void> setTags(String key, Map<String, String> tags) {
        return blobStore
                .setTags(key, tags)
                .exceptionally(this::handleException);
    }

    /**
     * Generates a presigned URL for uploading/downloading blobs
     * @param request The presigned request
     * @return Returns the presigned URL
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<URL> generatePresignedUrl(PresignedUrlRequest request) {
        return blobStore
                .generatePresignedUrl(request)
                .exceptionally(this::handleException);
    }

    /**
     * Determines if an object exists for a given key/versionId
     * @param key Name of the blob to check
     * @param versionId The version of the blob to check. This field is optional and should be null
     *                  unless you're checking for the existence of a specific key/version blob.
     * @return Returns true if the object exists. Returns false if it doesn't exist.
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<Boolean> doesObjectExist(String key, String versionId) {
        return blobStore
                .doesObjectExist(key, versionId)
                .exceptionally(this::handleException);
    }

    /**
     * Uploads the directory content to substrate-specific Blob storage
     * Note: Specifying the contentLength in the UploadRequest can dramatically improve upload efficiency
     * because the substrate SDKs do not need to buffer the contents and calculate it themselves.
     *
     * @param directoryUploadRequest Wrapper, containing directory upload data
     * @return Returns an DirectoryUploadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails
     */
    public CompletableFuture<DirectoryUploadResponse> uploadDirectory(DirectoryUploadRequest directoryUploadRequest) {
        return blobStore
                .uploadDirectory(directoryUploadRequest)
                .exceptionally(this::handleException);
    }

    /**
     * Downloads the directory content from substrate-specific Blob storage.
     * Throws an exception if the file already exists in the destination directory.
     *
     * @param directoryDownloadRequest downloadRequest Wrapper, containing directory download data
     * @return Returns a DirectoryDownloadResponse object that contains metadata about the blob
     * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the file already exists.
     */
    public CompletableFuture<DirectoryDownloadResponse> downloadDirectory(DirectoryDownloadRequest directoryDownloadRequest) {
        return blobStore
                .downloadDirectory(directoryDownloadRequest)
                .exceptionally(this::handleException);
    }

    /**
     * Deletes all blobs in the bucket which have keys that start with the given prefix.
     *
     * @param prefix The prefix of blobs that should be deleted (e.g. the directory)
     * @throws SubstrateSdkException Thrown if the operation fails. Throws an exception if the file already exists.
     */
    public CompletableFuture<Void> deleteDirectory(String prefix) {
        return blobStore
                .deleteDirectory(prefix)
                .exceptionally(this::handleException);
    }

    public static class Builder extends BlobClientBuilder<AsyncBucketClient, AsyncBlobStore> {

        public Builder(String providerId) {
            super(ProviderSupplier.findAsyncBuilder(providerId));
        }

        public Builder(AsyncBlobStoreProvider.Builder storeBuilder) {
            super(storeBuilder);
        }

        @Override
        public Builder withProperties(Properties properties) {
            super.withProperties(properties);
            return this;
        }

        @Override
        public Builder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            super.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        @Override
        public Builder withEndpoint(URI endpoint) {
            super.withEndpoint(endpoint);
            return this;
        }

        @Override
        public Builder withProxyEndpoint(URI proxyEndpoint) {
            super.withProxyEndpoint(proxyEndpoint);
            return this;
        }

        @Override
        public Builder withRegion(String region) {
            super.withRegion(region);
            return this;
        }

        @Override
        public Builder withBucket(String bucket) {
            super.withBucket(bucket);
            return this;
        }

        @Override
        public Builder withMaxConnections(Integer maxConnections) {
            super.withMaxConnections(maxConnections);
            return this;
        }

        @Override
        public Builder withSocketTimeout(Duration socketTimeout) {
            super.withSocketTimeout(socketTimeout);
            return this;
        }

        @Override
        public Builder withIdleConnectionTimeout(Duration idleConnectionTimeout) {
            super.withIdleConnectionTimeout(idleConnectionTimeout);
            return this;
        }

        @Override
        public Builder withExecutorService(ExecutorService executorService) {
            super.withExecutorService(executorService);
            return this;
        }

        /**
         * Method to supply multipart threshold in bytes
         * @param thresholdBytes The threshold in bytes above which multipart upload will be used
         * @return An instance of self
         */
        @Override
        public Builder withThresholdBytes(Long thresholdBytes) {
            super.withThresholdBytes(thresholdBytes);
            return this;
        }

        /**
         * Method to supply multipart part buffer size in bytes
         * @param partBufferSize The buffer size in bytes for each part in a multipart upload
         * @return An instance of self
         */
        @Override
        public Builder withPartBufferSize(Long partBufferSize) {
            super.withPartBufferSize(partBufferSize);
            return this;
        }

        /**
         * Method to enable/disable parallel uploads
         * @param parallelUploadsEnabled Whether to enable parallel uploads
         * @return An instance of self
         */
        @Override
        public Builder withParallelUploadsEnabled(Boolean parallelUploadsEnabled) {
            super.withParallelUploadsEnabled(parallelUploadsEnabled);
            return this;
        }

        /**
         * Method to enable/disable parallel downloads
         * @param parallelDownloadsEnabled Whether to enable parallel downloads
         * @return An instance of self
         */
        @Override
        public Builder withParallelDownloadsEnabled(Boolean parallelDownloadsEnabled) {
            super.withParallelDownloadsEnabled(parallelDownloadsEnabled);
            return this;
        }

        /**
         * Method to set target throughput in Gbps
         * @param targetThroughputInGbps The target throughput in Gbps
         * @return An instance of self
         */
        @Override
        public Builder withTargetThroughputInGbps(Double targetThroughputInGbps) {
            super.withTargetThroughputInGbps(targetThroughputInGbps);
            return this;
        }

        /**
         * Method to set maximum native memory limit in bytes
         * @param maxNativeMemoryLimitInBytes The maximum native memory limit in bytes
         * @return An instance of self
         */
        @Override
        public Builder withMaxNativeMemoryLimitInBytes(Long maxNativeMemoryLimitInBytes) {
            super.withMaxNativeMemoryLimitInBytes(maxNativeMemoryLimitInBytes);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public AsyncBucketClient build() {
            return new AsyncBucketClient(storeBuilder.build());
        }
    }
}
