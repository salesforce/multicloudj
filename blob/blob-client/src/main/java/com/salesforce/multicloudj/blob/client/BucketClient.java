package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Entry point for Client code to interact with the Blob storage.
 */
public class BucketClient {

    protected AbstractBlobStore<?> blobStore;

    protected BucketClient(AbstractBlobStore<?> blobStore) {
        this.blobStore = blobStore;
    }

    public static BlobBuilder builder(String providerId) {
        return new BlobBuilder(providerId);
    }

    public String getBucket() {
        return blobStore.getBucket();
    }

    /**
     * Uploads the Blob content to substrate-specific Blob storage.
     * Note: Specifying the contentLength in the UploadRequest can dramatically improve upload efficiency
     * because the substrate SDKs do not need to buffer the contents and calculate it themselves.
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param inputStream The input stream that contains the blob content
     * @return Returns an UploadResponse object that contains metadata about the blob
     */
    public UploadResponse upload(UploadRequest uploadRequest, InputStream inputStream) {
        try {
            return blobStore.upload(uploadRequest, inputStream);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Uploads the Blob content to substrate-specific Blob storage
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param content The byte array that contains the blob content
     * @return Returns an UploadResponse object that contains metadata about the blob
     */
    public UploadResponse upload(UploadRequest uploadRequest, byte[] content) {
        try {
            return blobStore.upload(uploadRequest, content);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Uploads the Blob content to substrate-specific Blob storage
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param file The File that contains the blob content
     * @return Returns an UploadResponse object that contains metadata about the blob
     */
    public UploadResponse upload(UploadRequest uploadRequest, File file) {
        try {
            return blobStore.upload(uploadRequest, file);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Uploads the Blob content to substrate-specific Blob storage
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param path The Path that contains the blob content
     * @return Returns an UploadResponse object that contains metadata about the blob
     */
    public UploadResponse upload(UploadRequest uploadRequest, Path path) {
        try {
            return blobStore.upload(uploadRequest, path);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Downloads the Blob content from substrate-specific Blob storage
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param outputStream The output stream that the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    public DownloadResponse download(DownloadRequest downloadRequest, OutputStream outputStream) {
        try {
            return blobStore.download(downloadRequest, outputStream);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Downloads the Blob content from substrate-specific Blob storage
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param byteArray The byte array that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    public DownloadResponse download(DownloadRequest downloadRequest, ByteArray byteArray) {
        try {
            return blobStore.download(downloadRequest, byteArray);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Downloads the Blob content from substrate-specific Blob storage.
     * Throws an exception if the file already exists.
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param file The File the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    public DownloadResponse download(DownloadRequest downloadRequest, File file) {
        try {
            return blobStore.download(downloadRequest, file);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Downloads the Blob content from substrate-specific Blob storage.
     * Throws an exception if a file already exists at the path location.
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param path The Path that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    public DownloadResponse download(DownloadRequest downloadRequest, Path path) {
        try {
            return blobStore.download(downloadRequest, path);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Deletes a single blob from substrate-specific Blob storage
     *
     * @param key Object name of the Blob
     * @param versionId The versionId of the blob. This field is optional and should be null
     *                  unless you're targeting the deletion of a specific key/version blob.
     */
    public void delete(String key, String versionId) {
        try {
            blobStore.delete(key, versionId);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Deletes a collection of Blobs from a substrate-specific Blob storage.
     *
     * @param objects A collection of blob identifiers to delete
     */
    public void delete(Collection<BlobIdentifier> objects) {
        try {
            blobStore.delete(objects);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Copies the Blob to other bucket
     *
     * @param request copy request wrapper. Contains the information necessary to perform a copy
     * @return CopyResponse of the copied Blob
     */
    public CopyResponse copy(CopyRequest request) {
        try {
            return blobStore.copy(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Retrieves the metadata of the Blob
     *
     * @param key Name of the Blob, whose metadata is to be retrieved
     * @param versionId The versionId of the blob. This field is optional and only used if your bucket
     *                  has versioning enabled. This value should be null unless you're targeting a
     *                  specific key/version blob.
     * @return Metadata of the Blob
     */
    public BlobMetadata getMetadata(String key, String versionId) {
        try {
            return blobStore.getMetadata(key, versionId);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Retrieves the list of Blob in the bucket
     *
     * @return Iterator object of the Blobs
     */
    public Iterator<BlobInfo> list(ListBlobsRequest request) {
        try {
            return blobStore.list(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Initiates a multipartUpload for a Blob
     *
     * @param request Contains information about the blob to upload
     */
    public MultipartUpload initiateMultipartUpload(MultipartUploadRequest request) {
        try {
            return blobStore.initiateMultipartUpload(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Uploads a part of the multipartUpload
     *
     * @param mpu The multipartUpload to use
     * @param mpp The multipartPart data
     */
    public UploadPartResponse uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
        try {
            return blobStore.uploadMultipartPart(mpu, mpp);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Completes a multipartUpload
     *
     * @param mpu The multipartUpload to use
     * @param parts A list of the parts contained in the multipartUpload
     */
    public MultipartUploadResponse completeMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts) {
        try {
            return blobStore.completeMultipartUpload(mpu, parts);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Returns a list of all uploaded parts for the given MultipartUpload
     *
     * @param mpu The multipartUpload to query against
     */
    public List<UploadPartResponse> listMultipartUpload(MultipartUpload mpu) {
        try {
            return blobStore.listMultipartUpload(mpu);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Aborts a multipartUpload
     * @param mpu The multipartUpload to abort
     */
    public void abortMultipartUpload(MultipartUpload mpu) {
        try {
            blobStore.abortMultipartUpload(mpu);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Returns a map of all the tags associated with the blob
     * @param key Name of the blob whose tags are to be retrieved
     * @return The blob's tags
     */
    public Map<String, String> getTags(String key) {
        try {
            return blobStore.getTags(key);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Sets tags on a blob
     * @param key Name of the blob to set tags on
     * @param tags The tags to set
     */
    public void setTags(String key, Map<String, String> tags) {
        try {
            blobStore.setTags(key, tags);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Generates a presigned URL for uploading/downloading blobs
     * @param request The presigned request
     * @return Returns the presigned URL
     */
    public URL generatePresignedUrl(PresignedUrlRequest request) {
        try {
            return blobStore.generatePresignedUrl(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = blobStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    public static class BlobBuilder {

        private final AbstractBlobStore.Builder<?> blobStoreBuilder;

        public BlobBuilder(String providerId) {
            this.blobStoreBuilder = ProviderSupplier.findProviderBuilder(providerId);
        }

        /**
         * Method to supply bucket
         * @param bucket Bucket
         * @return An instance of self
         */
        public BlobBuilder withBucket(String bucket) {
            this.blobStoreBuilder.withBucket(bucket);
            return this;
        }

        /**
         * Method to supply region
         * @param region Region
         * @return An instance of self
         */
        public BlobBuilder withRegion(String region) {
            this.blobStoreBuilder.withRegion(region);
            return this;
        }

        /**
         * Method to supply an endpoint override
         * @param endpoint The endpoint override
         * @return An instance of self
         */
        public BlobBuilder withEndpoint(URI endpoint) {
            this.blobStoreBuilder.withEndpoint(endpoint);
            return this;
        }

        /**
         * Method to supply a proxy endpoint override
         * @param proxyEndpoint The proxy endpoint override
         * @return An instance of self
         */
        public BlobBuilder withProxyEndpoint(URI proxyEndpoint) {
            this.blobStoreBuilder.withProxyEndpoint(proxyEndpoint);
            return this;
        }

        /**
         * Method to supply credentialsOverrider
         * @param credentialsOverrider CredentialsOverrider
         * @return An instance of self
         */
        public BlobBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.blobStoreBuilder.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        /**
         * Builds and returns an instance of BucketClient.
         * @return An instance of BucketClient.
         */
        public BucketClient build() {
            return new BucketClient(blobStoreBuilder.build());
        }
    }
}
