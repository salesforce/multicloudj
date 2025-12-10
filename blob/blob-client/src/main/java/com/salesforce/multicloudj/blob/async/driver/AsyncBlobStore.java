package com.salesforce.multicloudj.blob.async.driver;

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
import com.salesforce.multicloudj.common.service.SdkService;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * API for async interaction with a backing blob storage engine.
 */
public interface AsyncBlobStore extends SdkService, AutoCloseable {

    /**
     * Returns the bucket this blob store operates against.
     * @return the bucket this blob store operates against
     */
    String getBucket();

    /**
     * Returns the region this blob store operates against.
     * @return the region this blob store operates against.
     */
    String getRegion();

    /**
     * Performs args validation and passes the call to substrate-specific upload method
     * Note: Specifying the contentLength in the UploadRequest can dramatically improve upload efficiency
     * because the substrate SDKs do not need to buffer the contents and calculate it themselves.
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param inputStream The input stream that contains the blob content
     */
    CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, InputStream inputStream);

    /**
     * Performs args validation and passes the call to substrate-specific upload method
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param content The byte array that contains the blob content
     */
    CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, byte[] content);

    /**
     * Performs args validation and passes the call to substrate-specific upload method
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param file The File that contains the blob content
     */
    CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, File file);

    /**
     * Performs args validation and passes the call to substrate-specific upload method
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param path The Path that contains the blob content
     */
    CompletableFuture<UploadResponse> upload(UploadRequest uploadRequest, Path path);
    /**
     * Downloads the Blob content from substrate-specific Blob storage
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param outputStream The output stream that the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, OutputStream outputStream);

    /**
     * Downloads the Blob content from substrate-specific Blob storage
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param byteArray The byte array that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, ByteArray byteArray);

    /**
     * Downloads the Blob content from substrate-specific Blob storage.
     * Throws an exception if the file already exists.
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param file The File the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, File file);

    /**
     * Downloads the Blob content from substrate-specific Blob storage.
     * Throws an exception if a file already exists at the path location.
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @param path The Path that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest, Path path);

    /**
     * Downloads the Blob content from substrate-specific Blob storage.
     * Throws an exception if a file already exists at the path location.
     *
     * @param downloadRequest downloadRequest Wrapper, containing download data
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    CompletableFuture<DownloadResponse> download(DownloadRequest downloadRequest);

    /**
     * Performs args validation and passes the call to substrate-specific delete method
     *
     * @param key Object name of the Blob
     * @param versionId The versionId of the blob
     */
    CompletableFuture<Void> delete(String key, String versionId);

    /**
     * Performs args validation and passes the call to substrate-specific delete method
     *
     * @param objects A collection of BlobIdentifiers representing the blobs to delete
     */
    CompletableFuture<Void> delete(Collection<BlobIdentifier> objects);

    /**
     * Performs args validation and passes the call to substrate-specific copy method
     * @param request the request specifying what to copy
     * @return CopyResponse of the copied Blob
     */
    CompletableFuture<CopyResponse> copy(CopyRequest request);

    /**
     * Retrieves the metadata of the Blob
     *
     * @param key Name of the Blob, whose metadata is to be retrieved
     * @param versionId The versionId of the blob. This field is optional and only used if your bucket
     *                  has versioning enabled. This value should be null unless you're targeting a
     *                  specific key/version blob.
     * @return Metadata of the Blob
     */
    CompletableFuture<BlobMetadata> getMetadata(String key, String versionId);

    /**
     * Retrieves the list of Blob in the bucket
     *
     * @return Iterator object of the Blobs
     */
    CompletableFuture<Void> list(ListBlobsRequest request, Consumer<ListBlobsBatch> consumer);

    /**
     * Retrieves a single page of blobs from the bucket with pagination support
     *
     * @param request The pagination request containing filters, pagination token, and max results
     * @return ListBlobsPageResponse containing the blobs, truncation status, and next page token
     */
    CompletableFuture<ListBlobsPageResponse> listPage(ListBlobsPageRequest request);

    /**
     * Initiates a multipartUpload for a Blob
     *
     * @param request Contains information about the blob to upload
     */
    CompletableFuture<MultipartUpload> initiateMultipartUpload(MultipartUploadRequest request);

    /**
     * Uploads a part of the multipartUpload
     *
     * @param mpu The multipartUpload to use
     * @param mpp The multipartPart data
     */
    CompletableFuture<UploadPartResponse> uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp);

    /**
     * Completes a multipartUpload
     *
     * @param mpu The multipartUpload to use
     * @param parts A list of the parts contained in the multipartUpload
     */
    CompletableFuture<MultipartUploadResponse> completeMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts);

    /**
     * Returns a list of all uploaded parts for the given MultipartUpload
     *
     * @param mpu The multipartUpload to query against
     */
    CompletableFuture<List<UploadPartResponse>> listMultipartUpload(MultipartUpload mpu);

    /**
     * Aborts a multipartUpload
     * @param mpu The multipartUpload to abort
     */
    CompletableFuture<Void> abortMultipartUpload(MultipartUpload mpu);

    /**
     * Returns a map of all the tags associated with the blob
     * @param key Name of the blob whose tags are to be retrieved
     * @return The blob's tags
     */
    CompletableFuture<Map<String, String>> getTags(String key);

    /**
     * Sets tags on a blob
     * @param key Name of the blob to set tags on
     * @param tags The tags to set
     */
    CompletableFuture<Void> setTags(String key, Map<String, String> tags);

    /**
     * Generates a presigned URL for uploading/downloading blobs
     * @param request The presigned request
     * @return Returns the presigned URL
     */
    CompletableFuture<URL> generatePresignedUrl(PresignedUrlRequest request);

    /**
     * Determines if an object exists for a given key/versionId
     * @param key Name of the blob to check
     * @param versionId The version of the blob to check. This field is optional and should be null
     *                  unless you're checking for the existence of a specific key/version blob.
     * @return Returns true if the object exists. Returns false if it doesn't exist.
     */
    CompletableFuture<Boolean> doesObjectExist(String key, String versionId);

    /**
     * Downloads the directory content from substrate-specific Blob storage.
     *
     * @param directoryDownloadRequest directoryDownloadRequest Wrapper, containing directory download data
     * @return Returns a DirectoryDownloadResponse object that contains metadata about the blob
     */
    CompletableFuture<DirectoryDownloadResponse> downloadDirectory(DirectoryDownloadRequest directoryDownloadRequest);

    /**
     * Passes the call to substrate-specific directory upload method
     *
     * @param directoryUploadRequest Wrapper, containing directory upload data
     */
    CompletableFuture<DirectoryUploadResponse> uploadDirectory(DirectoryUploadRequest directoryUploadRequest);

    /**
     * Deletes the content from the substrate-specific Blob storage that contains the prefix
     * @param prefix The prefix of blobs that should be deleted (e.g. the directory)
     */
    CompletableFuture<Void> deleteDirectory(String prefix);
}
