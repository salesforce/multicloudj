package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.service.SdkService;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This interface defines the providing common (i.e. substrate-agnostic) functionality for blob store
 */
public interface BlobStore extends SdkService, Provider {

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
     * @return Wrapper object containing the upload result data
     */
    UploadResponse upload(UploadRequest uploadRequest, InputStream inputStream);

    /**
     * Performs args validation and passes the call to substrate-specific upload method
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param content The byte array that contains the blob content
     * @return Wrapper object containing the upload result data
     */
    UploadResponse upload(UploadRequest uploadRequest, byte[] content);

    /**
     * Performs args validation and passes the call to substrate-specific upload method
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param file The File that contains the blob content
     * @return Wrapper object containing the upload result data
     */
    UploadResponse upload(UploadRequest uploadRequest, File file);

    /**
     * Performs args validation and passes the call to substrate-specific upload method
     *
     * @param uploadRequest Wrapper, containing upload data
     * @param path The Path that contains the blob content
     * @return Wrapper object containing the upload result data
     */
    UploadResponse upload(UploadRequest uploadRequest, Path path);

    /**
     * Performs args validation and passes the call to substrate-specific download method
     *
     * @param downloadRequest Wrapper, containing download data
     * @param outputStream The output stream that the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    DownloadResponse download(DownloadRequest downloadRequest, OutputStream outputStream);

    /**
     * Performs args validation and passes the call to substrate-specific download method
     *
     * @param downloadRequest Wrapper, containing download data
     * @param byteArray The byte array that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    DownloadResponse download(DownloadRequest downloadRequest, ByteArray byteArray);

    /**
     * Performs args validation and passes the call to substrate-specific download method
     *
     * @param downloadRequest Wrapper, containing download data
     * @param file The File the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    DownloadResponse download(DownloadRequest downloadRequest, File file);

    /**
     * Performs args validation and passes the call to substrate-specific download method
     *
     * @param downloadRequest Wrapper, containing download data
     * @param path The Path that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    DownloadResponse download(DownloadRequest downloadRequest, Path path);

    /**
     * Performs args validation and passes the call to substrate-specific delete method
     *
     * @param key Object name of the Blob
     * @param versionId The versionId of the blob. This field is optional and should be null
     *                  unless you're targeting the deletion of a specific key/version blob.
     */
    void delete(String key, String versionId);

    /**
     * Performs args validation and passes the call to substrate-specific delete method
     *
     * @param objects A collection of BlobIdentifiers representing the blobs to delete
     */
    void delete(Collection<BlobIdentifier> objects);

    /**
     * Performs validation and invokes substrate-specific copy method.
     *
     * @param request the copy request
     * @return CopyResponse of the copied Blob
     */
    CopyResponse copy(CopyRequest request);

    /**
     * Retrieves the metadata of the Blob
     *
     * @param key Name of the Blob, whose metadata is to be retrieved
     * @param versionId The versionId of the blob. This field is optional and only used if your bucket
     *                  has versioning enabled. This value should be null unless you're targeting a
     *                  specific key/version blob.
     * @return Metadata of the Blob
     */
    BlobMetadata getMetadata(String key, String versionId);

    /**
     * Retrieves the list of Blob in the bucket
     *
     * @return Iterator object of the Blobs
     */
    Iterator<BlobInfo> list(ListBlobsRequest request);

    /**
     * Retrieves a single page of blobs from the bucket with pagination support
     *
     * @param request The pagination request containing filters, pagination token, and max results
     * @return ListBlobsPageResponse containing the blobs, truncation status, and next page token
     */
    ListBlobsPageResponse listPage(ListBlobsPageRequest request);

    /**
     * Initiates a multipart upload
     *
     * @param request the multipart request
     * @return An object that acts as an identifier for subsequent related multipart operations
     */
    MultipartUpload initiateMultipartUpload(MultipartUploadRequest request);

    /**
     * Uploads a part of the multipartUpload operation
     *
     * @param mpu The multipartUpload identifier
     * @param mpp The part to be uploaded
     * @return Returns an identifier of the uploaded part
     */
    UploadPartResponse uploadMultipartPart(MultipartUpload mpu, MultipartPart mpp);

    /**
     * Completes a multipartUpload operation
     *
     * @param mpu The multipartUpload identifier
     * @param parts The list of all parts that were uploaded
     * @return Returns a MultipartUploadResponse that contains an etag of the resultant blob
     */
    MultipartUploadResponse completeMultipartUpload(MultipartUpload mpu, List<UploadPartResponse> parts);

    /**
     * List all parts that have been uploaded for the multipartUpload so far
     *
     * @param mpu The multipartUpload identifier
     * @return Returns a list of all uploaded parts
     */
    List<UploadPartResponse> listMultipartUpload(MultipartUpload mpu);

    /**
     * Aborts a multipartUpload that's in progress
     *
     * @param mpu The multipartUpload identifier
     */
    void abortMultipartUpload(MultipartUpload mpu);

    /**
     * Returns a map of all the tags associated with the blob
     * @param key Name of the blob whose tags are to be retrieved
     * @return The blob's tags
     */
    Map<String, String> getTags(String key);

    /**
     * Sets tags on a blob. This replaces all existing tags.
     * @param key Name of the blob to set tags on
     * @param tags The tags to set
     */
    void setTags(String key, Map<String, String> tags);

    /**
     * Generates a presigned URL for uploading/downloading blobs
     * @param request The presigned request
     * @return Returns the presigned URL
     */
    URL generatePresignedUrl(PresignedUrlRequest request);

    /**
     * Determines if an object exists for a given key/versionId
     * @param key Name of the blob to check
     * @param versionId The version of the blob to check. This field is optional and should be null
     *                  unless you're checking for the existence of a specific key/version blob.
     * @return Returns true if the object exists. Returns false if it doesn't exist.
     */
    boolean doesObjectExist(String key, String versionId);
}
