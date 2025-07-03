package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.TagSet;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
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
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.provider.Provider;
import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Alibaba implementation of BlobStore
 */
@AutoService(AbstractBlobStore.class)
public class AliBlobStore extends AbstractBlobStore<AliBlobStore> {

    private final OSS ossClient;
    private final AliTransformer transformer;

    public AliBlobStore() {
        this(new Builder(), null);
    }

    public AliBlobStore(Builder builder, OSS ossClient) {
        super(builder);
        this.ossClient = ossClient;
        this.transformer = builder.getTransformerSupplier().get(bucket);
    }

    @Override
    public Provider.Builder builder() {
        return new AliBlobStore.Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        } else if (t instanceof ServiceException) {
            String errorCode = ((ServiceException) t).getErrorCode();
            return ErrorCodeMapping.getException(errorCode);
        } else if (t instanceof ClientException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }

    /**
     * Performs Blob upload
     * Note: Specifying the contentLength in the UploadRequest can dramatically improve upload efficiency
     * because the substrate SDKs do not need to buffer the contents and calculate it themselves.
     *
     * @param uploadRequest Wrapper object containing upload data
     * @param inputStream The input stream that contains the blob content
     * @return Wrapper object containing the upload result data
     */
    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream) {
        return doUpload(uploadRequest, transformer.toPutObjectRequest(uploadRequest, inputStream));
    }

    /**
     * Performs Blob upload
     *
     * @param uploadRequest Wrapper object containing upload data
     * @param content The byte array that contains the blob content
     * @return Wrapper object containing the upload result data
     */
    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, byte[] content) {
        return doUpload(uploadRequest, new ByteArrayInputStream(content));
    }

    /**
     * Performs Blob upload
     *
     * @param uploadRequest Wrapper object containing upload data
     * @param file The File that contains the blob content
     * @return Wrapper object containing the upload result data
     */
    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, File file) {
        return doUpload(uploadRequest, transformer.toPutObjectRequest(uploadRequest, file));
    }

    /**
     * Performs Blob upload
     *
     * @param uploadRequest Wrapper object containing upload data
     * @param path The Path that contains the blob content
     * @return Wrapper object containing the upload result data
     */
    @Override
    protected UploadResponse doUpload(UploadRequest uploadRequest, Path path) {
        return doUpload(uploadRequest, path.toFile());
    }

    /**
     * Helper function to upload blobs
     */
    protected UploadResponse doUpload(UploadRequest uploadRequest, PutObjectRequest request) {
        return transformer.toUploadResponse(uploadRequest, ossClient.putObject(request));
    }

    /**
     * Performs Blob download
     *
     * @param downloadRequest Wrapper object containing download data
     * @param outputStream The output stream that the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, OutputStream outputStream) {
        GetObjectRequest request = transformer.toGetObjectRequest(downloadRequest);
        try (OSSObject ossObject = ossClient.getObject(request)) {
            InputStream downloadedInputstream = ossObject.getObjectContent();
            copyStream(downloadedInputstream, outputStream);
            return transformer.toDownloadResponse(ossObject);
        } catch (IOException e) {
            throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
        }
    }

    /**
     * Performs Blob download
     *
     * @param downloadRequest Wrapper object containing download data
     * @param byteArray The byte array that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DownloadResponse downloadResponse = doDownload(downloadRequest, outputStream);
        byteArray.setBytes(outputStream.toByteArray());
        return downloadResponse;
    }

    /**
     * Performs Blob download
     *
     * @param downloadRequest Wrapper object containing download data
     * @param file The File the blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, File file) {
        return doDownload(downloadRequest, file.toPath());
    }

    /**
     * Performs Blob download
     *
     * @param downloadRequest Wrapper object containing download data
     * @param path The Path that blob content will be written to
     * @return Returns a DownloadResponse object that contains metadata about the blob
     */
    @Override
    protected DownloadResponse doDownload(DownloadRequest downloadRequest, Path path) {
        GetObjectRequest request = transformer.toGetObjectRequest(downloadRequest);
        try (OSSObject ossObject = ossClient.getObject(request)) {
            InputStream downloadedInputstream = ossObject.getObjectContent();
            Files.copy(downloadedInputstream, path);
            return transformer.toDownloadResponse(ossObject);
        } catch (IOException e) {
            throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
        }
    }

    private void copyStream(InputStream in, OutputStream out) {
        try {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes a single blob
     *
     * @param key Object name of the Blob
     * @param versionId The versionId of the blob
     */
    @Override
    protected void doDelete(String key, String versionId) {
        if(versionId == null){
            ossClient.deleteObject(bucket, key);
        }
        else {
            ossClient.deleteVersion(bucket, key, versionId);
        }
    }

    /**
     * Deletes a collection of Blobs
     *
     * @param objects A collection of blob identifiers to delete
     */
    @Override
    protected void doDelete(Collection<BlobIdentifier> objects) {

        // Split the BlobIdentifiers into collections of those with versionIds and those without
        Map<Boolean, List<BlobIdentifier>> partitionedIdentifiers = objects.stream()
                .collect(Collectors.partitioningBy(identifier -> identifier.getVersionId()!=null));

        List<BlobIdentifier> unversionedObjects = partitionedIdentifiers.get(false);
        List<BlobIdentifier> versionedObjects = partitionedIdentifiers.get(true);
        if(!versionedObjects.isEmpty()) {
            ossClient.deleteVersions(transformer.toDeleteVersionsRequest(versionedObjects));
        }
        if(!unversionedObjects.isEmpty()) {
            ossClient.deleteObjects(transformer.toDeleteObjectsRequest(unversionedObjects));
        }
    }

    /**
     * Copies a Blob to a different bucket
     *
     * @param request the copy request
     * @return CopyResponse of the copied Blob
     */
    @Override
    protected CopyResponse doCopy(CopyRequest request) {
        CopyObjectRequest copyRequest = transformer.toCopyObjectRequest(request);
        CopyObjectResult result = ossClient.copyObject(copyRequest);
        return transformer.toCopyResponse(request.getDestKey(), result);
    }

    /**
     * Retrieves the Blob metadata
     *
     * @param key Key of the Blob whose metadata is to be retrieved
     * @param versionId The versionId of the blob. This field is optional and only used if your bucket
     *                  has versioning enabled. This value should be null unless you're targeting a
     *                  specific key/version blob.
     * @return Wrapper Blob metadata object
     */
    @Override
    protected BlobMetadata doGetMetadata(String key, String versionId) {
        GenericRequest metadataRequest = transformer.toMetadataRequest(key, versionId);
        return transformer.toBlobMetadata(key, ossClient.getObjectMetadata(metadataRequest));
    }

    /**
     * Lists all objects in the bucket
     *
     * @return Iterator of the list
     */
    @Override
    protected Iterator<BlobInfo> doList(ListBlobsRequest request) {
        return new BlobInfoIterator(ossClient, bucket, request);
    }

    /**
     * Lists a single page of objects in the bucket with pagination support
     *
     * @param request The list request containing filters and optional pagination token
     * @return ListBlobsPageResult containing the blobs, truncation status, and next page token
     */
    @Override
    protected ListBlobsPageResponse doListPage(ListBlobsPageRequest request) {
        com.aliyun.oss.model.ListObjectsRequest listRequest = transformer.toListObjectsRequest(request);
        ObjectListing response = ossClient.listObjects(listRequest);
        
        List<BlobInfo> blobs = response.getObjectSummaries().stream()
                .map(objSum -> new BlobInfo.Builder()
                        .withKey(objSum.getKey())
                        .withObjectSize(objSum.getSize())
                        .build())
                .collect(Collectors.toList());

        return new ListBlobsPageResponse(
                blobs,
                response.isTruncated(),
                response.getNextMarker()
        );
    }

    /**
     * Initiates a multipart upload
     *
     * @param request the multipart request
     * @return An object that acts as an identifier for subsequent related multipart operations
     */
    @Override
    protected MultipartUpload doInitiateMultipartUpload(final MultipartUploadRequest request){
        InitiateMultipartUploadRequest initiateMultipartUploadRequest = transformer.toInitiateMultipartUploadRequest(request);
        InitiateMultipartUploadResult initiateMultipartUploadResult = ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
        return transformer.toMultipartUpload(initiateMultipartUploadResult);
    }

    /**
     * Uploads a part of the multipartUpload operation
     *
     * @param mpu The multipartUpload identifier
     * @param mpp The part to be uploaded
     * @return Returns an identifier of the uploaded part
     */
    @Override
    protected UploadPartResponse doUploadMultipartPart(final MultipartUpload mpu, final MultipartPart mpp){
        UploadPartRequest uploadPartRequest = transformer.toUploadPartRequest(mpu, mpp);
        UploadPartResult uploadPartResult = ossClient.uploadPart(uploadPartRequest);
        return transformer.toUploadPartResponse(mpp, uploadPartResult);
    }

    /**
     * Completes a multipartUpload operation
     *
     * @param mpu The multipartUpload identifier
     * @param parts The list of all parts that were uploaded
     * @return Returns a MultipartUploadResponse that contains an etag of the resultant blob
     */
    @Override
    protected MultipartUploadResponse doCompleteMultipartUpload(final MultipartUpload mpu, final List<UploadPartResponse> parts){
        CompleteMultipartUploadRequest completeMultipartUploadRequest = transformer.toCompleteMultipartUploadRequest(mpu, parts);
        CompleteMultipartUploadResult completeMultipartUploadResult = ossClient.completeMultipartUpload(completeMultipartUploadRequest);
        return new MultipartUploadResponse(completeMultipartUploadResult.getETag());
    }

    /**
     * List all parts that have been uploaded for the multipartUpload so far
     *
     * @param mpu The multipartUpload identifier
     * @return Returns a list of all uploaded parts
     */
    protected List<UploadPartResponse> doListMultipartUpload(final MultipartUpload mpu){
        ListPartsRequest listPartsRequest = transformer.toListPartsRequest(mpu);
        PartListing partListing = ossClient.listParts(listPartsRequest);
        return transformer.toListUploadPartResponse(partListing);
    }

    /**
     * Aborts a multipartUpload that's in progress
     *
     * @param mpu The multipartUpload identifier
     */
    protected void doAbortMultipartUpload(final MultipartUpload mpu) {
        ossClient.abortMultipartUpload(transformer.toAbortMultipartUploadRequest(mpu));
    }

    /**
     * Returns a map of all the tags associated with the blob
     * @param key Name of the blob whose tags are to be retrieved
     * @return The blob's tags
     */
    @Override
    protected Map<String, String> doGetTags(String key) {
        TagSet response = ossClient.getObjectTagging(bucket, key);
        return response.getAllTags();
    }

    /**
     * Sets tags on a blob
     * @param key Name of the blob to set tags on
     * @param tags The tags to set
     */
    @Override
    protected void doSetTags(String key, Map<String, String> tags) {
        ossClient.setObjectTagging(bucket, key, new TagSet(tags));
    }

    /**
     * Generates a presigned URL for uploading/downloading blobs
     * @param request The PresignedUrlRequest
     * @return Returns the presigned URL
     */
    @Override
    protected URL doGeneratePresignedUrl(PresignedUrlRequest request) {
        switch(request.getType()) {
            case UPLOAD:
                return ossClient.generatePresignedUrl(transformer.toPresignedUrlUploadRequest(request));
            case DOWNLOAD:
                return ossClient.generatePresignedUrl(transformer.toPresignedUrlDownloadRequest(request));
        }
        throw new InvalidArgumentException("Unsupported PresignedOperation. type="+request.getType());
    }

    /**
     * Determines if an object exists for a given key/versionId
     * @param key Name of the blob to check
     * @param versionId The version of the blob to check
     * @return Returns true if the object exists. Returns false if it doesn't exist.
     */
    @Override
    protected boolean doDoesObjectExist(String key, String versionId) {
        return ossClient.doesObjectExist(transformer.toMetadataRequest(key, versionId));
    }

    @Getter
    public static class Builder extends AbstractBlobStore.Builder<AliBlobStore> {

        private OSS client;
        private AliTransformerSupplier transformerSupplier = new AliTransformerSupplier();

        public Builder() {
            providerId(AliConstants.PROVIDER_ID);
        }

        public Builder withClient(OSS client) {
            this.client = client;
            return this;
        }

        public Builder withTransformerSupplier(AliTransformerSupplier transformerSupplier) {
            this.transformerSupplier = transformerSupplier;
            return this;
        }

        /**
         * Helper function for generating the OSS client
         */
        private static OSS buildOSSClient(Builder builder) {
            return OSSClientBuilder.create()
                    .region(builder.getRegion())
                    .endpoint(getEndpoint(builder))
                    .clientConfiguration(getClientBuilderConfiguration(builder))
                    .credentialsProvider(OSSCredentialsProvider.getCredentialsProvider(
                            builder.getCredentialsOverrider(),
                            builder.getRegion()))
                    .build();
        }

        /**
         * Helper function to produce the endpoint value
         */
        private static String getEndpoint(Builder builder) {
            if (builder.getEndpoint() != null) {
                return builder.getEndpoint().getHost();
            }
            return "https://oss-" + builder.getRegion() + ".aliyuncs.com";
        }

        /**
         * Helper function to generate the ClientBuilderConfiguration
         */
        private static ClientBuilderConfiguration getClientBuilderConfiguration(Builder builder) {
            ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
            clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
            if (builder.getProxyEndpoint() != null) {
                // Note: The proxy logic is hardwired to be HTTP-only in OSS
                clientBuilderConfiguration.setProxyHost(builder.getProxyEndpoint().getHost());
                clientBuilderConfiguration.setProxyPort(builder.getProxyEndpoint().getPort());
            }
            if(builder.getMaxConnections() != null) {
                clientBuilderConfiguration.setMaxConnections(builder.getMaxConnections());
            }
            if(builder.getSocketTimeout() != null) {
                clientBuilderConfiguration.setSocketTimeout((int) builder.getSocketTimeout().toMillis());
            }
            if(builder.getIdleConnectionTimeout() != null) {
                clientBuilderConfiguration.setIdleConnectionTime((int) builder.getIdleConnectionTimeout().toMillis());
            }
            return clientBuilderConfiguration;
        }

        @Override
        public AliBlobStore build() {
            OSS client = getClient();
            if(client == null) {
                client = buildOSSClient(this);
            }
            return new AliBlobStore(this, client);
        }
    }
}
