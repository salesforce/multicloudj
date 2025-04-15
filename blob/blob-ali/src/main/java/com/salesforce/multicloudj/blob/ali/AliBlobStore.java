package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteVersionsRequest;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PartSummary;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Alibaba implementation of BlobService
 */
@AutoService(AbstractBlobStore.class)
public class AliBlobStore extends AbstractBlobStore<AliBlobStore> {

    private OSS ossClient;

    public AliBlobStore(Builder builder) {
        super(builder);

        String endpoint = "https://oss-" + region + ".aliyuncs.com";
        if (builder.getEndpoint() != null) {
            endpoint = builder.getEndpoint().getHost();
        }
        OSSClientBuilder.OSSClientBuilderImpl impl = OSSClientBuilder.create().endpoint(endpoint);
        ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
        clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
        if (builder.getProxyEndpoint() != null) {
            // Note: The proxy logic is hardwired to be HTTP-only in OSS
            clientBuilderConfiguration.setProxyHost(builder.getProxyEndpoint().getHost());
            clientBuilderConfiguration.setProxyPort(builder.getProxyEndpoint().getPort());
        }
        CredentialsProvider credentialsProvider = OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, region);
        if (credentialsProvider != null) {
            impl.credentialsProvider(credentialsProvider);
        }
        impl.clientConfiguration(clientBuilderConfiguration);
        ossClient = impl.build();
    }

    public AliBlobStore() {
        super(new Builder());
    }

    @Override
    public Provider.Builder builder() {
        return new Builder();
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
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setUserMetadata(uploadRequest.getMetadata());
        metadata.setObjectTagging(uploadRequest.getTags());
        PutObjectRequest request = new PutObjectRequest(bucket, uploadRequest.getKey(), inputStream, metadata);

        PutObjectResult result = ossClient.putObject(request);
        return UploadResponse.builder()
                .key(uploadRequest.getKey())
                .versionId(result.getVersionId())
                .eTag(result.getETag())
                .build();
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
        return doUpload(uploadRequest, new PutObjectRequest(bucket, uploadRequest.getKey(), new ByteArrayInputStream(content)));
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
        return doUpload(uploadRequest, new PutObjectRequest(bucket, uploadRequest.getKey(), file));
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
        return doUpload(uploadRequest, new PutObjectRequest(bucket, uploadRequest.getKey(), path.toFile()));
    }

    /**
     * Helper function to upload blobs
     */
    protected UploadResponse doUpload(UploadRequest uploadRequest, PutObjectRequest request) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setUserMetadata(uploadRequest.getMetadata());
        metadata.setObjectTagging(uploadRequest.getTags());
        request.setMetadata(metadata);

        PutObjectResult result = ossClient.putObject(request);
        return UploadResponse.builder()
                .key(uploadRequest.getKey())
                .versionId(result.getVersionId())
                .eTag(result.getETag())
                .build();
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
        GetObjectRequest request = new GetObjectRequest(bucket, downloadRequest.getKey(), downloadRequest.getVersionId());
        try (OSSObject ossObject = ossClient.getObject(request)) {
            InputStream downloadedInputstream = ossObject.getObjectContent();
            copyStream(downloadedInputstream, outputStream);

            return DownloadResponse.builder()
                    .key(ossObject.getKey())
                    .metadata(BlobMetadata.builder()
                            .key(ossObject.getKey())
                            .versionId(ossObject.getObjectMetadata().getVersionId())
                            .eTag(ossObject.getObjectMetadata().getETag())
                            .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                            .metadata(ossObject.getObjectMetadata().getUserMetadata())
                            .objectSize(ossObject.getObjectMetadata().getContentLength())
                            .build())
                    .build();
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
        GetObjectRequest request = new GetObjectRequest(bucket, downloadRequest.getKey(), downloadRequest.getVersionId());
        try (OSSObject ossObject = ossClient.getObject(request)) {
            InputStream downloadedInputstream = ossObject.getObjectContent();
            Files.copy(downloadedInputstream, path);

            return DownloadResponse.builder()
                    .key(ossObject.getKey())
                    .metadata(BlobMetadata.builder()
                            .key(ossObject.getKey())
                            .versionId(ossObject.getObjectMetadata().getVersionId())
                            .eTag(ossObject.getObjectMetadata().getETag())
                            .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                            .metadata(ossObject.getObjectMetadata().getUserMetadata())
                            .objectSize(ossObject.getObjectMetadata().getContentLength())
                            .build())
                    .build();
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
        ossClient.deleteVersion(bucket, key, versionId);
    }

    /**
     * Deletes a collection of Blobs
     *
     * @param objects A collection of blob identifiers to delete
     */
    @Override
    protected void doDelete(Collection<BlobIdentifier> objects) {
        List<DeleteVersionsRequest.KeyVersion> objectsToDelete = new ArrayList<>();
        for(BlobIdentifier object : objects) {
            objectsToDelete.add(new DeleteVersionsRequest.KeyVersion(object.getKey(), object.getVersionId()));
        }
        ossClient.deleteVersions(new DeleteVersionsRequest(bucket).withKeys(objectsToDelete));
    }

    /**
     * Copies a Blob to a different bucket
     *
     * @param request the copy request
     * @return CopyResponse of the copied Blob
     */
    @Override
    protected CopyResponse doCopy(CopyRequest request) {
        CopyObjectRequest copyRequest = new CopyObjectRequest(
                bucket, request.getSrcKey(), request.getSrcVersionId(),
                request.getDestBucket(), request.getDestKey());

        CopyObjectResult result = ossClient.copyObject(copyRequest);
        return CopyResponse.builder()
                .key(request.getDestKey())
                .versionId(result.getVersionId())
                .eTag(result.getETag())
                .lastModified(result.getLastModified().toInstant())
                .build();
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
        GenericRequest metadataRequest = new GenericRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withVersionId(versionId);
        ObjectMetadata metadata = ossClient.getObjectMetadata(metadataRequest);
        long objectSize = metadata.getContentLength();
        Map<String, String> rawMetadata = metadata.getUserMetadata();

        return BlobMetadata.builder()
                .key(key)
                .versionId(metadata.getVersionId())
                .eTag(metadata.getETag())
                .objectSize(objectSize)
                .metadata(rawMetadata)
                .lastModified(metadata.getLastModified().toInstant())
                .build();
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
     * Initiates a multipart upload
     *
     * @param request the multipart request
     * @return An object that acts as an identifier for subsequent related multipart operations
     */
    @Override
    protected MultipartUpload doInitiateMultipartUpload(final MultipartUploadRequest request){

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setUserMetadata(request.getMetadata());

        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(getBucket(), request.getKey(), metadata);
        InitiateMultipartUploadResult initiateMultipartUploadResult = ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
        return new MultipartUpload(initiateMultipartUploadResult.getBucketName(), initiateMultipartUploadResult.getKey(), initiateMultipartUploadResult.getUploadId());
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

        UploadPartRequest uploadPartRequest = new UploadPartRequest(getBucket(), mpu.getKey(), mpu.getId(), mpp.getPartNumber(), mpp.getInputStream(), mpp.getContentLength());
        UploadPartResult uploadPartResult = ossClient.uploadPart(uploadPartRequest);

        return new UploadPartResponse(mpp.getPartNumber(), uploadPartResult.getPartETag().getETag(), mpp.getContentLength());
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

        List<PartETag> completedParts = parts.stream()
                .sorted(Comparator.comparingInt(UploadPartResponse::getPartNumber))
                .map(part -> new PartETag(part.getPartNumber(), part.getEtag()))
                .collect(Collectors.toList());

        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(getBucket(), mpu.getKey(), mpu.getId(), completedParts);
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

        ListPartsRequest listPartsRequest = new ListPartsRequest(bucket, mpu.getKey(), mpu.getId());
        PartListing partListing = ossClient.listParts(listPartsRequest);

        return partListing.getParts().stream()
                .sorted(Comparator.comparingInt(PartSummary::getPartNumber))
                .map((part) -> new com.salesforce.multicloudj.blob.driver.UploadPartResponse(part.getPartNumber(), part.getETag(), part.getSize()))
                .collect(Collectors.toList());
    }

    /**
     * Aborts a multipartUpload that's in progress
     *
     * @param mpu The multipartUpload identifier
     */
    protected void doAbortMultipartUpload(final MultipartUpload mpu) {

        AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(bucket, mpu.getKey(), mpu.getId());
        ossClient.abortMultipartUpload(abortMultipartUploadRequest);
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
                return doGeneratePresignedUploadUrl(request);
            case DOWNLOAD:
                return doGeneratePresignedDownloadUrl(request);
        }
        throw new InvalidArgumentException("Unsupported PresignedOperation. type="+request.getType());
    }

    /**
     * Generates a presigned URL for uploading blobs
     * @param request The presigned upload request
     * @return Returns the presigned upload URL
     */
    protected URL doGeneratePresignedUploadUrl(PresignedUrlRequest request) {
        return ossClient.generatePresignedUrl(generatePresignedUrlUploadRequest(request));
    }

    /**
     * Helper function to generate GeneratePresignedUrlRequest for upload
     * @param request The PresignedUploadRequest
     * @return Returns the GeneratePresignedUrlRequest for a PresignedUploadRequest
     */
    protected GeneratePresignedUrlRequest generatePresignedUrlUploadRequest(PresignedUrlRequest request) {
        Date expirationDate = Date.from(Instant.now().plus(request.getDuration()));
        GeneratePresignedUrlRequest presignedUrlRequest = new GeneratePresignedUrlRequest(getBucket(), request.getKey());
        presignedUrlRequest.setExpiration(expirationDate);
        presignedUrlRequest.setMethod(HttpMethod.PUT);
        presignedUrlRequest.setUserMetadata(request.getMetadata());

        // Note: Tagging is not supported by default for OSS presigned uploads so we have to manually append it
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setObjectTagging(request.getTags());
        Object encodedTagging = metadata.getRawMetadata().get(OSSHeaders.OSS_TAGGING);
        if(encodedTagging instanceof String) {
            presignedUrlRequest.addHeader(OSSHeaders.OSS_TAGGING, (String)encodedTagging);
        }
        return presignedUrlRequest;
    }

    /**
     * Generates a presigned URL for downloading blobs
     * @param request The presigned download request
     * @return Returns the presigned download URL
     */
    protected URL doGeneratePresignedDownloadUrl(PresignedUrlRequest request) {
        Date expirationDate = Date.from(Instant.now().plus(request.getDuration()));
        GeneratePresignedUrlRequest presignedUrlRequest = new GeneratePresignedUrlRequest(getBucket(), request.getKey());
        presignedUrlRequest.setExpiration(expirationDate);
        presignedUrlRequest.setMethod(HttpMethod.GET);

        return ossClient.generatePresignedUrl(presignedUrlRequest);
    }

    public static class Builder extends AbstractBlobStore.Builder<AliBlobStore> {

        public Builder() {
            providerId(AliConstants.PROVIDER_ID);
        }

        @Override
        public AliBlobStore build() {
            return new AliBlobStore(this);
        }
    }
}
