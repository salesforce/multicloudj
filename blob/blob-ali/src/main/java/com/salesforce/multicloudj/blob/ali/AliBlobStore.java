package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
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
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.provider.Provider;
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
import lombok.Getter;

/** Alibaba implementation of BlobStore */
@AutoService(AbstractBlobStore.class)
public class AliBlobStore extends AbstractBlobStore {

  private final OSS ossClient;
  private final OSSClient ossV2Client;
  private final AliTransformer transformer;

  public AliBlobStore() {
    this(new Builder(), null, null);
  }

  public AliBlobStore(Builder builder, OSS ossClient, OSSClient ossV2Client) {
    super(builder);
    this.ossClient = ossClient;
    this.ossV2Client = ossV2Client;
    this.transformer = builder.getTransformerSupplier().get(bucket);
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
    } else if (t instanceof com.aliyun.sdk.service.oss2.exceptions.OperationException) {
      Throwable cause = t.getCause();
      if (cause instanceof com.aliyun.sdk.service.oss2.exceptions.ServiceException) {
        String errorCode =
            ((com.aliyun.sdk.service.oss2.exceptions.ServiceException) cause).errorCode();
        return ErrorCodeMapping.getException(errorCode);
      }
      return UnknownException.class;
    } else if (t instanceof com.aliyun.sdk.service.oss2.exceptions.ServiceException) {
      String errorCode =
          ((com.aliyun.sdk.service.oss2.exceptions.ServiceException) t).errorCode();
      return ErrorCodeMapping.getException(errorCode);
    } else if (t instanceof ClientException || t instanceof IllegalArgumentException) {
      return InvalidArgumentException.class;
    }
    return UnknownException.class;
  }

  /**
   * Performs Blob upload Note: Specifying the contentLength in the UploadRequest can dramatically
   * improve upload efficiency because the substrate SDKs do not need to buffer the contents and
   * calculate it themselves.
   *
   * @param uploadRequest Wrapper object containing upload data
   * @param inputStream The input stream that contains the blob content
   * @return Wrapper object containing the upload result data
   */
  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream) {
    long contentLength = uploadRequest.getContentLength();
    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromStream(
            inputStream, contentLength > 0 ? contentLength : null);
    return doV2Upload(uploadRequest, body);
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
    com.aliyun.sdk.service.oss2.transport.BinaryData body =
        com.aliyun.sdk.service.oss2.transport.BinaryData.fromBytes(content);
    return doV2Upload(uploadRequest, body);
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
    try {
      com.aliyun.sdk.service.oss2.transport.BinaryData body =
          com.aliyun.sdk.service.oss2.transport.BinaryData.fromStream(
              Files.newInputStream(file.toPath()), file.length());
      return doV2Upload(uploadRequest, body);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read file for upload: " + file.getPath(), e);
    }
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

  /** Helper function to upload blobs via v2 SDK */
  protected UploadResponse doV2Upload(
      UploadRequest uploadRequest,
      com.aliyun.sdk.service.oss2.transport.BinaryData body) {
    com.aliyun.sdk.service.oss2.models.PutObjectRequest request =
        transformer.toV2PutObjectRequest(uploadRequest, body);
    com.aliyun.sdk.service.oss2.models.PutObjectResult result =
        ossV2Client.putObject(request,
            com.aliyun.sdk.service.oss2.OperationOptions.defaults());
    return transformer.toUploadResponse(uploadRequest, result);
  }

  /**
   * Performs Blob download
   *
   * @param downloadRequest Wrapper object containing download data
   * @param outputStream The output stream that the blob content will be written to
   * @return Returns a DownloadResponse object that contains metadata about the blob
   */
  @Override
  protected DownloadResponse doDownload(
      DownloadRequest downloadRequest, OutputStream outputStream) {
    com.aliyun.sdk.service.oss2.models.GetObjectRequest request =
        transformer.toV2GetObjectRequest(downloadRequest);
    try (com.aliyun.sdk.service.oss2.models.GetObjectResult result =
        ossV2Client.getObject(request,
            com.aliyun.sdk.service.oss2.OperationOptions.defaults())) {
      validateRangeResponse(downloadRequest, result);
      copyStream(result.body(), outputStream);
      return transformer.toDownloadResponse(downloadRequest.getKey(), result);
    } catch (IOException e) {
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
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
    Path destinationPath =
        createDownloadDestinationPath(downloadRequest, path);
    com.aliyun.sdk.service.oss2.models.GetObjectRequest request =
        transformer.toV2GetObjectRequest(downloadRequest);
    try (com.aliyun.sdk.service.oss2.models.GetObjectResult result =
        ossV2Client.getObject(request,
            com.aliyun.sdk.service.oss2.OperationOptions.defaults())) {
      validateRangeResponse(downloadRequest, result);
      Files.copy(result.body(), destinationPath);
      return transformer.toDownloadResponse(downloadRequest.getKey(), result);
    } catch (IOException e) {
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to download Blob: " + downloadRequest.getKey(), e);
    }
  }

  /**
   * Performs Blob download and returns an InputStream
   *
   * @param downloadRequest Wrapper object containing download data
   * @return Returns a DownloadResponse object that contains metadata about the blob and an
   *     InputStream for reading the content
   */
  @Override
  public DownloadResponse doDownload(DownloadRequest downloadRequest) {
    com.aliyun.sdk.service.oss2.models.GetObjectRequest request =
        transformer.toV2GetObjectRequest(downloadRequest);
    com.aliyun.sdk.service.oss2.models.GetObjectResult result =
        ossV2Client.getObject(request,
            com.aliyun.sdk.service.oss2.OperationOptions.defaults());
    try {
      validateRangeResponse(downloadRequest, result);
    } catch (RuntimeException e) {
      try {
        result.close();
      } catch (Exception ignored) {
        // best-effort cleanup
      }
      throw e;
    }
    return transformer.toDownloadResponse(downloadRequest.getKey(), result, result.body());
  }

  // OSS returns the full object with HTTP 200 when range start exceeds object size,
  // unlike S3/GCS which return HTTP 416. Detect via contentRange absence and throw
  // to make behavior consistent with other substrates.
  private void validateRangeResponse(
      DownloadRequest downloadRequest,
      com.aliyun.sdk.service.oss2.models.GetObjectResult result) {
    if (downloadRequest.getStart() == null) {
      return;
    }
    if (result.contentRange() == null) {
      Long contentLength = result.contentLength();
      long objectSize = contentLength != null ? contentLength : 0L;
      if (downloadRequest.getStart() >= objectSize) {
        throw new InvalidArgumentException(
            "The requested range start ("
                + downloadRequest.getStart()
                + ") is not satisfiable for object of size "
                + objectSize);
      }
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
    if (versionId == null) {
      ossClient.deleteObject(bucket, key);
    } else {
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
    Map<Boolean, List<BlobIdentifier>> partitionedIdentifiers =
        objects.stream()
            .collect(Collectors.partitioningBy(identifier -> identifier.getVersionId() != null));

    List<BlobIdentifier> unversionedObjects = partitionedIdentifiers.get(false);
    List<BlobIdentifier> versionedObjects = partitionedIdentifiers.get(true);
    if (!versionedObjects.isEmpty()) {
      ossClient.deleteVersions(transformer.toDeleteVersionsRequest(versionedObjects));
    }
    if (!unversionedObjects.isEmpty()) {
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
    com.aliyun.sdk.service.oss2.models.CopyObjectRequest copyRequest =
        transformer.toV2CopyObjectRequest(request);
    com.aliyun.sdk.service.oss2.models.CopyObjectResult result =
        ossV2Client.copyObject(copyRequest,
            com.aliyun.sdk.service.oss2.OperationOptions.defaults());
    return buildCopyResponse(request.getDestKey(), result);
  }

  /**
   * Copies a Blob from a source bucket to the current bucket
   *
   * @param request the copyFrom request
   * @return CopyResponse of the copied Blob
   */
  @Override
  protected CopyResponse doCopyFrom(CopyFromRequest request) {
    com.aliyun.sdk.service.oss2.models.CopyObjectRequest copyRequest =
        transformer.toV2CopyObjectRequest(request);
    com.aliyun.sdk.service.oss2.models.CopyObjectResult result =
        ossV2Client.copyObject(copyRequest,
            com.aliyun.sdk.service.oss2.OperationOptions.defaults());
    return buildCopyResponse(request.getDestKey(), result);
  }

  private CopyResponse buildCopyResponse(
      String destKey, com.aliyun.sdk.service.oss2.models.CopyObjectResult result) {
    CopyResponse response = transformer.toCopyResponse(destKey, result);
    if (response.getLastModified() == null) {
      com.aliyun.sdk.service.oss2.models.HeadObjectRequest headRequest =
          transformer.toHeadObjectRequest(destKey, response.getVersionId());
      com.aliyun.sdk.service.oss2.models.HeadObjectResult headResult =
          ossV2Client.headObject(headRequest,
              com.aliyun.sdk.service.oss2.OperationOptions.defaults());
      return CopyResponse.builder()
          .key(response.getKey())
          .versionId(response.getVersionId())
          .eTag(response.getETag())
          .lastModified(transformer.parseLastModified(headResult.lastModified()))
          .build();
    }
    return response;
  }

  /**
   * Retrieves the Blob metadata
   *
   * @param key Key of the Blob whose metadata is to be retrieved
   * @param versionId The versionId of the blob. This field is optional and only used if your bucket
   *     has versioning enabled. This value should be null unless you're targeting a specific
   *     key/version blob.
   * @return Wrapper Blob metadata object
   */
  @Override
  protected BlobMetadata doGetMetadata(String key, String versionId) {
    com.aliyun.sdk.service.oss2.models.HeadObjectRequest request =
        transformer.toHeadObjectRequest(key, versionId);
    com.aliyun.sdk.service.oss2.models.HeadObjectResult result =
        ossV2Client.headObject(request,
            com.aliyun.sdk.service.oss2.OperationOptions.defaults());
    return transformer.toBlobMetadata(key, result);
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

    List<BlobInfo> blobs =
        response.getObjectSummaries().stream()
            .map(
                objSum ->
                    new BlobInfo.Builder()
                        .withKey(objSum.getKey())
                        .withObjectSize(objSum.getSize())
                        .build())
            .collect(Collectors.toList());

    List<String> commonPrefixes = response.getCommonPrefixes();

    return new ListBlobsPageResponse(
        blobs, commonPrefixes, response.isTruncated(), response.getNextMarker());
  }

  /**
   * Initiates a multipart upload
   *
   * @param request the multipart request
   * @return An object that acts as an identifier for subsequent related multipart operations
   */
  @Override
  protected MultipartUpload doInitiateMultipartUpload(final MultipartUploadRequest request) {
    InitiateMultipartUploadRequest initiateMultipartUploadRequest =
        transformer.toInitiateMultipartUploadRequest(request);
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
    return transformer.toMultipartUpload(initiateMultipartUploadResult, request);
  }

  /**
   * Uploads a part of the multipartUpload operation
   *
   * @param mpu The multipartUpload identifier
   * @param mpp The part to be uploaded
   * @return Returns an identifier of the uploaded part
   */
  @Override
  protected UploadPartResponse doUploadMultipartPart(
      final MultipartUpload mpu, final MultipartPart mpp) {
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
  protected MultipartUploadResponse doCompleteMultipartUpload(
      final MultipartUpload mpu, final List<UploadPartResponse> parts) {
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        transformer.toCompleteMultipartUploadRequest(mpu, parts);
    CompleteMultipartUploadResult completeMultipartUploadResult =
        ossClient.completeMultipartUpload(completeMultipartUploadRequest);
    return new MultipartUploadResponse(completeMultipartUploadResult.getETag());
  }

  /**
   * List all parts that have been uploaded for the multipartUpload so far
   *
   * @param mpu The multipartUpload identifier
   * @return Returns a list of all uploaded parts
   */
  protected List<UploadPartResponse> doListMultipartUpload(final MultipartUpload mpu) {
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
   *
   * @param key Name of the blob whose tags are to be retrieved
   * @return The blob's tags
   */
  @Override
  protected Map<String, String> doGetTags(String key) {
    com.aliyun.sdk.service.oss2.models.GetObjectTaggingRequest request =
        com.aliyun.sdk.service.oss2.models.GetObjectTaggingRequest.newBuilder()
            .bucket(bucket)
            .key(key)
            .build();
    com.aliyun.sdk.service.oss2.models.GetObjectTaggingResult result =
        ossV2Client.getObjectTagging(request,
            com.aliyun.sdk.service.oss2.OperationOptions.defaults());
    return transformer.toTagMap(result);
  }

  /**
   * Sets tags on a blob
   *
   * @param key Name of the blob to set tags on
   * @param tags The tags to set
   */
  @Override
  protected void doSetTags(String key, Map<String, String> tags) {
    com.aliyun.sdk.service.oss2.models.PutObjectTaggingRequest request =
        transformer.toPutObjectTaggingRequest(key, tags);
    ossV2Client.putObjectTagging(request,
        com.aliyun.sdk.service.oss2.OperationOptions.defaults());
  }

  /** {@inheritdoc} */
  @Override
  public ObjectLockInfo getObjectLock(String key, String versionId) {
    throw new UnSupportedOperationException("Alibaba OSS does not support object lock");
  }

  /** {@inheritdoc} */
  @Override
  public void updateObjectRetention(
      String key, String versionId, java.time.Instant retainUntilDate) {
    throw new UnSupportedOperationException("Alibaba OSS does not support object lock/retention");
  }

  /** {@inheritdoc} */
  @Override
  public void updateLegalHold(String key, String versionId, boolean legalHold) {
    throw new UnSupportedOperationException("Alibaba OSS does not support object lock/legal hold");
  }

  /**
   * Generates a presigned URL for uploading/downloading blobs
   *
   * @param request The PresignedUrlRequest
   * @return Returns the presigned URL
   */
  @Override
  protected URL doGeneratePresignedUrl(PresignedUrlRequest request) {
    com.aliyun.sdk.service.oss2.PresignOptions options = transformer.toPresignOptions(request);
    com.aliyun.sdk.service.oss2.models.PresignResult result;
    switch (request.getType()) {
      case UPLOAD:
        result = ossV2Client.presign(transformer.toPresignedPutObjectRequest(request), options);
        break;
      case DOWNLOAD:
        result = ossV2Client.presign(transformer.toPresignedGetObjectRequest(request), options);
        break;
      default:
        throw new InvalidArgumentException(
            "Unsupported PresignedOperation. type=" + request.getType());
    }
    try {
      return new URL(result.url());
    } catch (java.net.MalformedURLException e) {
      throw new RuntimeException("Invalid presigned URL: " + result.url(), e);
    }
  }

  /** {@inheritdoc} */
  @Override
  protected boolean doDoesObjectExist(String key, String versionId) {
    com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest.Builder reqBuilder =
        com.aliyun.sdk.service.oss2.models.GetObjectMetaRequest.newBuilder()
            .bucket(bucket)
            .key(key);
    if (versionId != null) {
      reqBuilder.versionId(versionId);
    }
    return ossV2Client.doesObjectExist(reqBuilder.build());
  }

  /**
   * Determines if the bucket exists
   *
   * @return Returns true if the bucket exists. Returns false if it doesn't exist.
   */
  @Override
  protected boolean doDoesBucketExist() {
    return ossV2Client.doesBucketExist(bucket);
  }

  /** Closes the underlying OSS client and releases any resources. */
  @Override
  public void close() {
    if (ossClient != null) {
      ossClient.shutdown();
    }
  }

  @Getter
  public static class Builder extends AbstractBlobStore.Builder<AliBlobStore, Builder> {

    private OSS client;
    private OSSClient v2Client;
    private AliTransformerSupplier transformerSupplier = new AliTransformerSupplier();

    public Builder() {
      providerId(AliConstants.PROVIDER_ID);
    }

    @Override
    public Builder self() {
      return this;
    }

    public Builder withClient(OSS client) {
      this.client = client;
      return this;
    }

    public Builder withV2Client(OSSClient v2Client) {
      this.v2Client = v2Client;
      return this;
    }

    public Builder withTransformerSupplier(AliTransformerSupplier transformerSupplier) {
      this.transformerSupplier = transformerSupplier;
      return this;
    }

    /** Helper function for generating the v1 OSS client */
    private static OSS buildOSSClient(Builder builder) {
      return OSSClientBuilder.create()
          .region(builder.getRegion())
          .endpoint(getEndpoint(builder))
          .clientConfiguration(getClientBuilderConfiguration(builder))
          .credentialsProvider(
              OSSCredentialsProvider.getCredentialsProvider(
                  builder.getCredentialsOverrider(), builder.getRegion()))
          .build();
    }

    /** Helper function for generating the v2 OSS client. */
    private static OSSClient buildOSSV2Client(Builder builder) {
      com.aliyun.sdk.service.oss2.credentials.CredentialsProvider v2Creds =
          OSSCredentialsProvider.getV2CredentialsProvider(
              builder.getCredentialsOverrider());
      if (v2Creds == null) {
        return null;
      }

      var v2Builder = OSSClient.newBuilder()
          .region(builder.getRegion())
          .credentialsProvider(v2Creds);

      if (builder.getEndpoint() != null) {
        v2Builder.endpoint(builder.getEndpoint().toString());
      }
      if (builder.getProxyEndpoint() != null) {
        v2Builder.proxyHost(
            builder.getProxyEndpoint().getHost()
                + ":" + builder.getProxyEndpoint().getPort());
      }

      return v2Builder.build();
    }

    /** Helper function to produce the endpoint value */
    private static String getEndpoint(Builder builder) {
      if (builder.getEndpoint() != null) {
        return builder.getEndpoint().toString();
      }
      return "https://oss-" + builder.getRegion() + ".aliyuncs.com";
    }

    /** Helper function to generate the ClientBuilderConfiguration */
    private static ClientBuilderConfiguration getClientBuilderConfiguration(Builder builder) {
      ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
      clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
      if (builder.getProxyEndpoint() != null) {
        clientBuilderConfiguration.setProxyHost(builder.getProxyEndpoint().getHost());
        clientBuilderConfiguration.setProxyPort(builder.getProxyEndpoint().getPort());
      }
      if (builder.getMaxConnections() != null) {
        clientBuilderConfiguration.setMaxConnections(builder.getMaxConnections());
      }
      if (builder.getSocketTimeout() != null) {
        clientBuilderConfiguration.setSocketTimeout((int) builder.getSocketTimeout().toMillis());
      }
      if (builder.getIdleConnectionTimeout() != null) {
        clientBuilderConfiguration.setIdleConnectionTime(
            (int) builder.getIdleConnectionTimeout().toMillis());
      }
      return clientBuilderConfiguration;
    }

    @Override
    public AliBlobStore build() {
      OSS v1 = getClient();
      if (v1 == null) {
        v1 = buildOSSClient(this);
      }
      OSSClient v2 = this.v2Client;
      if (v2 == null) {
        v2 = buildOSSV2Client(this);
      }
      return new AliBlobStore(this, v1, v2);
    }
  }
}
