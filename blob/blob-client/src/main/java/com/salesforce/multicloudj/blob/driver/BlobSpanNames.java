package com.salesforce.multicloudj.blob.driver;

/**
 * Span names emitted by the BlobStore tracing layer when wrapping public operations on {@link
 * AbstractBlobStore}, {@link AbstractBlobClient}, and {@code AbstractAsyncBlobStore}.
 */
public final class BlobSpanNames {

  private BlobSpanNames() {}

  // Service-level operations (AbstractBlobClient).
  public static final String LIST_BUCKETS = "blob.listBuckets";
  public static final String CREATE_BUCKET = "blob.createBucket";

  // Bucket-level data path.
  public static final String UPLOAD = "blob.upload";
  public static final String DOWNLOAD = "blob.download";
  public static final String DELETE = "blob.delete";
  public static final String COPY = "blob.copy";
  public static final String COPY_FROM = "blob.copyFrom";
  public static final String GET_METADATA = "blob.getMetadata";
  public static final String LIST = "blob.list";
  public static final String LIST_PAGE = "blob.listPage";
  public static final String LIST_BLOB_VERSIONS = "blob.listBlobVersions";
  public static final String GET_TAGS = "blob.getTags";
  public static final String SET_TAGS = "blob.setTags";
  public static final String GENERATE_PRESIGNED_URL = "blob.generatePresignedUrl";
  public static final String DOES_OBJECT_EXIST = "blob.doesObjectExist";
  public static final String DOES_BUCKET_EXIST = "blob.doesBucketExist";
  public static final String GET_BUCKET_VERSIONING = "blob.getBucketVersioning";

  // Multipart operations.
  public static final String INITIATE_MULTIPART_UPLOAD = "blob.initiateMultipartUpload";
  public static final String UPLOAD_MULTIPART_PART = "blob.uploadMultipartPart";
  public static final String COMPLETE_MULTIPART_UPLOAD = "blob.completeMultipartUpload";
  public static final String LIST_MULTIPART_UPLOAD = "blob.listMultipartUpload";
  public static final String ABORT_MULTIPART_UPLOAD = "blob.abortMultipartUpload";

  // Directory operations.
  public static final String UPLOAD_DIRECTORY = "blob.uploadDirectory";
  public static final String DOWNLOAD_DIRECTORY = "blob.downloadDirectory";
  public static final String DELETE_DIRECTORY = "blob.deleteDirectory";

  // Object-lock operations.
  public static final String GET_OBJECT_LOCK = "blob.getObjectLock";
  public static final String UPDATE_OBJECT_RETENTION = "blob.updateObjectRetention";
  public static final String UPDATE_LEGAL_HOLD = "blob.updateLegalHold";
}
