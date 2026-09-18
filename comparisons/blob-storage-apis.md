---
layout: default
title: Blob Storage APIs
nav_order: 1
parent: Provider Implementation Comparisons
permalink: /api/implementation/latest/blob-storage-apis/
---

{% assign source_root = "https://github.com/salesforce/multicloudj/blob/cfc1fddbc2ba8f1f22bd564cc79a889ba9e96570" %}

<style>
.source-excerpt {
  display: block;
  margin-bottom: 0.35rem;
  white-space: normal;
}
</style>

# Class BucketClient

`java.lang.Object`<br>
`com.salesforce.multicloudj.blob.client.BucketClient`

```java
public class BucketClient
extends Object
implements AutoCloseable
```

`BucketClient` is the synchronous, bucket-scoped Blob Storage API. This reference compares the AWS S3, Google Cloud Storage, and Alibaba Cloud OSS implementations behind the same public contract.

See the [generated BucketClient Java API]({{ site.baseurl }}/api/java/latest/com/salesforce/multicloudj/blob/client/BucketClient.html) for parameter, return-value, and exception documentation. This page focuses on the provider behavior hidden behind that API.

**Source snapshot:** [MultiCloudJ cfc1fddb]({{ source_root }}) (`multicloudj-v0.4.4-8-gcfc1fddb`), reviewed 2026-09-03. Native SDK versions are AWS SDK for Java v2 2.42.40, Google Cloud Storage 2.62.0, and Alibaba Cloud OSS v2 0.4.0.

## Class overview

| Concern | AWS S3 | Google Cloud Storage | Alibaba Cloud OSS |
|---|---|---|---|
| Primary client | Retained synchronous `S3Client` | Retained `Storage` client | Retained OSS v2 `OSSClient` |
| Standard transport | AWS synchronous HTTP | GCS JSON over HTTP | OSS v2 HTTP |
| Additional clients | `S3Presigner` per presign call | Separate HTTP multipart client and `TransferManager` | None for multipart or presigning |
| Retry owner | AWS SDK | GCS SDK/GAX | OSS v2 SDK |
| MultiCloudJ retry loop | None | None | None |
| Resource lifecycle | Closes `S3Client` | Closes `TransferManager` and `Storage` | Closes `OSSClient` |

> The standard public GCP builder uses GCS JSON/HTTP, not gRPC. A `GrpcStorageImpl` stack trace indicates an injected native `Storage`, different deployed code, or another GCS client path.

## Method summary

The table covers all 44 public methods declared directly on `BucketClient`. Overloads that add only `OperationContext` delegate to the same provider operation; the context affects common tracing and correlation behavior.

| Modifier and type | Method | Description |
|---|---|---|
| `static BlobBuilder` | [`builder(String)`](#builderstring) | Creates a provider-specific client builder. |
| `String` | [`getBucket()`](#getbucket) | Returns the configured bucket name. |
| `UploadResponse` | [`upload(UploadRequest, InputStream)`](#upload) | Uploads a stream. |
| `UploadResponse` | [`upload(UploadRequest, byte[])`](#upload) | Uploads in-memory bytes. |
| `UploadResponse` | [`upload(UploadRequest, File)`](#upload) | Uploads a file. |
| `UploadResponse` | [`upload(UploadRequest, Path)`](#upload) | Uploads a path. |
| `DownloadResponse` | [`download(DownloadRequest, OutputStream)`](#download) | Streams an object to an output stream. |
| `DownloadResponse` | [`download(DownloadRequest, ByteArray)`](#download) | Downloads an object into a mutable byte holder. |
| `DownloadResponse` | [`download(DownloadRequest, File)`](#download) | Downloads an object to a file. |
| `DownloadResponse` | [`download(DownloadRequest, Path)`](#download) | Downloads an object to a path. |
| `DownloadResponse` | [`download(DownloadRequest)`](#download) | Returns a live object input stream. |
| `void` | [`delete(String, String)`](#delete) | Deletes one object or version. |
| `void` | [`delete(String, String, OperationContext)`](#delete) | Deletes one object with explicit operation context. |
| `void` | [`delete(Collection)`](#delete) | Deletes multiple objects. |
| `void` | [`delete(Collection, OperationContext)`](#delete) | Deletes multiple objects with explicit operation context. |
| `CopyResponse` | [`copy(CopyRequest)`](#copy) | Copies from this bucket to a destination. |
| `CopyResponse` | [`copyFrom(CopyFromRequest)`](#copyfrom) | Copies from a source bucket into this bucket. |
| `BlobMetadata` | [`getMetadata(String, String)`](#getmetadata) | Reads object metadata. |
| `BlobMetadata` | [`getMetadata(String, String, OperationContext)`](#getmetadata) | Reads metadata with explicit operation context. |
| `Iterator<BlobInfo>` | [`list(ListBlobsRequest)`](#list) | Lazily lists objects. |
| `ListBlobsPageResponse` | [`listPage(ListBlobsPageRequest)`](#listpage) | Lists one page of objects. |
| `Iterator<BlobMetadata>` | [`listBlobVersions(ListBlobVersionsRequest)`](#listblobversions) | Lazily lists versions for one key. |
| `MultipartUpload` | [`initiateMultipartUpload(MultipartUploadRequest)`](#initiatemultipartupload) | Starts multipart upload. |
| `UploadPartResponse` | [`uploadMultipartPart(MultipartUpload, MultipartPart)`](#uploadmultipartpart) | Uploads one part. |
| `UploadPartResponse` | [`uploadMultipartPart(..., OperationContext)`](#uploadmultipartpart) | Uploads one part with explicit context. |
| `MultipartUploadResponse` | [`completeMultipartUpload(MultipartUpload, List)`](#completemultipartupload) | Completes multipart upload. |
| `MultipartUploadResponse` | [`completeMultipartUpload(..., OperationContext)`](#completemultipartupload) | Completes multipart upload with explicit context. |
| `List<UploadPartResponse>` | [`listMultipartUpload(MultipartUpload)`](#listmultipartupload) | Lists uploaded parts. |
| `List<UploadPartResponse>` | [`listMultipartUpload(..., OperationContext)`](#listmultipartupload) | Lists parts with explicit context. |
| `void` | [`abortMultipartUpload(MultipartUpload)`](#abortmultipartupload) | Aborts multipart upload. |
| `void` | [`abortMultipartUpload(..., OperationContext)`](#abortmultipartupload) | Aborts multipart upload with explicit context. |
| `Map<String,String>` | [`getTags(String)`](#gettags) | Reads object tags. |
| `Map<String,String>` | [`getTags(String, OperationContext)`](#gettags) | Reads tags with explicit context. |
| `void` | [`setTags(String, Map)`](#settags) | Replaces object tags. |
| `URL` | [`generatePresignedUrl(PresignedUrlRequest)`](#presigned-urls) | Generates a URL only. |
| `PresignedUrlResponse` | [`presign(PresignedUrlRequest)`](#presigned-urls) | Generates URL, signed headers, and expiration. |
| `boolean` | [`doesObjectExist(String, String)`](#doesobjectexist) | Checks object or version existence. |
| `boolean` | [`doesBucketExist()`](#doesbucketexist) | Checks bucket existence. |
| `BucketVersioningConfiguration` | [`getBucketVersioning()`](#getbucketversioning) | Reads versioning state. |
| `ObjectLockInfo` | [`getObjectLock(String, String)`](#getobjectlock) | Reads retention and legal-hold state. |
| `void` | [`updateObjectRetention(String, String, Instant)`](#updateobjectretention) | Updates a retain-until date using the legacy overload. |
| `void` | [`updateObjectRetention(String, String, ObjectRetentionConfig)`](#updateobjectretention) | Updates retention using the full configuration. |
| `void` | [`updateLegalHold(String, String, boolean)`](#updatelegalhold) | Applies or releases legal hold. |
| `void` | [`close()`](#close) | Releases provider resources. |

## Method details

### `builder(String)`

```java
public static BucketClient.BlobBuilder builder(String providerId)
```

Selects the SPI provider and creates its native clients when `build()` is called.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Builds or accepts one `S3Client`. | <code class="source-excerpt">return new AwsBlobStore(this, s3Client);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L820-L828) |
| Google Cloud Storage | Eagerly builds `Storage`, the preview multipart client, and `TransferManager`. | <code class="source-excerpt">return new GcpBlobStore(this, storage, mpuClient, transferManager);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L1790-L1804) |
| Alibaba Cloud OSS | Builds or accepts one OSS v2 `OSSClient`. | <code class="source-excerpt">return new AliBlobStore(this, ossClient);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L1025-L1032) |

### `getBucket()`

```java
public String getBucket()
```

All providers return the bucket value stored in the common `AbstractBlobStore`; no native request is made.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Returns the configured value. | <code class="source-excerpt">return blobStore.getBucket();</code><br>[source]({{ source_root }}/blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/client/BucketClient.java#L73-L75) |
| Google Cloud Storage | Returns the configured value. | <code class="source-excerpt">return blobStore.getBucket();</code><br>[source]({{ source_root }}/blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/client/BucketClient.java#L73-L75) |
| Alibaba Cloud OSS | Returns the configured value. | <code class="source-excerpt">return blobStore.getBucket();</code><br>[source]({{ source_root }}/blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/client/BucketClient.java#L73-L75) |

### `upload`

```java
public UploadResponse upload(UploadRequest request, InputStream stream)
public UploadResponse upload(UploadRequest request, byte[] content)
public UploadResponse upload(UploadRequest request, File file)
public UploadResponse upload(UploadRequest request, Path path)
```

Uploads one object. Request metadata, tags, storage class, encryption, checksum, content type, retention, and legal-hold support differ by provider.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Blocking `PutObject`; file/path uploads are single PUTs. Stream retryability depends on whether the request body can be replayed. Supports native tags, SSE-KMS, CRC32C/SHA256/MD5, and object-lock fields. | <code class="source-excerpt">PutObjectResponse response = s3Client.putObject(request, requestBody);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L137-L181) |
| Google Cloud Storage | Uses resumable `Storage.createFrom`. Stream `contentLength` is not consumed. Tags are stored as `gcp-tag-` custom metadata; SHA256 and CRC64 are rejected. | <code class="source-excerpt">Blob blob = storage.createFrom( transformer.toBlobInfo(request), inputStream, options);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L182-L229) |
| Alibaba Cloud OSS | Uses `BinaryData` and `putObject`; file/path uploads are not automatically multipart. MD5 is server-validated and OSS returns CRC64. Retention/hold are follow-up calls. | <code class="source-excerpt">PutObjectResult result = ossClient.putObject(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L132-L219) |

GCP allocates a 15 MiB HTTP buffer per concurrent stream upload (16 MiB aligned for an injected gRPC client). OSS callers should provide a positive content length when known.

### `download`

```java
public DownloadResponse download(DownloadRequest request, OutputStream target)
public DownloadResponse download(DownloadRequest request, ByteArray target)
public DownloadResponse download(DownloadRequest request, File target)
public DownloadResponse download(DownloadRequest request, Path target)
public DownloadResponse download(DownloadRequest request)
```

Downloads an object or byte range. The no-target overload returns a live stream that the caller must close.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | One `GetObject`; uses AWS response transformers for stream, bytes, file, and path. | <code class="source-excerpt">s3Client.getObject(request, ResponseTransformer.toOutputStream(outputStream));</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L192-L286) |
| Google Cloud Storage | Reads metadata, opens a `ReadChannel`, and applies ranges with `seek`/`limit`. Full file downloads can use `TransferManager`. | <code class="source-excerpt">try (ReadChannel reader = storage.reader(blobId)) { applyRange(reader, request, blob); }</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L235-L335) |
| Alibaba Cloud OSS | Copies one OSS response body with a 16 KiB buffer; validates unsatisfied ranges when OSS returns a full-object response. | <code class="source-excerpt">GetObjectResult result = ossClient.getObject(request); copyStream(result.body(), outputStream);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L230-L355) |

### `delete`

```java
public void delete(String key, String versionId)
public void delete(String key, String versionId, OperationContext context)
public void delete(Collection<BlobIdentifier> objects)
public void delete(Collection<BlobIdentifier> objects, OperationContext context)
```

Deletes one object/version or a collection. `OperationContext` overloads make the same provider calls.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Direct `DeleteObject` or one `DeleteObjects`; the batch is not partitioned above 1,000 keys. | <code class="source-excerpt">s3Client.deleteObject(deleteRequest); s3Client.deleteObjects(deleteRequests);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L321-L333) |
| Google Cloud Storage | Performs a list-access bucket probe, then `Storage.delete`; therefore delete requires list permission too. | <code class="source-excerpt">validateBucketExists(key); storage.delete(transformer.toBlobId(bucket, key, versionId));</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L473-L486) |
| Alibaba Cloud OSS | Direct delete or one quiet `deleteMultipleObjects`; individual batch results are not surfaced. | <code class="source-excerpt">ossClient.deleteMultipleObjects(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L517-L541) |

### `copy`

```java
public CopyResponse copy(CopyRequest request)
```

Copies an object from the current bucket to the request's destination.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | One native `CopyObject`. | <code class="source-excerpt">CopyObjectResponse response = s3Client.copyObject(copyRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L342-L351) |
| Google Cloud Storage | Uses GCS rewrite/copy and waits for the result. | <code class="source-excerpt">Blob blob = storage.copy(copyReq).getResult();</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L489-L494) |
| Alibaba Cloud OSS | One `copyObject`; follows with `headObject` only when last-modified is absent. | <code class="source-excerpt">CopyObjectResult result = ossClient.copyObject(copyRequest, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L547-L595) |

### `copyFrom`

```java
public CopyResponse copyFrom(CopyFromRequest request)
```

Copies a source object into the bucket represented by this client. Source version/generation is preserved in each native request.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Native `CopyObject` with source version in the encoded copy source. | <code class="source-excerpt">s3Client.copyObject(transformer.toRequest(request));</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L355-L364) |
| Google Cloud Storage | GCS rewrite/copy with source generation. | <code class="source-excerpt">Blob blob = storage.copy(copyReq).getResult();</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L496-L501) |
| Alibaba Cloud OSS | Native copy with source version. | <code class="source-excerpt">ossClient.copyObject(copyRequest, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L563-L571) |

### `getMetadata`

```java
public BlobMetadata getMetadata(String key, String versionId)
public BlobMetadata getMetadata(String key, String versionId, OperationContext context)
```

Reads metadata without downloading content. The context overload makes the same native call.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | `HeadObject`. | <code class="source-excerpt">HeadObjectResponse response = s3Client.headObject(request);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L371-L376) |
| Google Cloud Storage | `Storage.get`; a null blob is normalized to not found. | <code class="source-excerpt">Blob blob = getRequiredBlob(blobId);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L503-L508) |
| Alibaba Cloud OSS | `headObject`; exposes OSS CRC64 and maps created time from last-modified. | <code class="source-excerpt">HeadObjectResult result = ossClient.headObject(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L601-L610) |

### `list`

```java
public Iterator<BlobInfo> list(ListBlobsRequest request)
```

Returns a lazy iterator. Later provider page requests can fail after the public method has returned.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Lazy `ListObjectsV2` iterator. | <code class="source-excerpt">return new BlobInfoIterator( s3Client, getBucket(), request);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L383-L385) |
| Google Cloud Storage | Lazy `iterateAll()` with directory placeholders filtered. | <code class="source-excerpt">storage.list(getBucket(), options).iterateAll();</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L510-L551) |
| Alibaba Cloud OSS | Lazy OSS `ListObjectsV2` iterator. | <code class="source-excerpt">return new BlobInfoIterator( ossClient, transformer, request);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L616-L618) |

### `listPage`

```java
public ListBlobsPageResponse listPage(ListBlobsPageRequest request)
```

Returns exactly one provider page. Continuation tokens are provider-opaque.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | One `ListObjectsV2`; returns contents and common prefixes. | <code class="source-excerpt">ListObjectsV2Response response = s3Client.listObjectsV2(awsRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L394-L408) |
| Google Cloud Storage | One GCS page; directory objects become common prefixes. | <code class="source-excerpt">Page&lt;Blob&gt; page = storage.list(getBucket(), options);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L558-L587) |
| Alibaba Cloud OSS | One OSS `ListObjectsV2` page. | <code class="source-excerpt">ListObjectsV2Result response = ossClient.listObjectsV2(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L627-L633) |

### `listBlobVersions`

```java
public Iterator<BlobMetadata> listBlobVersions(ListBlobVersionsRequest request)
```

Lazily lists versions for the requested exact key.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Walks the version paginator and omits delete markers. | <code class="source-excerpt">return new BlobMetadataIterator( s3Client, getBucket(), request.getKey());</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L414-L416) |
| Google Cloud Storage | Uses bounded lexicographic generation listing plus an exact-name filter. | <code class="source-excerpt">storage.list(getBucket(), options).iterateAll();</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L594-L620) |
| Alibaba Cloud OSS | Walks the OSS object-version iterator for the requested key. | <code class="source-excerpt">return new BlobMetadataIterator( ossClient, getBucket(), request.getKey());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L637-L639) |

### `initiateMultipartUpload`

```java
public MultipartUpload initiateMultipartUpload(MultipartUploadRequest request)
```

Starts a native multipart upload and returns the provider upload identifier.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Native create-multipart request. | <code class="source-excerpt">s3Client.createMultipartUpload(createRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L425-L433) |
| Google Cloud Storage | Uses a separate preview HTTP multipart client after a bucket-access probe. | <code class="source-excerpt">multipartUploadClient.createMultipartUpload( createRequestBuilder.build());</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L628-L687) |
| Alibaba Cloud OSS | Native initiate request; explicit non-CRC64 checksum algorithms are rejected. | <code class="source-excerpt">ossClient.initiateMultipartUpload( ossRequest, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L648-L660) |

### `uploadMultipartPart`

```java
public UploadPartResponse uploadMultipartPart(MultipartUpload upload, MultipartPart part)
public UploadPartResponse uploadMultipartPart(
    MultipartUpload upload, MultipartPart part, OperationContext context)
```

Uploads one numbered part. The context overload makes the same native call.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Streams the declared part length to `UploadPart`. | <code class="source-excerpt">s3Client.uploadPart(request, RequestBody.fromInputStream(stream, contentLength));</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L441-L453) |
| Google Cloud Storage | Reads the entire input stream into a byte array before the HTTP multipart request. | <code class="source-excerpt">byte[] data = ByteStreams.toByteArray(stream); multipartUploadClient.uploadPart(request, RequestBody.of(ByteBuffer.wrap(data)));</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L691-L713) |
| Alibaba Cloud OSS | Streams `BinaryData` with the supplied part length. | <code class="source-excerpt">UploadPartResult result = ossClient.uploadPart(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L666-L678) |

### `completeMultipartUpload`

```java
public MultipartUploadResponse completeMultipartUpload(
    MultipartUpload upload, List<UploadPartResponse> parts)
public MultipartUploadResponse completeMultipartUpload(
    MultipartUpload upload, List<UploadPartResponse> parts, OperationContext context)
```

Sorts or transforms uploaded parts as required and assembles the final object.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Completes the native multipart upload. | <code class="source-excerpt">s3Client.completeMultipartUpload(completeRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L459-L468) |
| Google Cloud Storage | Sorts parts, completes through the multipart client, then applies legal hold. | <code class="source-excerpt">multipartUploadClient.completeMultipartUpload(request); applyMultipartLegalHold(mpu);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L717-L754) |
| Alibaba Cloud OSS | Completes, returns OSS CRC64, then applies retention/legal hold. | <code class="source-excerpt">ossClient.completeMultipartUpload(request, options); applyObjectLockAfterUpload(key, versionId, lock);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L684-L702) |

### `listMultipartUpload`

```java
public List<UploadPartResponse> listMultipartUpload(MultipartUpload upload)
public List<UploadPartResponse> listMultipartUpload(
    MultipartUpload upload, OperationContext context)
```

Lists one native page of uploaded parts. Implementations do not explicitly follow part pagination.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | One `ListParts` request, sorted by part number. | <code class="source-excerpt">ListPartsResponse response = s3Client.listParts(listPartsRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L474-L482) |
| Google Cloud Storage | One multipart `listParts` request. | <code class="source-excerpt">ListPartsResponse response = multipartUploadClient.listParts(request);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L782-L797) |
| Alibaba Cloud OSS | One OSS `listParts` request. | <code class="source-excerpt">ListPartsResult result = ossClient.listParts(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L707-L715) |

### `abortMultipartUpload`

```java
public void abortMultipartUpload(MultipartUpload upload)
public void abortMultipartUpload(MultipartUpload upload, OperationContext context)
```

Aborts an in-progress multipart upload. The context overload makes the same native call.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Native abort. | <code class="source-excerpt">s3Client.abortMultipartUpload(abortRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L488-L490) |
| Google Cloud Storage | Abort through the separate HTTP multipart client. | <code class="source-excerpt">multipartUploadClient.abortMultipartUpload(request);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L801-L809) |
| Alibaba Cloud OSS | Native abort. | <code class="source-excerpt">ossClient.abortMultipartUpload(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L721-L724) |

### `getTags`

```java
public Map<String, String> getTags(String key)
public Map<String, String> getTags(String key, OperationContext context)
```

Reads the common tag view. The context overload makes the same native call.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Reads the native object-tagging subresource. | <code class="source-excerpt">s3Client.getObjectTagging(taggingRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L499-L504) |
| Google Cloud Storage | Reads custom metadata keys prefixed with `gcp-tag-`. | <code class="source-excerpt">blob.getMetadata().entrySet().stream() .filter(e -&gt; e.getKey().startsWith(TAG_PREFIX));</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L912-L922) |
| Alibaba Cloud OSS | Reads native object tags. | <code class="source-excerpt">ossClient.getObjectTagging(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L743-L755) |

### `setTags`

```java
public void setTags(String key, Map<String, String> tags)
```

Replaces the common tag view.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Native put-tagging. | <code class="source-excerpt">s3Client.putObjectTagging(taggingRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L512-L514) |
| Google Cloud Storage | GET plus metadata read/modify/write without a metageneration precondition. | <code class="source-excerpt">Blob updated = blob.toBuilder().setMetadata(metadata).build(); storage.update(updated);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L925-L949) |
| Alibaba Cloud OSS | Native put-tagging. | <code class="source-excerpt">ossClient.putObjectTagging(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L762-L767) |

### Presigned URLs

```java
public URL generatePresignedUrl(PresignedUrlRequest request)
public PresignedUrlResponse presign(PresignedUrlRequest request)
```

Both APIs invoke the same provider `doPresign` implementation. `generatePresignedUrl` returns only the URL; `presign` also returns required signed headers and expiration.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Creates and closes an `S3Presigner` for each call. | <code class="source-excerpt">presigner.presignPutObject(putRequest); presigner.presignGetObject(getRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L517-L535) |
| Google Cloud Storage | Locally generates a GCS V4 signed URL. | <code class="source-excerpt">URL url = storage.signUrl( blobInfo, durationMillis, MILLISECONDS, options);</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L951-L1026) |
| Alibaba Cloud OSS | Uses the native OSS v2 presign API and expiration. | <code class="source-excerpt">PresignResult result = ossClient.presign(request, options);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L903-L926) |

### `doesObjectExist`

```java
public boolean doesObjectExist(String key, String versionId)
```

Checks one object or version without returning metadata.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | `HeadObject`; 404 becomes `false`. | <code class="source-excerpt">s3Client.headObject(headRequest); return true;</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L557-L568) |
| Google Cloud Storage | `Storage.get(...) != null`. | <code class="source-excerpt">return storage.get(blobId) != null;</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L1029-L1031) |
| Alibaba Cloud OSS | Native `doesObjectExist`, with optional version id. | <code class="source-excerpt">return ossClient.doesObjectExist(request);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L930-L940) |

### `doesBucketExist`

```java
public boolean doesBucketExist()
```

Checks whether the configured bucket exists.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | `HeadBucket`; 404 becomes `false`. | <code class="source-excerpt">s3Client.headBucket(b -&gt; b.bucket(bucket));</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L570-L581) |
| Google Cloud Storage | Bucket GET; null or 404 becomes `false`. | <code class="source-excerpt">Bucket bucketObj = storage.get(bucket); return bucketObj != null;</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L1039-L1049) |
| Alibaba Cloud OSS | Native bucket-existence API. | <code class="source-excerpt">return ossClient.doesBucketExist(bucket);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L947-L949) |

### `getBucketVersioning`

```java
public BucketVersioningConfiguration getBucketVersioning()
```

Reads the configured bucket versioning state.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Maps enabled, suspended, or absent state. | <code class="source-excerpt">s3Client.getBucketVersioning(request);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L583-L587) |
| Google Cloud Storage | Maps enabled or unversioned; GCS has no suspended state. | <code class="source-excerpt">return transformer.toBucketVersioningConfiguration( bucketObj.versioningEnabled());</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L1052-L1059) |
| Alibaba Cloud OSS | Maps OSS enabled, suspended, or absent state. | <code class="source-excerpt">ossClient.getBucketVersioning(request, options);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L958-L963) |

### `getObjectLock`

```java
public ObjectLockInfo getObjectLock(String key, String versionId)
```

Reads retention and legal-hold state.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Separate retention and legal-hold GETs; missing lock configuration is treated as unset. | <code class="source-excerpt">s3Client.getObjectRetention(retentionRequest); s3Client.getObjectLegalHold(holdRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L590-L629) |
| Google Cloud Storage | One object GET supplies retention plus temporary/event holds. | <code class="source-excerpt">Retention retention = blob.getRetention(); Boolean tempHold = blob.getTemporaryHold();</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L1291-L1331) |
| Alibaba Cloud OSS | Separate retention and legal-hold GETs; missing configuration is treated as unset without masking a missing object. | <code class="source-excerpt">ossClient.getObjectRetention(retentionRequest, options); ossClient.getObjectLegalHold(holdRequest, options);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L770-L811) |

### `updateObjectRetention`

```java
@Deprecated
public void updateObjectRetention(String key, String versionId, Instant retainUntilDate)

public void updateObjectRetention(
    String key, String versionId, ObjectRetentionConfig config)
```

The full configuration overload applies common validation before the provider hook. The deprecated overload preserves historical provider behavior where overridden.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Uses retention GET/PUT. Legacy overload rejects compliance changes; full config applies common mode and bypass rules. | <code class="source-excerpt">s3Client.putObjectRetention( transformer.toPutObjectRetentionRequest(...));</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L632-L712) |
| Google Cloud Storage | GET plus field-level object patch; governance shortening uses `overrideUnlockedRetention(true)`. | <code class="source-excerpt">storage.update(updatedBlobInfo, Storage.BlobTargetOption.overrideUnlockedRetention(true));</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L1341-L1444) |
| Alibaba Cloud OSS | GET plus retention PUT. Retention mode is immutable; governance-to-compliance upgrade is rejected. | <code class="source-excerpt">ossClient.putObjectRetention( transformer.toPutObjectRetentionRequest(...), options);</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L842-L893) |

### `updateLegalHold`

```java
public void updateLegalHold(String key, String versionId, boolean legalHold)
```

Applies or releases the provider's object-level legal hold representation.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Native `PutObjectLegalHold`. | <code class="source-excerpt">s3Client.putObjectLegalHold(legalHoldRequest);</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L715-L717) |
| Google Cloud Storage | GET plus object update, preserving the existing hold type when possible. | <code class="source-excerpt">builder.setTemporaryHold(legalHold); storage.update(builder.build());</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L1467-L1484) |
| Alibaba Cloud OSS | Native `putObjectLegalHold`. | <code class="source-excerpt">ossClient.putObjectLegalHold(request, OperationOptions.defaults());</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L896-L900) |

### `close`

```java
public void close() throws Exception
```

Releases provider resources. Applications should close each `BucketClient` they build.

| Provider | Implementation behavior | Short implementation excerpt |
|---|---|---|
| AWS S3 | Closes the retained `S3Client`. | <code class="source-excerpt">s3Client.close();</code><br>[source]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java#L721-L726) |
| Google Cloud Storage | Closes `TransferManager` and `Storage`; closing the standard HTTP `Storage` is a no-op in the pinned SDK. | <code class="source-excerpt">transferManager.close(); storage.close();</code><br>[source]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java#L1517-L1530) |
| Alibaba Cloud OSS | Closes the retained `OSSClient`. | <code class="source-excerpt">ossClient.close();</code><br>[source]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java#L966-L975) |

## Retry configuration

MultiCloudJ does not retry Blob operations itself. `RetryConfig` is translated into provider-native settings; an exception's retryable flag is only a signal after the native SDK has finished.

| Behavior | AWS S3 | Google Cloud Storage | Alibaba Cloud OSS |
|---|---|---|---|
| No common `RetryConfig` | Native AWS policy | Native GCS defaults, subject to idempotency | Native OSS v2 default retryer |
| `maxAttempts` | `StandardRetryStrategy`; includes initial call | GAX `RetrySettings`; does not change idempotency classification | `StandardRetryer.maxAttempts` |
| Exponential delay | Initial/max delay; AWS owns exponent | Initial/max delay and multiplier | Equal-jitter backoff with internal 2x factor |
| Fixed delay | Native fixed-delay backoff | Initial/max delay equal, multiplier 1 | Native fixed-delay backoff |
| `attemptTimeout` | API-call attempt timeout | Generic RPC timeout, not ordinary HTTP connect/read timeout | OSS `readWriteTimeout`; overrides socket timeout |
| `totalTimeout` | API-call timeout | Retry/backoff budget | Not applied; OSS v2 has no equivalent overall deadline |

### GCP partial-configuration caveat

`GcpTransformer.toGcpRetrySettings()` starts from a blank `RetrySettings.Builder`, not the GCS service defaults. Unset duration and attempt-count fields become zero, which can yield effectively unbounded zero-delay retries for operations GCS considers retryable. Omitting `withRetryConfig(...)` preserves native GCS defaults.

## Builder option comparison

| Builder option | AWS S3 | Google Cloud Storage | Alibaba Cloud OSS |
|---|---|---|---|
| `withBucket` | Used | Used | Used |
| `withRegion` | Applied | Stored but not consumed | Applied |
| `withEndpoint` | Native override | Normalized host for main/multipart clients | Applied |
| `withProxyEndpoint` | Apache proxy | Switches main client to Apache transport | OSS host:port proxy |
| `withMaxConnections` | Apache max connections | Apache total/per-route max | Apache5 max connections |
| `withSocketTimeout` | Apache read timeout | Apache default; request settings may override | OSS read/write timeout unless attempt timeout is set |
| `withIdleConnectionTimeout` | Max idle setting | Apache idle eviction | Apache5 keep-alive timeout |
| `withCredentialsOverrider` | Built client only | Built clients, not injected `Storage` | Built client, not injected `OSSClient` |
| `withRetryConfig` | AWS retry/timeouts | Main/multipart settings, not injected `Storage` | OSS retryer and read/write timeout |
| `withUseSystemPropertyProxyValues` | Used | Not consumed | Not consumed |
| `withUseEnvironmentVariableProxyValues` | Used | Not consumed | Not consumed |
| `withQuotaProjectId` | Not applicable | Multipart client only | Not consumed |
| `withTracingPolicy` | Common wrapper | Common wrapper | Common wrapper |

## Conformance coverage and limits

AWS, GCP, and Alibaba OSS each extend the common Blob conformance suite. The suite establishes broad happy-path API parity, but it does not establish identical internals.

Not covered by common conformance testing:

- retry count, idempotency gating, backoff timing, or partial retry configuration;
- HTTP pool or GCP gRPC-channel exhaustion;
- mid-stream interruption, stale connections, or connection acquisition;
- shared native-client ownership and concurrent close;
- high-concurrency heap, thread, socket, or file-descriptor pressure;
- large batch deletes or multipart part counts;
- per-object batch-delete failures; and
- an injected GCP gRPC `Storage` client.

## Source files

- [Public API: `BucketClient.java`]({{ source_root }}/blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/client/BucketClient.java)
- [Common driver: `AbstractBlobStore.java`]({{ source_root }}/blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/driver/AbstractBlobStore.java)
- [AWS implementation: `AwsBlobStore.java`]({{ source_root }}/blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java)
- [GCP implementation: `GcpBlobStore.java`]({{ source_root }}/blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java)
- [Alibaba implementation: `AliBlobStore.java`]({{ source_root }}/blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliBlobStore.java)

This page covers the synchronous bucket-level client. `BlobClient` service-level bucket listing and asynchronous/directory APIs are separate contracts.
