---
layout: default
title: How to Blob store
nav_order: 2
parent: Usage Guides
---
# BucketClient

The `BucketClient` class in the `multicloudj` library provides a comprehensive, cloud-agnostic interface to interact with individual buckets in object storage services like AWS S3, Azure Blob Storage, and Google Cloud Storage.

This client enables uploading, downloading, deleting, listing, copying, and managing blob metadata and multipart uploads across multiple cloud providers.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP              | AWS | ALI | Comments |
|--------------|------------------|-----|-----|----------|
| **Basic Upload** | ✅ Supported      | ✅ Supported | ✅ Supported | Upload from InputStream, byte[], File, Path |
| **Basic Download** | ✅ Supported      | ✅ Supported | ✅ Supported | Download to OutputStream, byte[], File, Path |
| **Delete Objects** | ✅ Supported      | ✅ Supported | ✅ Supported | Single and batch delete operations |
| **Copy Objects** | ✅ Supported      | ✅ Supported | ✅ Supported | Server-side copy within and across buckets |
| **Get Metadata** | ✅ Supported      | ✅ Supported | ✅ Supported | Retrieve object metadata and properties |
| **List Objects** | ✅ Supported      | ✅ Supported | ✅ Supported | Paginated listing with filters |
| **Object Tagging** | ⏱️ End of July'25 | ✅ Supported | ✅ Supported | Get and set object tags |
| **Presigned URLs** | ✅ Supported | ✅ Supported | ✅ Supported | Generate temporary access URLs |
| **Versioning Support** | ✅ Supported | ✅ Supported | ✅ Supported | Object version-specific operations |

### Multipart Upload Features

| Feature Name | GCP               | AWS | ALI | Comments |
|--------------|-------------------|-----|-----|----------|
| **Initiate Multipart** | ️ Mid of Aug'25   | ✅ Supported | ✅ Supported | Start multipart upload session |
| **Upload Part** | Mid of Aug'25 | ✅ Supported | ✅ Supported | Upload individual parts |
| **Complete Multipart** | Mid of Aug'25 | ✅ Supported | ✅ Supported | Finalize multipart upload |
| **List Parts** | Mid of Aug'25 | ✅ Supported | ✅ Supported | List uploaded parts |
| **Abort Multipart** | Mid of Aug'25 | ✅ Supported | ✅ Supported | Cancel multipart upload |

### Advanced Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Async Operations** | ✅ Supported | ✅ Supported | 📅 In Roadmap | CompletableFuture-based async API available only for AWS |
| **Bucket Operations** | ✅ Supported | ✅ Supported | ✅ Supported | List buckets via BlobClient |

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Endpoint Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom endpoint configuration |
| **Proxy Support** | ✅ Supported | ✅ Supported | ✅ Supported | HTTP proxy configuration |
| **Credentials Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom credential providers via STS |

### Performance Configuration

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Parallel Uploads** | ✅ Supported | ✅ Supported | ✅ Supported | Multipart parallel uploads for large objects |
| **Part Buffer Size** | ⚠️ Indirect | ✅ Supported | ⚠️ API-level only | Size of each part in multipart upload |
| **Threshold Bytes** | 🔍 Under investigation | ✅ Supported | ❌ Not supported | File size threshold to trigger multipart upload |
| **Max Concurrency** | ✅ Supported | ✅ Supported | ✅ Supported | Maximum concurrent transfer threads |
| **Parallel Downloads** | ✅ Supported | ✅ Supported | ❌ Not supported | Parallel range-based downloads |
| **Target Throughput (Gbps)** | ❌ Not supported | ✅ CRT client only | ❌ Not supported | Network throughput hint |
| **Max Native Memory Limit** | ❌ Not supported | ✅ CRT client only | ❌ Not supported | Caps native memory for CRT client |

### Provider-Specific Notes

#### AWS S3
- **Parallel Uploads**: Enabled via `multipartEnabled(true)` on the S3 async client
- **Parallel Downloads**: Uses CRT-based S3 client when enabled for high-throughput range-based downloads
- **Part Buffer Size**: Maps to `minimumPartSizeInBytes` in `MultipartConfiguration`
- **Threshold Bytes**: Maps to `thresholdInBytes` — default 150MB, configurable
- **Max Concurrency**: Configured via `ExecutorService` or CRT client `maxConcurrency`
- **Target Throughput / Max Native Memory**: Available only with the CRT-based client, activated when parallel downloads are enabled

#### GCP GCS
- **Parallel Uploads**: Automatic when file size exceeds SDK threshold; uses `ParallelCompositeUpload` (default threshold: 150MB, configurable via `isAllowParallelCompositeUpload`)
- **Parallel Downloads**: Uses `AllowDivideAndConquerDownload(true)` for parallel range-based downloads (configurable via `isAllowDivideAndConquer`)
- **Part Buffer Size**: Indirect support via `setPerWorkerBufferSize` in Transfer Manager
- **Max Concurrency**: Configured via `setMaxWorkers()` on the Transfer Manager
- **Threshold Bytes**: Default 150MB in the SDK; direct configuration API under investigation

#### Alibaba OSS
- **Parallel Uploads**: Via `InitiateMultipartUpload` and `UploadPart` APIs
- **Max Concurrency**: Manual thread pool configuration, similar to AWS approach
- **Part Buffer Size**: Direct part size available in API but not as a builder config

---

## Creating a Client

```java
BucketClient bucketClient = BucketClient.builder("aws")
    .withRegion("us-west-2")
    .withBucket("my-bucket")
    .build();
```

You can also configure advanced options:

```java
URI endpoint = URI.create("https://custom-endpoint.com");
URI proxy = URI.create("https://proxy.example.com");

bucketClient = BucketClient.builder("aws")
    .withRegion("us-west-2")
    .withBucket("my-bucket")
    .withEndpoint(endpoint)
    .withProxyEndpoint(proxy)
    .build();
```

---

## Performance Tuning

Configure parallel uploads, downloads, and throughput settings when building the client:

```java
BucketClient bucketClient = BucketClient.builder("aws")
    .withRegion("us-west-2")
    .withBucket("my-bucket")
    .withParallelUploadsEnabled(true)
    .withParallelDownloadsEnabled(true)
    .withThresholdBytes(10 * 1024 * 1024L)            // 10MB multipart threshold
    .withPartBufferSize(2 * 1024 * 1024L)              // 2MB per part
    .withMaxConcurrency(50)
    .withTargetThroughputInGbps(25.0)                   // CRT client only (AWS)
    .withMaxNativeMemoryLimitInBytes(2L * 1024 * 1024 * 1024)  // 2GB (AWS CRT only)
    .build();
```

| Parameter | Description | Default Behavior |
|-----------|-------------|------------------|
| `withParallelUploadsEnabled` | Enables multipart parallel uploads for large objects | Disabled |
| `withParallelDownloadsEnabled` | Enables parallel range-based downloads | Disabled |
| `withThresholdBytes` | File size above which multipart upload kicks in | Provider default (typically 150MB) |
| `withPartBufferSize` | Size of each part in a multipart upload | Provider default |
| `withMaxConcurrency` | Maximum number of concurrent transfer threads | Provider default |
| `withTargetThroughputInGbps` | Network throughput hint for the transfer | AWS CRT client only |
| `withMaxNativeMemoryLimitInBytes` | Caps native memory usage | AWS CRT client only |

> **Note:** `withTargetThroughputInGbps` and `withMaxNativeMemoryLimitInBytes` only take effect on AWS when the CRT-based S3 client is activated (i.e., when parallel downloads are enabled). On GCP and Alibaba, these settings are ignored.

---

## Uploading Files

Supports various sources:

```java
UploadRequest request = new UploadRequest("object-key");

bucketClient.upload(request, inputStream);
bucketClient.upload(request, new File("file.txt"));
bucketClient.upload(request, path);
bucketClient.upload(request, byteArray);
```

---

## Downloading Files

```java
DownloadRequest request = new DownloadRequest("object-key");

bucketClient.download(request, outputStream);
bucketClient.download(request, new File("dest.txt"));
bucketClient.download(request, path);
bucketClient.download(request, byteArray);
```

---

## Listing Blobs

```java
ListBlobsRequest request = new ListBlobsRequest();
Iterator<BlobInfo> blobs = bucketClient.list(request);
while (blobs.hasNext()) {
    System.out.println(blobs.next().getName());
}
```

---

## Deleting Blobs

```java
bucketClient.delete("object-key", null); // optional versionId

Collection<BlobIdentifier> toDelete = List.of(
    new BlobIdentifier("object1"),
    new BlobIdentifier("object2")
);
bucketClient.delete(toDelete);
```

---

## Copying Blobs

```java
CopyRequest copyRequest = new CopyRequest();
// populate source and destination
CopyResponse response = bucketClient.copy(copyRequest);
```

---

## Metadata and Tags

```java
BlobMetadata metadata = bucketClient.getMetadata("object-key", null);
Map<String, String> tags = bucketClient.getTags("object-key");
bucketClient.setTags("object-key", Map.of("env", "prod"));
```

---

## Presigned URLs

```java
PresignedUrlRequest presignedRequest = new PresignedUrlRequest();
URL url = bucketClient.generatePresignedUrl(presignedRequest);
```

---

## Multipart Uploads

```java
MultipartUploadRequest initRequest = new MultipartUploadRequest();
MultipartUpload upload = bucketClient.initiateMultipartUpload(initRequest);

UploadPartResponse part = bucketClient.uploadMultipartPart(upload, partData);

List<UploadPartResponse> parts = List.of(part1, part2);
bucketClient.completeMultipartUpload(upload, parts);

List<UploadPartResponse> uploadedParts = bucketClient.listMultipartUpload(upload);

bucketClient.abortMultipartUpload(upload);
```

---

## Error Handling

All operations may throw `SubstrateSdkException`. These can be caught and handled generically:

```java
try {
    bucketClient.upload(request, new File("file.txt"));
} catch (SubstrateSdkException e) {
    // Handle access denied, IO failure, etc.
    e.printStackTrace();
}
```

---

Use `BucketClient` when you need full CRUD support and advanced control over blobs inside a single bucket, across any supported cloud provider.
