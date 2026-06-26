---
layout: default
title: How to Blob store
nav_order: 2
parent: Usage Guides
---
# BucketClient

The `BucketClient` class in the `multicloudj` library provides a comprehensive, cloud-agnostic interface to interact with individual buckets in object storage services like AWS S3, Google Cloud Storage, and Alibaba Cloud OSS.

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
| **Object Tagging** | ✅ Supported | ✅ Supported | ✅ Supported | Get and set object tags |
| **Presigned URLs** | ✅ Supported | ✅ Supported | ✅ Supported | Generate temporary access URLs |
| **Versioning Support** | ✅ Supported | ✅ Supported | ✅ Supported | Object version-specific operations |

### Multipart Upload Features

| Feature Name | GCP               | AWS | ALI | Comments |
|--------------|-------------------|-----|-----|----------|
| **Initiate Multipart** | ✅ Supported | ✅ Supported | ✅ Supported | Start multipart upload session |
| **Upload Part** | ✅ Supported | ✅ Supported | ✅ Supported | Upload individual parts |
| **Complete Multipart** | ✅ Supported | ✅ Supported | ✅ Supported | Finalize multipart upload |
| **List Parts** | ✅ Supported | ✅ Supported | ✅ Supported | List uploaded parts |
| **Abort Multipart** | ✅ Supported | ✅ Supported | ✅ Supported | Cancel multipart upload |

### Object Lock & Retention Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Get Object Lock** | ✅ Supported | ✅ Supported | ✅ Supported | Retrieve lock config (mode, retain-until, legal hold) |
| **Update Object Retention** | ✅ Supported | ✅ Supported | ✅ Supported | Change retention mode and/or expiration date |
| **Retention Mode Support** | ✅ Supported | ✅ Supported | ✅ Supported | GOVERNANCE and COMPLIANCE modes with uniform rules |
| **Bypass Governance Retention** | ✅ Supported | ✅ Supported | ✅ Supported | Shorten or remove GOVERNANCE-mode retention |
| **Update Legal Hold** | ✅ Supported | ✅ Supported | ✅ Supported | Enable/disable legal hold on objects |
| **Object Lock on Upload** | ✅ Supported | ✅ Supported | ✅ Supported | Set retention at upload time via ObjectLockConfiguration |

### Advanced Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Async Operations** | ✅ Supported | ✅ Supported | ✅ Supported | CompletableFuture-based async API via AsyncBucketClient |
| **Bucket Operations** | ✅ Supported | ✅ Supported | ✅ Supported | List buckets via BlobClient |

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Endpoint Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom endpoint configuration |
| **Proxy Support** | ✅ Supported | ✅ Supported | ✅ Supported | HTTP proxy configuration |
| **Credentials Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom credential providers via STS |

### Performance Configuration

| Configuration | GCP                                  | AWS | ALI | Comments |
|---------------|--------------------------------------|-----|-----|----------|
| **Parallel Uploads** | ✅ Supported                          | ✅ Supported | ✅ Supported | Multipart parallel uploads for large objects |
| **Part Buffer Size** | ⚠️ Indirect                          | ✅ Supported | ⚠️ API-level only | Size of each part in multipart upload |
| **Threshold Bytes** | ⚠️ Indirect through part buffer size | ✅ Supported | ❌ Not supported | File size threshold to trigger multipart upload |
| **Max Concurrency** | ✅ Supported                          | ✅ Supported | ✅ Supported | Maximum concurrent transfer threads |
| **Parallel Downloads** | ✅ Supported                          | ✅ Supported | ❌ Not supported | Parallel range-based downloads |
| **Target Throughput (Gbps)** | ❌ Not supported                      | ✅ CRT client only | ❌ Not supported | Network throughput hint |
| **Max Native Memory Limit** | ❌ Not supported                      | ✅ CRT client only | ❌ Not supported | Caps native memory for CRT client |

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
- **Threshold Bytes**: 4*`setPerWorkerBufferSize`, defaults to 64MiB

#### Alibaba OSS
- **Parallel Uploads**: Via `InitiateMultipartUpload` and `UploadPart` APIs
- **Max Concurrency**: Manual thread pool configuration via a thread pool
- **Part Buffer Size**: Direct part size available in API but not as a builder config

---

### Provider IDs

| Provider | Provider ID |
|----------|-------------|
| AWS S3 | `aws` |
| GCP (Google Cloud Storage) | `gcp` |
| Alibaba Cloud OSS | `ali` |
| In-memory (testing) | `memory` |

> The `memory` provider is an in-memory `BucketClient` implementation intended for unit testing and local development. It needs no cloud credentials and stores objects in process memory, so it is not durable.

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

Iterate every matching object lazily:

```java
ListBlobsRequest request = ListBlobsRequest.builder()
    .withPrefix("logs/")
    .build();
Iterator<BlobInfo> blobs = bucketClient.list(request);
while (blobs.hasNext()) {
    System.out.println(blobs.next().getName());
}
```

### Listing with a Delimiter (Common Prefixes)

Supply a delimiter to group keys into "folders". The returned `ListBlobsPageResponse`
exposes both the matching blobs and the common prefixes:

```java
ListBlobsPageRequest pageRequest = ListBlobsPageRequest.builder()
    .withPrefix("logs/")
    .withDelimiter("/")
    .build();

ListBlobsPageResponse page = bucketClient.listPage(pageRequest);
page.getBlobs().forEach(b -> System.out.println("Object: " + b.getName()));
page.getCommonPrefixes().forEach(p -> System.out.println("Prefix: " + p));

// Fetch the next page when truncated
if (page.isTruncated()) {
    ListBlobsPageRequest next = ListBlobsPageRequest.builder()
        .withPrefix("logs/")
        .withDelimiter("/")
        .withPaginationToken(page.getNextPageToken())
        .build();
    ListBlobsPageResponse nextPage = bucketClient.listPage(next);
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

## Object Retention

Update per-object retention with explicit mode and bypass control using `ObjectRetentionConfig`:

```java
import com.salesforce.multicloudj.blob.driver.ObjectRetentionConfig;
import com.salesforce.multicloudj.blob.driver.RetentionMode;

// Extend retention on a GOVERNANCE-mode object
bucketClient.updateObjectRetention("object-key", null,
    ObjectRetentionConfig.builder()
        .mode(RetentionMode.GOVERNANCE)
        .retainUntilDate(Instant.now().plus(Duration.ofDays(90)))
        .build());

// Extend retention on a COMPLIANCE-mode object
bucketClient.updateObjectRetention("object-key", null,
    ObjectRetentionConfig.builder()
        .mode(RetentionMode.COMPLIANCE)
        .retainUntilDate(Instant.now().plus(Duration.ofDays(365)))
        .build());

// Shorten GOVERNANCE retention (requires bypass flag)
bucketClient.updateObjectRetention("object-key", null,
    ObjectRetentionConfig.builder()
        .mode(RetentionMode.GOVERNANCE)
        .retainUntilDate(Instant.now().plus(Duration.ofDays(7)))
        .bypassGovernanceRetention(true)
        .build());

// Upgrade from GOVERNANCE to COMPLIANCE (requires bypass flag)
bucketClient.updateObjectRetention("object-key", null,
    ObjectRetentionConfig.builder()
        .mode(RetentionMode.COMPLIANCE)
        .retainUntilDate(Instant.now().plus(Duration.ofDays(365)))
        .bypassGovernanceRetention(true)
        .build());

// Preserve current mode (pass null for mode)
bucketClient.updateObjectRetention("object-key", null,
    ObjectRetentionConfig.builder()
        .retainUntilDate(Instant.now().plus(Duration.ofDays(60)))
        .build());
```

### Retention Modes

| Mode | Description |
|------|-------------|
| `GOVERNANCE` | Retention can be shortened or removed by users with bypass permissions |
| `COMPLIANCE` | Retention cannot be shortened or removed by anyone until it expires |

### Rules Table

The SDK enforces uniform rules across all providers:

| Current Mode | Action | Bypass Flag | Outcome |
|-------------|--------|-------------|---------|
| GOVERNANCE | Extend retention | Not required | ✅ Succeeds |
| GOVERNANCE | Shorten retention | `true` | ✅ Succeeds |
| GOVERNANCE | Shorten retention | `false` | ❌ `FailedPreconditionException` |
| GOVERNANCE | Upgrade to COMPLIANCE | `true` | ✅ Succeeds |
| GOVERNANCE | Upgrade to COMPLIANCE | `false` | ❌ `FailedPreconditionException` |
| COMPLIANCE | Extend retention | Not required | ✅ Succeeds |
| COMPLIANCE | Shorten retention | Any | ❌ `FailedPreconditionException` |
| COMPLIANCE | Downgrade to GOVERNANCE | Any | ❌ `FailedPreconditionException` |
| _(none)_ | Any update | Any | ❌ `FailedPreconditionException` |

> **Note:** The `bypassGovernanceRetention` flag maps to `bypassGovernanceRetention` on AWS S3 and `overrideUnlockedRetention` on GCP GCS. It has no effect on COMPLIANCE-mode objects.

> **Alibaba OSS limitation:** OSS treats an object's retention mode as immutable once set, so a GOVERNANCE → COMPLIANCE upgrade (allowed on AWS/GCP with the bypass flag) is rejected on OSS with `UnSupportedOperationException`.

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
