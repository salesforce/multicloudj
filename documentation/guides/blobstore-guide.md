---
layout: default
title: How to Blob store
nav_order: 2
parent: Usage Guides
---
# BucketClient

The `BucketClient` class in the `multicloudj` library provides a comprehensive, cloud-agnostic interface to interact with individual buckets in object storage services like AWS S3 and Google Cloud Storage.

This client enables uploading, downloading, deleting, listing, copying, and managing blob metadata and multipart uploads across multiple cloud providers.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Basic Upload** | ✅ Supported | ✅ Supported | ✅ Supported | Upload from InputStream, byte[], File, Path |
| **Basic Download** | ✅ Supported | ✅ Supported | ✅ Supported | Download to OutputStream, byte[], File, Path, or InputStream |
| **Delete Objects** | ✅ Supported | ✅ Supported | ✅ Supported | Single and batch delete operations |
| **Copy Objects** | ✅ Supported | ✅ Supported | ✅ Supported | Server-side copy within and across buckets |
| **Get Metadata** | ✅ Supported | ✅ Supported | ✅ Supported | Retrieve object metadata and properties |
| **List Objects** | ✅ Supported | ✅ Supported | ✅ Supported | Paginated listing with filters |
| **Paginated Listing** | ✅ Supported | ✅ Supported | ✅ Supported | Page-based listing with token and maxResults |
| **Object Tagging** | ✅ Supported | ✅ Supported | ✅ Supported | Get and set object tags |
| **Presigned URLs** | ✅ Supported | ✅ Supported | ✅ Supported | Generate temporary access URLs |
| **Versioning Support** | ✅ Supported | ✅ Supported | ✅ Supported | Object version-specific operations |
| **Existence Checks** | ✅ Supported | ✅ Supported | ✅ Supported | Check if object or bucket exists |

### Multipart Upload Features

| Feature Name | GCP          | AWS | ALI | Comments |
|--------------|--------------|-----|-----|----------|
| **Initiate Multipart** | ✅ Supported  | ✅ Supported | ✅ Supported | Start multipart upload session |
| **Upload Part** | ✅ Supported  | ✅ Supported | ✅ Supported | Upload individual parts |
| **Complete Multipart** | ✅ Supported  | ✅ Supported | ✅ Supported | Finalize multipart upload |
| **List Parts** | ✅ Supported  | ✅ Supported | ✅ Supported | List uploaded parts |
| **Abort Multipart** | ✅ Supported  | ✅ Supported | ✅ Supported | Cancel multipart upload |

### Advanced Features

| Feature Name           | GCP | AWS | ALI | Comments                                                |
|------------------------|-----|-----|-----|---------------------------------------------------------|
| **Async Operations**   | ✅ Supported | ✅ Supported | 📅 In Roadmap | CompletableFuture-based async API via AsyncBucketClient |
| **Bucket Operations**  | ✅ Supported | ✅ Supported | ✅ Supported | List and create buckets via BlobClient                  |
| **Directory Upload**   | ✅ Supported | ✅ Supported | ❌ Not Supported | Upload directory with all blobs under it             |
| **Directory Delete**   | ✅ Supported | ✅ Supported | ❌ Not Supported | Delete directory with all blobs under it             |
| **Directory Download** | ✅ Supported | ✅ Supported | ❌ Not Supported | Download directory with all blobs under it           |
| **Object Lock**        | ✅ Supported | ✅ Supported | ❌ Not Supported | Get object lock info (retention + legal hold)        |
| **Object Retention**   | ✅ Supported | ✅ Supported | ❌ Not Supported | Update WORM retention date on a blob                 |
| **Legal Hold**         | ✅ Supported | ✅ Supported | ❌ Not Supported | Apply or remove legal hold on a blob                 |

### Configuration Options

| Configuration | GCP               | AWS | ALI | Comments |
|---------------|-------------------|-----|-----|----------|
| **Regional Support** | ⏱️ End of June'26 | ✅ Supported | ✅ Supported | Region-specific bucket operations |
| **Endpoint Override** | ✅ Supported       | ✅ Supported | ✅ Supported | Custom endpoint configuration |
| **Proxy Support** | ✅ Supported       | ✅ Supported | ✅ Supported | HTTP proxy configuration |
| **Credentials Override** | ✅ Supported       | ✅ Supported | ✅ Supported | Custom credential providers via STS |
| **Retry Configuration** | ✅ Supported       | ✅ Supported | ✅ Supported | Custom retry policy |
| **Connection Tuning** | ✅ Supported       | ✅ Supported | ✅ Supported | Max connections, socket timeout, idle timeout |
| **Quota Project ID** | ✅ Supported       | ❌ Not Applicable | ❌ Not Applicable | GCP billing quota project |

### Provider-Specific Notes

**GCP (Google Cloud Platform)**
- Object lock maps to GCP event-based hold. The `useEventBasedHold` field in `ObjectLockInfo` is GCP-specific.
- `withQuotaProjectId()` is a GCP-only builder option for specifying a billing quota project.

**AWS**
- Object retention supports two modes: `GOVERNANCE` (bypassable with special permissions) and `COMPLIANCE` (cannot be bypassed until expiry).

---

## Creating a BucketClient

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

BucketClient bucketClient = BucketClient.builder("aws")
    .withRegion("us-west-2")
    .withBucket("my-bucket")
    .withEndpoint(endpoint)
    .withProxyEndpoint(proxy)
    .withMaxConnections(100)
    .withSocketTimeout(Duration.ofSeconds(30))
    .withIdleConnectionTimeout(Duration.ofSeconds(60))
    .withRetryConfig(retryConfig)
    .withCredentialsOverrider(credentialsOverrider)
    .build();
```

For GCP, you can additionally specify a quota project:

```java
BucketClient bucketClient = BucketClient.builder("gcp")
    .withRegion("us-central1")
    .withBucket("my-bucket")
    .withQuotaProjectId("my-billing-project")
    .build();
```

---

## BlobClient (Service-Level Operations)

`BlobClient` provides service-wide operations that are not scoped to a single bucket, such as listing or creating buckets.

```java
BlobClient blobClient = BlobClient.builder("aws")
    .withRegion("us-west-2")
    .build();

// List all buckets
ListBucketsResponse response = blobClient.listBuckets();

// Create a new bucket
blobClient.createBucket("new-bucket-name");
```

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

> **Tip:** Specifying `contentLength` in the `UploadRequest` can significantly improve upload efficiency — the SDK does not need to buffer the stream to calculate it.

---

## Downloading Files

```java
DownloadRequest request = new DownloadRequest("object-key");

bucketClient.download(request, outputStream);
bucketClient.download(request, new File("dest.txt"));
bucketClient.download(request, path);
bucketClient.download(request, byteArray);
```

To receive the content as a readable stream:

```java
DownloadResponse response = bucketClient.download(request);
InputStream content = response.getInputStream(); // caller is responsible for closing
```

---

## Listing Blobs

### Iterator-based (full scan)

```java
ListBlobsRequest request = new ListBlobsRequest();
Iterator<BlobInfo> blobs = bucketClient.list(request);
while (blobs.hasNext()) {
    System.out.println(blobs.next().getName());
}
```

### Page-based (paginated)

Use `listPage` when you need explicit pagination control — for example, to implement a paginated API response or process large buckets incrementally.

```java
ListBlobsPageRequest request = ListBlobsPageRequest.builder()
    .withPrefix("logs/")
    .withMaxResults(100)
    .build();

ListBlobsPageResponse page = bucketClient.listPage(request);
for (BlobInfo blob : page.getBlobs()) {
    System.out.println(blob.getName());
}

// Fetch the next page
if (page.isTruncated()) {
    ListBlobsPageRequest nextRequest = ListBlobsPageRequest.builder()
        .withPrefix("logs/")
        .withMaxResults(100)
        .withPaginationToken(page.getNextPageToken())
        .build();
    ListBlobsPageResponse nextPage = bucketClient.listPage(nextRequest);
}
```

The response also contains `commonPrefixes` when a `delimiter` is specified, useful for virtual directory traversal.

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

### Copy to another bucket (current bucket as source)

```java
CopyRequest copyRequest = CopyRequest.builder()
    .srcKey("source-object")
    .destBucket("destination-bucket")
    .destKey("destination-object")
    .build();
CopyResponse response = bucketClient.copy(copyRequest);
```

### Copy from another bucket (current bucket as destination)

```java
CopyFromRequest copyFromRequest = CopyFromRequest.builder()
    .srcBucket("source-bucket")
    .srcKey("source-object")
    .destKey("destination-object")
    .build();
CopyResponse response = bucketClient.copyFrom(copyFromRequest);
```

Both methods support an optional `srcVersionId` for versioned buckets.

---

## Metadata and Tags

```java
BlobMetadata metadata = bucketClient.getMetadata("object-key", null);
Map<String, String> tags = bucketClient.getTags("object-key");
bucketClient.setTags("object-key", Map.of("env", "prod"));
```

---

## Existence Checks

```java
boolean objectExists = bucketClient.doesObjectExist("object-key", null); // versionId optional
boolean bucketExists = bucketClient.doesBucketExist();
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

## Object Lock & Legal Hold

Object lock prevents blobs from being deleted or overwritten for a defined period (WORM protection). Supported on AWS and GCP only.

### Retrieve lock info

```java
ObjectLockInfo lockInfo = bucketClient.getObjectLock("object-key", null);
System.out.println("Retention mode: " + lockInfo.getMode());           // GOVERNANCE or COMPLIANCE (AWS)
System.out.println("Retain until: " + lockInfo.getRetainUntilDate());
System.out.println("Legal hold: " + lockInfo.isLegalHold());
System.out.println("Event-based hold: " + lockInfo.getUseEventBasedHold()); // GCP-specific
```

### Update retention date

```java
Instant newExpiry = Instant.now().plus(Duration.ofDays(365));
bucketClient.updateObjectRetention("object-key", null, newExpiry);
```

### Apply or remove legal hold

```java
bucketClient.updateLegalHold("object-key", null, true);  // apply
bucketClient.updateLegalHold("object-key", null, false); // remove
```

**Provider notes:**
- **AWS**: `RetentionMode` is either `GOVERNANCE` (bypassable with `s3:BypassGovernanceRetention` permission) or `COMPLIANCE` (irrevocable until expiry).
- **GCP**: Object lock maps to GCP's event-based hold mechanism. Use `getUseEventBasedHold()` for GCP-specific state.

---

## Async API

`AsyncBucketClient` provides a `CompletableFuture`-based API for non-blocking operations. Supported on AWS and GCP; ALI is in roadmap.

```java
AsyncBucketClient asyncClient = AsyncBucketClient.builder("aws")
    .withRegion("us-west-2")
    .withBucket("my-bucket")
    .build();

CompletableFuture<UploadResponse> future = asyncClient.upload(uploadRequest, inputStream);
future.thenAccept(response -> System.out.println("Uploaded: " + response.getKey()));
```

The async client mirrors the synchronous `BucketClient` API — all upload, download, delete, copy, list, and multipart operations are available as `CompletableFuture`-returning variants.

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
