---
layout: default
title: How to Blob store
nav_order: 2
parent: Usage Guides
feature_support_layout: true
---
# BucketClient

The `BucketClient` class in the `multicloudj` library provides a comprehensive, cloud-agnostic interface to interact with individual buckets in object storage services like AWS S3, Google Cloud Storage, and Alibaba Cloud OSS.

This client enables uploading, downloading, deleting, listing, copying, and managing blob metadata and multipart uploads across multiple cloud providers.

---

<!-- Empty cell comments prevent Kramdown from inserting a nonbreaking space,
     so the responsive layout can omit empty note fields. -->
<div class="feature-support" markdown="1">

<a id="provider-specific-exceptions"></a>
<a id="provider-specific-notes"></a>

## Feature Support Across Providers

MultiCloudJ provides a shared API across providers. Feature Detail describes that API;
Provider-Specific Notes explains the workarounds that preserve shared behavior and any
critical limitations or configuration requirements. Supported indicates an SDK capability,
including native implementations and workarounds; the notes qualify client and option scope.
An empty cell means no additional provider-specific guidance is needed for that feature.

### Core API Features

| Feature Name | Feature Detail | GCP | AWS | ALI | Provider-Specific Notes |
|--------------|----------------|-----|-----|-----|-----------------------------|
| **Basic Upload** | `upload`: upload from InputStream, byte[], File, or Path | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Basic Download** | `download`: download to OutputStream, byte[], File, or Path | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Delete Objects** | `delete`: single and batch delete operations | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Copy Objects** | `copy`, `copyFrom`: server-side copy within and across buckets | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Get Metadata** | `getMetadata`: retrieve object metadata and properties | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **List Objects** | `list`, `listPage`: paginated listing with filters | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Object Tagging** | `getTags`, `setTags`, and tags on upload requests | ✅ Supported | ✅ Supported | ✅ Supported | GCP implements object tags through custom metadata with the `gcp-tag-` prefix. The SDK adds the prefix when writing tags and removes it when reading them, preserving the same `getTags`/`setTags` API. AWS and Alibaba use native object tags. |
| **Presigned URLs** | `presign`, `generatePresignedUrl`: generate temporary access URLs | ✅ Supported | ✅ Supported | ✅ Supported | For GCP tagged uploads, use `presign` and send its returned signed headers: `x-goog-meta-gcp-tag-*` and, for `expiration-days`, `x-goog-custom-time` anchored at signing time. Tag keys must be valid HTTP header names and unique ignoring case; values must be visible ASCII without surrounding whitespace. With bare service-account access tokens, the SDK resolves the account through tokeninfo and signs through IAM `signBlob`: use an undownscoped token with `cloud-platform` and `userinfo.email` scopes and grant `iam.serviceAccounts.signBlob` on the account. For GCP upload integrity, use CRC32C or MD5; SHA256 is rejected. |
| **Versioning Support** | Object version-specific operations using `versionId` | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Upload Checksums** | `ChecksumMethod` on upload and multipart requests | ✅ Supported | ✅ Supported | ✅ Supported | GCP accepts CRC32C or MD5. AWS single uploads accept CRC32C, SHA256, or MD5 (sent as `Content-MD5`); multipart checksumming accepts CRC32C or SHA256. Alibaba multipart accepts CRC64. For Alibaba single uploads, use MD5 (`Content-MD5`) for integrity validation; forwarding other checksum headers does not provide equivalent validation. |
| **Provider-Managed KMS Encryption** | `withUseKmsManagedKey` on upload requests | ⚠️ Ignored | ✅ Supported | ✅ Supported | GCP ignores this flag and uses the bucket's encryption default; configure the bucket accordingly. |

### Multipart Upload Features

| Feature Name | Feature Detail | GCP | AWS | ALI | Provider-Specific Notes |
|--------------|----------------|-----|-----|-----|-----------------------------|
| **Initiate Multipart** | `initiateMultipartUpload`: start a multipart upload session | ✅ Supported | ✅ Supported | ✅ Supported | GCP uses its XML multipart API over HTTP, including when gRPC is enabled. |
| **Upload Part** | `uploadMultipartPart`: upload individual parts | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Complete Multipart** | `completeMultipartUpload`: finalize the upload | ✅ Supported | ✅ Supported | ✅ Supported | GCP sorts parts for its XML API and applies tags and legal holds through metadata updates after the object is committed. If a follow-up update fails, the object remains written; verify its final tags and hold state when completion reports a failure. |
| **List Parts** | `listMultipartUpload`: list uploaded parts | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Abort Multipart** | `abortMultipartUpload`: cancel the upload | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |

### Object Lock & Retention Features

| Feature Name | Feature Detail | GCP | AWS | ALI | Provider-Specific Notes |
|--------------|----------------|-----|-----|-----|-----------------------------|
| **Get Object Lock** | `getObjectLock`: retrieve mode, retain-until date, and legal hold | ✅ Supported | ✅ Supported | ✅ Supported | GCP derives the SDK legal-hold status from temporary and event-based object holds; either active hold is reported as a legal hold. |
| **Update Object Retention** | `updateObjectRetention`: change an existing retention mode and/or expiration date | ✅ Supported | ✅ Supported | ✅ Supported | GCP updates retention through object metadata. Alibaba cannot upgrade an existing object from GOVERNANCE to COMPLIANCE; choose the required mode at upload. AWS and GCP allow the upgrade with governance bypass. See [retention rules](#rules-table). |
| **Retention Mode Support** | `RetentionMode.GOVERNANCE` and `RetentionMode.COMPLIANCE` | ✅ Supported | ✅ Supported | ⚠️ Partial | GCP maps GOVERNANCE to `Unlocked` and COMPLIANCE to `Locked`. These are per-object retention settings, separate from a bucket retention policy. For Alibaba mode-change limitations, see **Update Object Retention**. |
| **Bypass Governance Retention** | `ObjectRetentionConfig.bypassGovernanceRetention`: explicitly allow shortening GOVERNANCE retention | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Update Legal Hold** | `updateLegalHold`: enable or disable a hold | ✅ Supported | ✅ Supported | ✅ Supported | GCP maps legal holds to temporary object holds by default, or updates an existing event-based hold. On upload, `useEventBasedHold` selects the event-based form. Event-based holds cannot coexist with per-object retention; releasing one restarts any bucket retention period. |
| **Object Lock on Upload** | `ObjectLockConfiguration`: set retention and/or a legal hold at upload time | ✅ Supported | ✅ Supported | ✅ Supported | AWS requires Object Lock and versioning enabled on the bucket; GCP requires Object Retention Lock enabled (versioning is optional). Alibaba synchronous uploads apply locks after writing, so a lock-update failure can leave an incompletely protected object. Alibaba async single, multipart, and directory uploads ignore `ObjectLockConfiguration`. |

### Advanced Features

| Feature Name | Feature Detail | GCP | AWS | ALI | Provider-Specific Notes |
|--------------|----------------|-----|-----|-----|-----------------------------|
| **Async Operations** | CompletableFuture-based API via `AsyncBucketClient` | ✅ Supported | ✅ Supported | ✅ Supported | GCP adapts core synchronous operations to `CompletableFuture` through an executor. AWS and Alibaba offload synchronous presigning to an executor; Alibaba also offloads its synchronous parallel downloader. These adapters preserve the public async API. |
| **Bucket Operations** | `BlobClient.listBuckets`: list buckets | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Directory Transfers** | `AsyncBucketClient.uploadDirectory`, `downloadDirectory`: transfer files under a directory or prefix | ✅ Supported | ✅ Supported | ✅ Supported | For GCP directory uploads with retention or holds, disable parallel composite uploads. Protected component objects can otherwise remain behind and the transfer can report failure even after creating the final object. |
| **Lifecycle Expiration Tag** | Reserved `expiration-days` tag marks an object for deletion by an out-of-band bucket lifecycle rule ([details](#lifecycle-expiration-via-tagging)) | ✅ Supported | ✅ Supported | ✅ Supported | GCP emulates tag-based lifecycle expiration by setting object custom time from `expiration-days`; bucket rules use `daysSinceCustomTime`. AWS and Alibaba use native tag filters. The GCP custom-time marker only moves later and is not cleared by removing the tag. See [lifecycle setup](#lifecycle-expiration-via-tagging). |

### Configuration Options

| Configuration | Feature Detail | GCP | AWS | ALI | Provider-Specific Notes |
|---------------|----------------|-----|-----|-----|-----------------------------|
| **Endpoint Override** | `withEndpoint`: configure a custom endpoint | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Proxy Support** | `withProxyEndpoint`: configure an HTTP proxy | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Automatic Proxy Discovery** | `withUseSystemPropertyProxyValues`, `withUseEnvironmentVariableProxyValues` | ⚠️ Ignored | ✅ Supported | ⚠️ Ignored | <!-- --> |
| **Credentials Override** | Custom credential providers via STS | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Region** | `withRegion`: configure the service region | ⚠️ Ignored | ✅ Supported | ✅ Supported | <!-- --> |
| **gRPC Transport** | `withGrpcEnabled`: select gRPC | ✅ Supported | ⚠️ Ignored | ⚠️ Ignored | GCP automatically uses HTTP/JSON for collection delete, signed URLs, and retention updates requiring governance bypass. Multipart uploads use XML/HTTP. These operations remain available when gRPC is enabled. |
| **Quota Project** | `BucketClient.BlobBuilder.withQuotaProjectId`: configure billing and quota attribution | ✅ Supported | ⚠️ Ignored | ⚠️ Ignored | <!-- --> |
| **Retry Timeouts** | `RetryConfig.totalTimeout`, `RetryConfig.attemptTimeout` | ⚠️ Transport-dependent | ✅ Supported | ⚠️ Partial | Alibaba ignores `totalTimeout`; `attemptTimeout` controls read/write timeouts. GCP applies `attemptTimeout` only to gRPC requests; HTTP/JSON ignores it. |

### Performance Configuration

Performance settings can apply to specific clients or transfer types; the support columns
and critical notes below identify their scope.

| Configuration | Feature Detail | GCP | AWS | ALI | Provider-Specific Notes |
|---------------|----------------|-----|-----|-----|-----------------------------|
| **Parallel Uploads** | Parallel uploads for large objects; `withParallelUploadsEnabled` where applicable | ✅ Supported | ✅ Supported | ✅ Supported | On GCP, this flag affects directory uploads only. On AWS, it affects the standard async client; the CRT client selected by `withParallelDownloadsEnabled(true)` ignores it. Alibaba ignores the flag; use explicit multipart APIs. |
| **Part Buffer Size** | `withPartBufferSize`: configure part size or transfer buffers | ⚠️ Indirect | ✅ Supported | ⚠️ Ignored | <!-- --> |
| **Threshold Bytes** | `withThresholdBytes`: configure the multipart threshold | ⚠️ Ignored | ✅ Supported | ⚠️ Ignored | <!-- --> |
| **Max Concurrency** | Concurrent transfers; `withMaxConcurrency` where applicable | ✅ Supported | ✅ Supported | ✅ Supported | On AWS, `withMaxConcurrency` applies to the CRT client selected by `withParallelDownloadsEnabled(true)`; the standard client ignores it. GCP and Alibaba ignore this builder setting. Configure GCP transfer workers with `withTransferManagerThreadPoolSize`. |
| **Max Connections** | `withMaxConnections`: configure the HTTP connection pool | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Parallel Downloads** | `withParallelDownloadsEnabled` and request-level `parallelDownload` | ✅ Supported | ✅ Supported | ✅ Supported | For GCP and Alibaba, request parallel download explicitly for a whole-object File/Path destination. Ranged and stream downloads use the regular path. Alibaba supports parallel download only through `AsyncBucketClient` and ignores the builder flag. |
| **Target Throughput (Gbps)** | `withTargetThroughputInGbps`: provide a throughput hint | ⚠️ Ignored | ✅ CRT client only | ⚠️ Ignored | <!-- --> |
| **Max Native Memory Limit** | `withMaxNativeMemoryLimitInBytes`: cap native transfer memory | ⚠️ Ignored | ✅ CRT client only | ⚠️ Ignored | <!-- --> |
| **Initial Read Buffer Size** | `AsyncBucketClient.Builder.withInitialReadBufferSizeInBytes` | ⚠️ Ignored | ✅ CRT client only | ⚠️ Ignored | <!-- --> |
| **Transfer Manager Thread Pool Size** | `AsyncBucketClient.Builder.withTransferManagerThreadPoolSize` | ✅ Supported | ✅ Supported | ⚠️ Ignored | <!-- --> |
| **Directory Transfer Concurrency** | `AsyncBucketClient.Builder.withTransferDirectoryMaxConcurrency` | ⚠️ Ignored | ✅ Supported | ⚠️ Ignored | <!-- --> |

</div>

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
| `withMaxConnections` | Maximum number of connections the client keeps open in its pool | Provider default |
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

## Lifecycle Expiration via Tagging

`expiration-days` is a **reserved tag key**. Setting it with a positive integer value marks the object as eligible for lifecycle-based expiration, where the value is the number of days from the object's creation time after which it should expire:

```java
// Mark the object to expire 30 days after its creation time
bucketClient.setTags("blob-key", Map.of("expiration-days", "30"));
```

The SDK only **classifies** the object as eligible; it does not delete it. The actual deletion is performed by a **bucket lifecycle rule that you configure out-of-band** (for example, with Terraform) on the bucket. The two pieces work together: the reserved tag marks each object, and the bucket rule expires objects that carry the marker.

**Forward-only:** once an object has been marked, the expiration can only be moved to a **later** time. Lowering the value or removing the tag does not retract an expiration that was already scheduled; only extending it to a later date takes effect. A non-positive or non-integer value (for example a "never expire" sentinel) leaves any existing marker untouched.

The lifecycle rule differs by provider — see below. All examples use Terraform (HCL).

### AWS S3

The S3 lifecycle filter matches an exact tag key/value and the expiration interval is a fixed number of days, so you configure **one rule per supported day-count value**:

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "expiration" {
  bucket = aws_s3_bucket.example.id

  rule {
    id     = "expire-after-30-days"
    status = "Enabled"

    filter {
      tag {
        key   = "expiration-days"
        value = "30"
      }
    }

    expiration {
      days = 30
    }
  }
}
```

### GCP GCS

Cloud Storage lifecycle rules cannot match on object tags, so the SDK stamps the object's **custom time** (`customTime = creation time + expiration-days`). A **single rule** keyed on `days_since_custom_time` then covers every day-count value:

```hcl
resource "google_storage_bucket" "expiration" {
  name = "example-bucket"

  lifecycle_rule {
    condition {
      days_since_custom_time = 0
    }
    action {
      type = "Delete"
    }
  }
}
```

### Alibaba OSS

OSS matches an exact tag key/value and the expiration interval is a fixed number of days, so you configure **one rule per supported day-count value**:

```hcl
resource "alicloud_oss_bucket_lifecycle" "expiration" {
  bucket = alicloud_oss_bucket.example.bucket

  rule {
    id     = "expire-after-30-days"
    status = "Enabled"

    filter {
      tag {
        key   = "expiration-days"
        value = "30"
      }
    }

    expiration {
      days = 30
    }
  }
}
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

The SDK enforces the following rules. The GOVERNANCE → COMPLIANCE upgrade is supported on
AWS and GCP; Alibaba rejects it as described in the feature support table above:

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
