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
     so the responsive layout can omit empty exception fields. -->
<div class="feature-support" markdown="1">

<a id="provider-specific-exceptions"></a>

## Feature Support Across Providers

Feature Detail describes the shared API behavior. Provider-Specific Exceptions describes
provider limitations, native mode mappings, workarounds, and options that only apply to certain
clients or transports. An empty cell means no provider-specific exception is documented for that row.
Support refers to MultiCloudJ support, which may use a workaround instead of a native API.

### Core API Features

| Feature Name | Feature Detail | GCP | AWS | ALI | Provider-Specific Exceptions |
|--------------|----------------|-----|-----|-----|-----------------------------|
| **Basic Upload** | `upload`: upload from InputStream, byte[], File, or Path | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Basic Download** | `download`: download to OutputStream, byte[], File, or Path | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Delete Objects** | `delete`: single and batch delete operations | ✅ Supported | ✅ Supported | ✅ Supported | GCP uses an HTTP/JSON fallback for collection delete when gRPC is enabled. |
| **Copy Objects** | `copy`, `copyFrom`: server-side copy within and across buckets | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Get Metadata** | `getMetadata`: retrieve object metadata and properties | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **List Objects** | `list`, `listPage`: paginated listing with filters | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Object Tagging** | `getTags`, `setTags`, and tags on upload requests | ✅ Supported | ✅ Supported | ✅ Supported | GCS has no object-tag API. MultiCloudJ stores tags as custom metadata prefixed with `gcp-tag-` and removes the prefix when reading them. AWS and Alibaba use native object tags. |
| **Presigned URLs** | `presign`, `generatePresignedUrl`: generate temporary access URLs | ✅ Supported | ✅ Supported | ✅ Supported | GCP signs through HTTP/JSON even when gRPC is enabled. GCS V4 signing cannot enforce SHA256 upload integrity, so MultiCloudJ rejects a SHA256 upload checksum value with `UnSupportedOperationException`; use CRC32C or MD5. |
| **Versioning Support** | Object version-specific operations using `versionId` | ✅ Supported | ✅ Supported | ✅ Supported | GCP maps `versionId` to the object's numeric generation. AWS and Alibaba use native version IDs. |
| **Upload Checksums** | `ChecksumMethod` on upload and multipart requests | ✅ Supported | ✅ Supported | ✅ Supported | GCP accepts CRC32C or MD5; other algorithms surface from `BucketClient` as `UnknownException` wrapping `java.lang.UnsupportedOperationException`. When a checksum value is supplied, AWS single upload accepts CRC32C, SHA256, or MD5 and rejects CRC64 with `InvalidArgumentException`. When multipart checksumming is enabled, AWS accepts only CRC32C or SHA256 and rejects other algorithms with `InvalidArgumentException`. Alibaba multipart accepts only CRC64 and rejects other explicit algorithms with `UnSupportedOperationException`. Alibaba single upload does not reject the selected algorithm: MD5 is sent as `Content-MD5`, SHA256 as `x-oss-content-sha256`, and other selections as `x-oss-hash-crc64ecma`. |
| **Provider-Managed KMS Encryption** | `withUseKmsManagedKey` on upload requests | ⚠️ Ignored | ✅ Supported | ✅ Supported | AWS and Alibaba explicitly request provider-managed KMS encryption. GCP ignores this flag and uses the bucket's configured encryption default. |

### Multipart Upload Features

| Feature Name | Feature Detail | GCP | AWS | ALI | Provider-Specific Exceptions |
|--------------|----------------|-----|-----|-----|-----------------------------|
| **Initiate Multipart** | `initiateMultipartUpload`: start a multipart upload session | ✅ Supported | ✅ Supported | ✅ Supported | GCP uses the GCS XML multipart API. Checksum algorithms follow **Upload Checksums**. |
| **Upload Part** | `uploadMultipartPart`: upload individual parts | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Complete Multipart** | `completeMultipartUpload`: finalize the upload | ✅ Supported | ✅ Supported | ✅ Supported | GCP sorts parts by part number before completion, then applies tags and legal holds in separate metadata updates after the object is committed. A failure in those updates can leave a completed object without all requested tags or holds. |
| **List Parts** | `listMultipartUpload`: list uploaded parts | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Abort Multipart** | `abortMultipartUpload`: cancel the upload | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |

### Object Lock & Retention Features

| Feature Name | Feature Detail | GCP | AWS | ALI | Provider-Specific Exceptions |
|--------------|----------------|-----|-----|-----|-----------------------------|
| **Get Object Lock** | `getObjectLock`: retrieve mode, retain-until date, and legal hold | ✅ Supported | ✅ Supported | ✅ Supported | GCP reads per-object retention and holds from object metadata and reports a legal hold if either a temporary or event-based hold is active. AWS and Alibaba read native object retention and legal-hold configurations. |
| **Update Object Retention** | `updateObjectRetention`: change an existing retention mode and/or expiration date | ✅ Supported | ✅ Supported | ⚠️ Partial | GCP patches object metadata; AWS and Alibaba use dedicated retention APIs. Alibaba rejects a GOVERNANCE → COMPLIANCE upgrade with `UnSupportedOperationException` because its mode is immutable once set. AWS/GCP allow the upgrade with bypass. See [retention rules](#rules-table). |
| **Retention Mode Support** | `RetentionMode.GOVERNANCE` and `RetentionMode.COMPLIANCE` | ✅ Supported | ✅ Supported | ⚠️ Partial | GCP maps GOVERNANCE to `Unlocked` and COMPLIANCE to `Locked`; AWS and Alibaba use GOVERNANCE/COMPLIANCE directly. GCP's [per-object retention](https://docs.cloud.google.com/storage/docs/object-lock) is separate from a bucket retention policy (Bucket Lock). Alibaba supports both modes but cannot change an object's mode once set. |
| **Bypass Governance Retention** | `ObjectRetentionConfig.bypassGovernanceRetention`: explicitly allow shortening GOVERNANCE retention | ✅ Supported | ✅ Supported | ✅ Supported | AWS/Alibaba use `bypassGovernanceRetention`; GCP uses `overrideUnlockedRetention` over HTTP/JSON, including when gRPC is enabled. Provider permissions are still required. Bypass cannot shorten COMPLIANCE retention. |
| **Update Legal Hold** | `updateLegalHold`: enable or disable a hold | ✅ Supported | ✅ Supported | ✅ Supported | GCP updates an active event-based hold, otherwise a temporary hold. On upload, `useEventBasedHold` selects the type (default: temporary); AWS/Alibaba use native legal holds. GCP [event-based holds](https://docs.cloud.google.com/storage/docs/object-holds) cannot coexist with per-object retention; releasing one restarts any bucket retention period. Temporary holds can coexist with per-object retention. |
| **Object Lock on Upload** | `ObjectLockConfiguration`: set retention and/or a legal hold at upload time | ✅ Supported | ✅ Supported | ⚠️ Partial | [AWS Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html) requires an Object Lock-enabled, versioned bucket. GCP requires Object Retention Lock enabled on the bucket for per-object retention and maps modes as above; versioning is not required. Alibaba `BucketClient` applies retention and legal hold with separate `putObjectRetention` and `putObjectLegalHold` calls after upload or multipart completion; a failure leaves the object written without all requested lock settings. Alibaba `AsyncBucketClient` ignores `ObjectLockConfiguration` on single uploads, multipart completion, and directory uploads. For GCP multipart legal holds, see **Complete Multipart**; for directory uploads, see **Directory Transfers**. |

### Advanced Features

| Feature Name | Feature Detail | GCP | AWS | ALI | Provider-Specific Exceptions |
|--------------|----------------|-----|-----|-----|-----------------------------|
| **Async Operations** | CompletableFuture-based API via `AsyncBucketClient` | ✅ Supported | ✅ Supported | ✅ Supported | GCP runs core synchronous Blob operations on the configured executor; directory transfers use the GCS Transfer Manager. |
| **Bucket Operations** | `BlobClient.listBuckets`: list buckets | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Directory Transfers** | `uploadDirectory`, `downloadDirectory`: transfer files under a directory or prefix | ✅ Supported | ✅ Supported | ✅ Supported | Async only: available on `AsyncBucketClient`, not `BucketClient`. AWS and Alibaba attach per-file logging listeners when `transferStatusLoggingEnabled` is set; GCP ignores that flag. GCP parallel composite uploads with object retention or holds can leave protected component objects behind and report a failed file even after creating the final object; disable parallel uploads for these directory transfers. |
| **Lifecycle Expiration Tag** | Reserved `expiration-days` tag marks an object for deletion by an out-of-band bucket lifecycle rule ([details](#lifecycle-expiration-via-tagging)) | ✅ Supported | ✅ Supported | ✅ Supported | GCS lifecycle rules cannot filter by tag, so MultiCloudJ sets object custom time and the rule uses `daysSinceCustomTime`. AWS and Alibaba use native tag filters with a rule for each supported day-count value. |

### Configuration Options

| Configuration | Feature Detail | GCP | AWS | ALI | Provider-Specific Exceptions |
|---------------|----------------|-----|-----|-----|-----------------------------|
| **Endpoint Override** | `withEndpoint`: configure a custom endpoint | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Proxy Support** | `withProxyEndpoint`: configure an HTTP proxy | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Automatic Proxy Discovery** | `withUseSystemPropertyProxyValues`, `withUseEnvironmentVariableProxyValues` | ⚠️ Ignored | ✅ Supported | ⚠️ Ignored | These flags control AWS SDK proxy discovery. GCP and Alibaba ignore them; use explicit proxy configuration. |
| **Credentials Override** | Custom credential providers via STS | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Region** | `withRegion`: configure the service region | ⚠️ Ignored | ✅ Supported | ✅ Supported | AWS and Alibaba use the region. GCS is a global service and does not consume this option. |
| **gRPC Transport** | `withGrpcEnabled`: select gRPC | ✅ Supported | ⚠️ Ignored | ⚠️ Ignored | Selects GCS gRPC. Signed URLs, collection delete, and retention updates requiring `overrideUnlockedRetention` use an HTTP/JSON fallback. |
| **Quota Project** | `withQuotaProjectId`: configure billing and quota attribution | ✅ Supported | ⚠️ Ignored | ⚠️ Ignored | Sets the GCP billing and quota project. Available only on `BucketClient.BlobBuilder`; `AsyncBucketClient.Builder` has no `withQuotaProjectId`. AWS and Alibaba ignore this option. |
| **Retry Timeouts** | `RetryConfig.totalTimeout`, `RetryConfig.attemptTimeout` | ⚠️ Transport-dependent | ✅ Supported | ⚠️ Partial | Alibaba OSS v2 has no overall deadline, so `totalTimeout` is ignored and `attemptTimeout` maps to read/write timeouts. GCP applies `attemptTimeout` only to gRPC RPCs; HTTP/JSON ignores it. |

### Performance Configuration

Builder-option scope differs between `BucketClient` and `AsyncBucketClient`; the exceptions below
call out async-only behavior. A supported transfer does not imply every tuning option is consumed.

| Configuration | Feature Detail | GCP | AWS | ALI | Provider-Specific Exceptions |
|---------------|----------------|-----|-----|-----|-----------------------------|
| **Parallel Uploads** | `withParallelUploadsEnabled`: enable parallel uploads for large objects | ✅ Supported | ✅ Supported | ⚠️ Ignored | GCP maps this to GCS Transfer Manager parallel composite uploads, which apply only to `uploadDirectory`; single uploads use `storage.createFrom`. AWS passes this flag to `multipartEnabled` on the standard async client. The AWS CRT client selected by `withParallelDownloadsEnabled(true)` ignores it. Alibaba ignores this async builder option; use explicit multipart APIs. |
| **Part Buffer Size** | `withPartBufferSize`: configure part size or transfer buffers | ⚠️ Indirect | ✅ Supported | ⚠️ Ignored | GCP maps this to the GCS Transfer Manager per-worker buffer size. Alibaba ignores the async builder option; callers choose part sizes through multipart APIs. |
| **Threshold Bytes** | `withThresholdBytes`: configure the multipart threshold | ⚠️ Ignored | ✅ Supported | ⚠️ Ignored | `AsyncBucketClient.Builder.withThresholdBytes` configures AWS multipart uploads. GCP/Alibaba ignore it; GCP transfer buffering is configured separately through `withPartBufferSize`. |
| **Max Concurrency** | `withMaxConcurrency`: configure concurrent transfers | ⚠️ Ignored | ✅ CRT client only | ⚠️ Ignored | On `AsyncBucketClient.Builder`, this applies only to the AWS CRT client, which MultiCloudJ builds when `withParallelDownloadsEnabled(true)`; the standard AWS client ignores it. GCP and Alibaba ignore it. GCP transfer workers can be configured with `withTransferManagerThreadPoolSize`. |
| **Max Connections** | `withMaxConnections`: configure the HTTP connection pool | ✅ Supported | ✅ Supported | ✅ Supported | <!-- --> |
| **Parallel Downloads** | `withParallelDownloadsEnabled` and request-level `parallelDownload` | ✅ Supported | ✅ Supported | ⚠️ Async client only | GCP uses GCS Transfer Manager only when `DownloadRequest.parallelDownload` is set, the destination is a File or Path, and no range is requested; `withParallelDownloadsEnabled` (default true on GCP) maps to the Transfer Manager's `setAllowDivideAndConquerDownload`. Other GCP downloads use streaming reads. Alibaba ignores the async builder flag; set `DownloadRequest.parallelDownload` for whole-object File/Path downloads. Ranged downloads use the regular path. |
| **Target Throughput (Gbps)** | `withTargetThroughputInGbps`: provide a throughput hint | ⚠️ Ignored | ✅ CRT client only | ⚠️ Ignored | AWS CRT only; ignored by GCP/Alibaba. |
| **Max Native Memory Limit** | `withMaxNativeMemoryLimitInBytes`: cap native transfer memory | ⚠️ Ignored | ✅ CRT client only | ⚠️ Ignored | AWS CRT only; ignored by GCP/Alibaba. |
| **Initial Read Buffer Size** | `AsyncBucketClient.Builder.withInitialReadBufferSizeInBytes` | ⚠️ Ignored | ✅ CRT client only | ⚠️ Ignored | Configures AWS CRT read buffering; ignored by GCP/Alibaba. |
| **Transfer Manager Thread Pool Size** | `AsyncBucketClient.Builder.withTransferManagerThreadPoolSize` | ✅ Supported | ✅ Supported | ⚠️ Ignored | GCP maps this to GCS Transfer Manager workers; AWS configures its transfer executor. Alibaba ignores this builder option. |
| **Directory Transfer Concurrency** | `AsyncBucketClient.Builder.withTransferDirectoryMaxConcurrency` | ⚠️ Ignored | ✅ Supported | ⚠️ Ignored | Configures AWS S3 Transfer Manager directory concurrency; ignored by GCP/Alibaba. |

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
