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

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Basic Upload** | ✅ Supported | ✅ Supported | ✅ Supported | Upload from InputStream, byte[], File, Path |
| **Basic Download** | ✅ Supported | ✅ Supported | ✅ Supported | Download to OutputStream, byte[], File, Path |
| **Delete Objects** | ✅ Supported | ✅ Supported | ✅ Supported | Single and batch delete operations |
| **Copy Objects** | ✅ Supported | ✅ Supported | ✅ Supported | Server-side copy within and across buckets |
| **Get Metadata** | ✅ Supported | ✅ Supported | ✅ Supported | Retrieve object metadata and properties |
| **List Objects** | ✅ Supported | ✅ Supported | ✅ Supported | Paginated listing with filters |
| **Object Tagging** | ✅ Supported | ✅ Supported | ✅ Supported | Get and set object tags |
| **Presigned URLs** | ✅ Supported | ✅ Supported | ✅ Supported | Generate temporary access URLs |
| **Versioning Support** | ✅ Supported | ✅ Supported | ✅ Supported | Object version-specific operations |

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
| **Async Operations**   | ✅ Supported | ✅ Supported | 📅 In Roadmap | CompletableFuture-based async API available only for AWS |
| **Bucket Operations**  | ✅ Supported | ✅ Supported | ✅ Supported | List buckets via BlobClient                             |
| **Directory Upload**   | ✅ Supported | ✅ Supported  |     | Upload directory with all blobs under it                |
| **Directory Delete**   |  ✅ Supported  |  ✅ Supported   |     | Delete directory with all blobs under it                                     |
| **Directory Download** |  ✅ Supported  |  ✅ Supported   |     | Download directory with all blobs under it                               |

### Configuration Options

| Configuration | GCP               | AWS | ALI | Comments |
|---------------|-------------------|-----|-----|----------|
| **Regional Support** | ⏱️ End of June'26 | ✅ Supported | ✅ Supported | Region-specific bucket operations |
| **Endpoint Override** | ✅ Supported       | ✅ Supported | ✅ Supported | Custom endpoint configuration |
| **Proxy Support** | ✅ Supported       | ✅ Supported | ✅ Supported | HTTP proxy configuration |
| **Credentials Override** | ✅ Supported       | ✅ Supported | ✅ Supported | Custom credential providers via STS |

### Provider-Specific Notes


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

## Observability: Correlation, Tenant, and Service IDs

MulticloudJ blob operations support attaching observability identifiers to each request via
`OperationContext`. When supplied, these identifiers are stamped onto three surfaces: the stored
object's metadata, the SLF4J MDC (making them available to your logging framework), and the
OpenTelemetry span as attributes.

### Stamping Correlation, Tenant, and Service IDs

```java
OperationContext context = OperationContext.builder()
    .correlationId("request-xyz-456")   // ties logs/traces to this request
    .tenantId("tenant-1234")            // identifies the tenant
    .serviceId("my-service")            // identifies the calling service
    .build();

UploadRequest request = new UploadRequest.Builder()
    .withKey("object-key")
    .withOperationContext(context)
    .build();

bucketClient.upload(request, inputStream);
```

After upload, the stored object's metadata will include:

- `sdk-logging-correlation-id=request-xyz-456` (or your custom key; see below)
- `sdk-logging-tenant-id=tenant-1234`
- `sdk-logging-service-id=my-service`

The same identifiers appear in your application's logs (via SLF4J MDC keys) and in the
OpenTelemetry trace span for the operation.

### Customizing the Correlation ID Key

By default, the correlation id is stamped under the key `sdk-logging-correlation-id` in object
metadata, and under `correlation_id` in MDC and the span attribute. If your organization has an
existing correlation-id convention (e.g., `X-Request-Id`), you can customize the key name:

```java
OperationContext context = OperationContext.builder()
    .correlationId("abc-123")
    .correlationIdKey("x-request-id")  // custom key name (must be lowercase)
    .build();
```

**When a custom key is supplied:**

- All three surfaces (object metadata, MDC, span attribute) use that custom name.
- The default keys (`sdk-logging-correlation-id` / `correlation_id`) are **not** also stamped
  (replace semantics, not dual stamping).
- The key must match `^[a-z0-9][a-z0-9_-]{0,127}$` (lowercase-only; S3 and GCS lowercase
  user-metadata keys on read, so uppercase would round-trip inconsistently).
- Reserved SDK keys (`trace_id`, `span_id`, `tenant_id`, `service_id`, etc.) are rejected to
  prevent collision with the SDK's own observability schema.

**Important:** Choosing a custom correlation key opts you out of any shared MulticloudJ dashboards
or alerts keyed on `correlation_id`. We recommend setting the key once per deployment (e.g., in
your service's configuration) rather than varying it per request.

**Note:** Only the correlation-id key is customizable. `sdk-logging-tenant-id` and
`sdk-logging-service-id` remain fixed across all deployments to maintain the cross-provider
wire contract.

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
