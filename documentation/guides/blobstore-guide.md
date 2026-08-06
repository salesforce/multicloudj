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
| **Credentials Override** | ✅ Supported       | ✅ Supported | ✅ Supported | Custom credential providers via STS. See [Credentials and Long-Lived Clients](#credentials-and-long-lived-clients) |

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

## Credentials and Long-Lived Clients

A `BucketClient` holds a single underlying cloud connection for its whole lifetime. How you supply credentials therefore decides whether a client that outlives its credentials keeps working.

### Prefer the default credential chain

When the process already has an identity attached, such as a Kubernetes pod or a VM instance, supply no `CredentialsOverrider` at all. The cloud SDK's default credential chain then resolves and renews credentials on its own, and this remains the recommendation for those environments.

```java
BucketClient bucketClient = BucketClient.builder("aws")
    .withRegion("us-west-2")
    .withBucket("my-bucket")
    .build();
```

### Session credentials supplied by value are fixed

`withSessionCredentials(...)` freezes one set of credentials for the client's entire lifetime. That is fine for a client you build, use and discard. A client cached for the lifetime of the JVM, however, starts failing every call once the session token's TTL elapses, and only a restart recovers it.

```java
// Suitable only for a client that will not outlive these credentials.
StsCredentials credentials = new StsCredentials(accessKeyId, accessKeySecret, sessionToken);
CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
    .withSessionCredentials(credentials)
    .build();
```

### Session credentials supplied by callback are renewed

`withSessionCredentialsSupplier(...)` hands the SDK a `Supplier<StsCredentials>` instead of a value. The SDK invokes it again whenever the credentials it holds need renewing, so the client keeps working past the lifetime of any single set of credentials. When both forms are set on the same builder, the callback takes precedence.

```java
// secretBroker is your own component that holds currently valid session credentials.
CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
    .withSessionCredentialsSupplier(() -> new StsCredentials(
        secretBroker.accessKeyId(),
        secretBroker.accessKeySecret(),
        secretBroker.sessionToken(),
        secretBroker.expiresAt()))
    .build();

BucketClient bucketClient = BucketClient.builder("aws")
    .withRegion("us-west-2")
    .withBucket("my-bucket")
    .withCredentialsOverrider(credsOverrider)
    .build();
```

Two things to get right in the callback:

- **Populate the expiration.** The fourth `StsCredentials` constructor argument is a `java.time.Instant`, and supplying it lets renewal be scheduled ahead of expiry. Without it the SDK has nothing to schedule against: it treats the credentials as living for a fixed 15 minutes and renews on that cadence, so credentials that expire sooner will lapse before a renewal is due. The three-argument constructor leaves the expiration unset and remains supported.
- **Be thread-safe and quick.** The callback runs on request threads. It is invoked once per renewal rather than once per request, but other threads can block on that invocation, so it should return promptly rather than performing a slow synchronous fetch.

### Provider support for session credentials

| Provider | By value | By callback |
|----------|----------|-------------|
| AWS | Supported | Supported |
| GCP | Supported | Supported |
| ALI | Supported | Not supported |

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
