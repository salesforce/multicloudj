---
layout: default
title: How to Container Registry
nav_order: 7
parent: Usage Guides
---
# Container Registry

The `ContainerRegistryClient` class in the `multicloudj` library provides a cloud-agnostic interface for pulling container images and extracting their filesystems from cloud container registries. Supported providers are AWS Elastic Container Registry (ECR) and GCP Artifact Registry.

The client implements the [OCI Distribution Spec v2](https://github.com/opencontainers/distribution-spec) — authentication, manifest fetching, multi-platform image selection, and layer extraction are all handled automatically.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP Artifact Registry | AWS ECR | Comments |
|--------------|-----------------------|---------|----------|
| **Pull Image** | ✅ Supported | ✅ Supported | Fetches manifest and returns image metadata |
| **Extract Filesystem** | ✅ Supported | ✅ Supported | Returns flattened filesystem as a tar `InputStream` |
| **Pull by Tag** | ✅ Supported | ✅ Supported | e.g., `my-repo/my-image:latest` |
| **Pull by Digest** | ✅ Supported | ✅ Supported | e.g., `my-repo/my-image@sha256:abc...` |
| **Multi-arch Image Selection** | ✅ Supported | ✅ Supported | Selects the right manifest from an OCI image index |
| **Custom Platform Target** | ✅ Supported | ✅ Supported | Override default `linux/amd64` |

### Configuration Options

| Configuration | GCP Artifact Registry | AWS ECR | Comments |
|---------------|-----------------------|---------|----------|
| **Registry Endpoint** | ✅ Supported | ✅ Supported | Required for both providers |
| **Region** | ❌ Not Required | ✅ Supported | Required for AWS; not used by GCP |
| **Proxy Support** | ✅ Supported | ✅ Supported | HTTP proxy for outbound requests |
| **Credentials Override** | ✅ Supported | ✅ Supported | Custom credential providers via STS |
| **Platform Override** | ✅ Supported | ✅ Supported | Target OS/architecture for multi-arch images |

### Provider-Specific Notes

**AWS ECR (`aws`)**
- Registry endpoint format: `https://{accountId}.dkr.ecr.{region}.amazonaws.com`
- Authentication uses ECR's `GetAuthorizationToken` API. The token is valid for **12 hours** and is proactively refreshed at the **6-hour** mark. If a refresh fails, the cached token is reused as a fallback.
- Blob downloads are redirected to S3 pre-signed URLs. The SDK handles this transparently by stripping the `Authorization` header on S3 redirects to prevent authentication conflicts.
- Both `registryEndpoint` and `region` are required.

**GCP Artifact Registry (`gcp`)**
- Registry endpoint format: `https://{location}-docker.pkg.dev`
- Authentication uses OAuth2 access tokens from `GoogleCredentials`, exchanged for a registry-scoped bearer token. Tokens are automatically refreshed when expired.
- `region` is not required — the location is encoded in the registry endpoint.

---

## Creating a Client

```java
ContainerRegistryClient registryClient = ContainerRegistryClient.builder("aws")
    .withRegistryEndpoint("https://123456789012.dkr.ecr.us-west-2.amazonaws.com")
    .withRegion("us-west-2")
    .build();
```

For GCP Artifact Registry:

```java
ContainerRegistryClient registryClient = ContainerRegistryClient.builder("gcp")
    .withRegistryEndpoint("https://us-central1-docker.pkg.dev")
    .build();
```

With optional configuration:

```java
ContainerRegistryClient registryClient = ContainerRegistryClient.builder("aws")
    .withRegistryEndpoint("https://123456789012.dkr.ecr.us-west-2.amazonaws.com")
    .withRegion("us-west-2")
    .withProxyEndpoint(URI.create("https://proxy.example.com"))
    .withCredentialsOverrider(credentialsOverrider)
    .build();
```

---

## Pulling an Image

`pull` fetches the image manifest from the registry and returns an `Image` object. Layer blobs are loaded lazily on demand.

### Pull by tag

```java
Image image = registryClient.pull("my-repo/my-image:latest");
System.out.println("Digest: " + image.getDigest());
System.out.println("Layers: " + image.getLayers().size());
```

### Pull by digest

Pulling by digest is reproducible — it always refers to the same content regardless of tag mutations.

```java
Image image = registryClient.pull("my-repo/my-image@sha256:abc123...");
```

### Inspect layers

```java
for (Layer layer : image.getLayers()) {
    System.out.println("Layer digest: " + layer.getDigest());
    System.out.println("Layer size:   " + layer.getSize()); // -1 if unknown until download
}
```

---

## Multi-arch Images

When pulling from a multi-platform image index, the SDK automatically selects the manifest matching the target platform. The default target is `linux/amd64`.

To pull for a different platform, set it on the builder:

```java
Platform arm64 = Platform.builder()
    .operatingSystem("linux")
    .architecture("arm64")
    .build();

ContainerRegistryClient registryClient = ContainerRegistryClient.builder("aws")
    .withRegistryEndpoint("https://123456789012.dkr.ecr.us-west-2.amazonaws.com")
    .withRegion("us-west-2")
    .withPlatform(arm64)
    .build();

Image image = registryClient.pull("my-repo/my-image:latest");
```

The `Platform` matcher treats empty/null fields as wildcards — only specified fields must match.

---

## Extracting the Filesystem

`extract` downloads and decompresses all layers, then returns a single flattened tar `InputStream` representing the complete image filesystem. Layers are applied in order with later layers overriding earlier ones.

```java
Image image = registryClient.pull("my-repo/my-image:latest");

try (InputStream tar = registryClient.extract(image)) {
    // Read or write the tar stream
    Files.copy(tar, Path.of("/tmp/image-filesystem.tar"));
}
```

Extraction runs in a background thread and streams data through a pipe — memory usage stays bounded regardless of image size. The caller is responsible for closing the returned `InputStream`.

> **Note:** Whiteout files (`.wh.*`) that mark deletions in upper layers are handled automatically during extraction and do not appear in the output stream.

---

## Full Example

```java
try (ContainerRegistryClient registryClient = ContainerRegistryClient.builder("gcp")
        .withRegistryEndpoint("https://us-central1-docker.pkg.dev")
        .build()) {

    // Pull image
    Image image = registryClient.pull(
        "my-project/my-repo/my-image:v1.2.3");

    System.out.println("Image digest: " + image.getDigest());
    System.out.println("Layer count:  " + image.getLayers().size());

    // Extract filesystem
    try (InputStream tar = registryClient.extract(image)) {
        Files.copy(tar, Path.of("/tmp/image.tar"),
            StandardCopyOption.REPLACE_EXISTING);
    }

    System.out.println("Extraction complete.");
}
```

---

## Error Handling

All operations may throw `SubstrateSdkException` subclasses:

| Exception | Cause |
|-----------|-------|
| `ResourceNotFoundException` | Image, tag, or digest not found in the registry |
| `UnAuthorizedException` | Authentication failed or insufficient permissions |
| `InvalidArgumentException` | Null or malformed image reference; image has no layers; unsupported platform requested |
| `SubstrateSdkException` | Catch-all for network failures or unexpected registry responses |

```java
try {
    Image image = registryClient.pull("my-repo/my-image:missing-tag");
} catch (ResourceNotFoundException e) {
    System.out.println("Image not found");
} catch (UnAuthorizedException e) {
    System.out.println("Check registry credentials");
} catch (SubstrateSdkException e) {
    e.printStackTrace();
}
```

---

## Closing the Client

```java
registryClient.close();
```

Use try-with-resources for guaranteed cleanup:

```java
try (ContainerRegistryClient registryClient = ContainerRegistryClient.builder("aws")
        .withRegistryEndpoint("https://123456789012.dkr.ecr.us-west-2.amazonaws.com")
        .withRegion("us-west-2")
        .build()) {
    Image image = registryClient.pull("my-repo/my-image:latest");
    // use image...
}
```
