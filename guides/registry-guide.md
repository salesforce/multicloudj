---
layout: default
title: How to Container Registry
nav_order: 8
parent: Usage Guides
---
# ContainerRegistryClient

The `ContainerRegistryClient` class in the `multicloudj` library provides a cloud-agnostic interface to pull OCI/Docker container images from cloud-hosted container registries such as AWS Elastic Container Registry (ECR) and GCP Artifact Registry.

This client enables pulling images by tag or digest, selecting platform-specific images from multi-architecture manifests, and extracting image layer contents — all with a consistent API across cloud providers.

> **Note:** This client is read-only. It supports pulling and extracting images only. Pushing images, listing repositories, and deleting images are not supported.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP | AWS | ALI | Comments |
|---|---|---|---|---|
| **Pull image by tag** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Pull a named image version |
| **Pull image by digest** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Pull an immutable image by its sha256 digest |
| **Multi-arch image selection** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Selects the right image from an OCI image index based on platform |
| **Extract layers to tar stream** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Flattens all image layers into a single merged tar `InputStream` |
| **Lazy layer download** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Layer blobs are only downloaded when `getUncompressed()` is called |

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---|---|---|---|---|
| **Registry endpoint** | ✅ Required | ✅ Required | 📅 In Roadmap | Base URL of the container registry |
| **Region** | ➖ Not required | ✅ Required | 📅 In Roadmap | AWS uses region to construct and authenticate against ECR |
| **Platform (multi-arch)** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Target OS/architecture; defaults to `linux/amd64` |
| **Proxy endpoint** | ✅ Supported | ✅ Supported | 📅 In Roadmap | HTTP proxy for outbound registry traffic |
| **Credentials override** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Custom credential providers via `CredentialsOverrider` |

---

### Provider-Specific Notes

**AWS (Elastic Container Registry)**
- Authentication uses `EcrClient.getAuthorizationToken()`. Tokens are valid for 12 hours and are proactively refreshed at the 6-hour mark.
- Blob (layer) downloads are redirected to S3 pre-signed URLs. The client automatically strips the `Authorization` header for those requests to prevent authentication conflicts.
- Both `registryEndpoint` and `region` are required builder parameters.

**GCP (Artifact Registry)**
- Authentication uses Google Application Default Credentials (`GoogleCredentials`), scoped to `https://www.googleapis.com/auth/cloud-platform`. Credentials are automatically refreshed when expired.
- Auth username is `oauth2accesstoken` (standard for GCP Docker-compatible registries).
- `region` is not required; only `registryEndpoint` is mandatory.

---

## Creating a Client

### AWS ECR

```java
ContainerRegistryClient client = ContainerRegistryClient.builder("aws")
    .withRegistryEndpoint("https://123456789012.dkr.ecr.us-east-1.amazonaws.com")
    .withRegion("us-east-1")
    .build();
```

### GCP Artifact Registry

```java
ContainerRegistryClient client = ContainerRegistryClient.builder("gcp")
    .withRegistryEndpoint("https://us-docker.pkg.dev")
    .build();
```

### With a Proxy

```java
URI proxy = URI.create("http://proxy.example.com:8080");

ContainerRegistryClient client = ContainerRegistryClient.builder("aws")
    .withRegistryEndpoint("https://123456789012.dkr.ecr.us-east-1.amazonaws.com")
    .withRegion("us-east-1")
    .withProxyEndpoint(proxy)
    .build();
```

`ContainerRegistryClient` implements `AutoCloseable`. Use try-with-resources or call `close()` when done.

---

## Pulling an Image

`pull(String imageRef)` fetches the image manifest and returns an `Image` object. Layer blobs are **not downloaded** at this point — they are fetched lazily when `getUncompressed()` is called on a layer.

### Pull by Tag

```java
try (ContainerRegistryClient client = ContainerRegistryClient.builder("aws")
        .withRegistryEndpoint("https://123456789012.dkr.ecr.us-east-1.amazonaws.com")
        .withRegion("us-east-1")
        .build()) {

    Image image = client.pull("my-repo/my-image:latest");
    System.out.println("Pulled image digest: " + image.getDigest());
}
```

### Pull by Digest

```java
Image image = client.pull("my-repo/my-image@sha256:abcdef1234567890...");
```

---

## Working with Image Metadata

After pulling, you can inspect the image without downloading any data:

```java
Image image = client.pull("my-repo/my-image:latest");

System.out.println("Image ref:    " + image.getImageRef());   // original reference used to pull
System.out.println("Image digest: " + image.getDigest());     // sha256 manifest digest

List<Layer> layers = image.getLayers();
System.out.println("Layer count:  " + layers.size());

for (Layer layer : layers) {
    System.out.println("Layer digest: " + layer.getDigest() + " size: " + layer.getSize());
}
```

---

## Extracting Image Layers

`extract(Image image)` flattens all image layers into a single merged tar `InputStream`, applying OCI whiteout semantics (deleted files and opaque directories are handled correctly). This is useful for inspecting the image filesystem contents.

```java
Image image = client.pull("my-repo/my-image:latest");

try (InputStream tarStream = client.extract(image)) {
    // pipe to a TarArchiveInputStream for entry-by-entry inspection,
    // or write directly to disk
    Files.copy(tarStream, Paths.get("/tmp/image.tar"));
}
```

> **Note:** `extract()` starts a background thread to stream layer data. Close the returned `InputStream` when done to release resources.

---

## Multi-Architecture Images

When pulling from a multi-arch image index, the client selects the image matching the configured `Platform`. The default platform is `linux/amd64`.

```java
Platform arm64 = Platform.builder()
    .operatingSystem("linux")
    .architecture("arm64")
    .build();

ContainerRegistryClient client = ContainerRegistryClient.builder("gcp")
    .withRegistryEndpoint("https://us-docker.pkg.dev")
    .withPlatform(arm64)
    .build();

Image image = client.pull("my-project/my-repo/my-image:latest");
```

Use `Platform.DEFAULT` explicitly if you want `linux/amd64`:

```java
ContainerRegistryClient client = ContainerRegistryClient.builder("aws")
    .withRegistryEndpoint("https://123456789012.dkr.ecr.us-east-1.amazonaws.com")
    .withRegion("us-east-1")
    .withPlatform(Platform.DEFAULT)
    .build();
```

---

## Downloading Individual Layers

You can work with layers directly if you need per-layer access rather than a merged tar:

```java
Image image = client.pull("my-repo/my-image:latest");

for (Layer layer : image.getLayers()) {
    System.out.println("Downloading layer: " + layer.getDigest());

    // getUncompressed() triggers blob download and gzip-decompresses on the fly
    try (InputStream layerStream = layer.getUncompressed()) {
        // process the uncompressed tar stream for this layer
    }
}
```

---

## Error Handling

All operations may throw `SubstrateSdkException`:

```java
try (ContainerRegistryClient client = ContainerRegistryClient.builder("aws")
        .withRegistryEndpoint("https://123456789012.dkr.ecr.us-east-1.amazonaws.com")
        .withRegion("us-east-1")
        .build()) {

    Image image = client.pull("my-repo/my-image:latest");
    try (InputStream tarStream = client.extract(image)) {
        Files.copy(tarStream, Paths.get("/tmp/image.tar"));
    }

} catch (SubstrateSdkException e) {
    // covers: unauthorized, image not found, network errors, digest mismatch
    e.printStackTrace();
} catch (IOException e) {
    // covers: stream I/O errors during extract
    e.printStackTrace();
}
```

---

Use `ContainerRegistryClient` when you need a cloud-neutral way to pull and inspect OCI container images from AWS ECR or GCP Artifact Registry.
