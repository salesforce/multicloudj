# Proposed MultiCloudJ Registry Module Design

## Executive Summary

This document proposes the design and implementation of a **Registry** module for MultiCloudJ, enabling unified Docker/OCI image pull operations across multiple cloud providers (AWS ECR, GCP Artifact Registry, Alibaba ACR). The design follows MultiCloudJ's established architectural patterns and maintains consistency with existing modules (Blob Store, DocStore, PubSub, STS).

## 1. Overview

### 1.1 Purpose

The Registry module provides a cloud-agnostic API for pulling Docker/OCI container images from remote registries. It abstracts away provider-specific differences in authentication and endpoint formats, allowing developers to write once and deploy to any cloud provider.

### 1.2 Key Features

- **Unified Pull API**: Single API for pulling images from AWS ECR, GCP GAR, and Alibaba ACR
- **Automatic Authentication**: Handles provider-specific authentication automatically
- **Multi-Architecture Support**: Supports multi-arch images (image indexes)
- **Docker Tar Format**: Outputs standard Docker image tar files compatible with `docker load`
- **Streaming Downloads**: Efficient streaming for large images
- **Provider Auto-Discovery**: Uses Java ServiceLoader for dynamic provider loading

### 1.3 Supported Providers

| Provider | Service | Status |
|----------|---------|--------|
| AWS | ECR (Elastic Container Registry) | ✅ Supported |
| GCP | Artifact Registry (GAR) | ✅ Supported |
| Alibaba | ACR (Container Registry) | ✅ Supported |

## 2. Architecture Design

### 2.1 Layered Architecture

The Registry module follows MultiCloudJ's three-layer architecture:

```
┌─────────────────────────────────────────┐
│   Portable Layer (RepositoryClient)    │  ← User-facing API
├─────────────────────────────────────────┤
│   Driver Layer (AbstractRegistry)      │  ← Common implementation
├─────────────────────────────────────────┤
│   Provider Layer (AwsRegistry, etc.)   │  ← Cloud-specific logic
└─────────────────────────────────────────┘
```

#### Portable Layer
- **RepositoryClient**: High-level client for repository operations
- **Builder Pattern**: `RepositoryClient.builder(providerId).withRepository(...).build()`
- **Error Handling**: Automatic exception mapping and propagation

#### Driver Layer
- **AbstractRegistry**: Base class with common pull implementation
- **OciRegistryClient**: HTTP client for OCI Registry API v2 operations
- **DockerTarBuilder**: Assembles Docker image tar files
- **ImageReference**: Parses and validates image references

#### Provider Layer
- **AwsRegistry**: AWS ECR implementation
- **GcpRegistry**: GCP Artifact Registry implementation
- **AliRegistry**: Alibaba ACR implementation

### 2.2 Core Components

#### 2.2.1 ImageReference
Represents a Docker image reference with parsing support:
```java
public class ImageReference {
    private final String registry;    // e.g., "gcr.io"
    private final String repository;  // e.g., "my-project/my-image"
    private final String reference;  // tag or digest
    private final boolean isDigest;
    
    public static ImageReference parse(String imageRef);
}
```

**Supported Formats:**
- `"my-image:latest"` → repository="my-image", reference="latest"
- `"my-image@sha256:abc123"` → repository="my-image", reference="sha256:abc123", isDigest=true
- `"gcr.io/project/image:v1"` → registry="gcr.io", repository="project/image", reference="v1"

#### 2.2.2 AbstractRegistry
Base class implementing the common pull flow:

```java
public abstract class AbstractRegistry {
    // Provider-specific implementations
    protected abstract String getRegistryEndpoint();
    protected abstract String getDockerAuthToken() throws IOException;
    protected abstract String getDockerAuthUsername();
    
    // Common implementation
    public PullResult pullImage(String imageRef, Path destination) {
        // 1. Parse image reference
        // 2. Fetch manifest (with multi-arch support)
        // 3. Download config and layers
        // 4. Build Docker tar file
        // 5. Return result
    }
}
```

**Pull Flow:**
1. Parse image reference (`ImageReference.parse()`)
2. Fetch manifest from registry (`GET /v2/{repo}/manifests/{ref}`)
3. Handle multi-arch image index (if present)
4. Download config blob (`GET /v2/{repo}/blobs/{digest}`)
5. Download each layer blob (streaming)
6. Assemble Docker tar file using `DockerTarBuilder`
7. Return `PullResult` with metadata

#### 2.2.3 OciRegistryClient
HTTP client for OCI Registry API v2:

```java
class OciRegistryClient {
    interface AuthProvider {
        String getAuthHeader() throws IOException;
    }
    
    Manifest fetchManifest(String reference);
    InputStream downloadBlob(String digest);
}
```

**Features:**
- Supports both image manifests and image indexes (multi-arch)
- Handles authentication with retry on 401
- Accepts multiple media types for compatibility
- Streaming blob downloads

#### 2.2.4 DockerTarBuilder
Assembles Docker image tar files from components:

```java
class DockerTarBuilder {
    void addVersion();                    // VERSION file
    void addConfig(String layerId, String configJson);
    void addLayer(String layerId, InputStream layerData);
    void addManifest(String manifestJson);
}
```

**Output Format:**
```
image.tar
├── VERSION
├── manifest.json
├── sha256_abc123/
│   ├── json (config)
│   └── layer.tar
└── sha256_def456/
    └── layer.tar
```

### 2.3 Authentication Architecture

The authentication design follows MultiCloudJ's established pattern:

#### Design Decision: Abstract Base Class Pattern

**Why this pattern?**
- Consistent with other MultiCloudJ modules (BlobStore, DocStore, PubSub)
- Simple and straightforward
- Type-safe with compile-time checks
- Easy to understand and maintain

**Implementation:**
```java
public abstract class AbstractRegistry {
    // Provider implements these
    protected abstract String getDockerAuthToken() throws IOException;
    protected abstract String getDockerAuthUsername();
    
    // Common implementation
    protected final String getDockerAuthHeader() throws IOException {
        String username = getDockerAuthUsername();
        String token = getDockerAuthToken();
        String credentials = username + ":" + token;
        return "Basic " + Base64.getEncoder()
            .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }
}
```

**Provider Implementations:**

| Provider | Token Source | Username |
|----------|-------------|----------|
| AWS ECR | `ECR.GetAuthorizationToken` API | `"AWS"` |
| GCP GAR | OAuth2 access token | `"oauth2accesstoken"` |
| Alibaba ACR | ACR authorization API | Aliyun account name |

## 3. API Design

### 3.1 RepositoryClient API

```java
public class RepositoryClient implements AutoCloseable {
    public static RepositoryClientBuilder builder(String providerId);
    
    public PullResult pullImage(String imageRef, Path destination);
}

// Builder
public class RepositoryClientBuilder {
    public RepositoryClientBuilder withRepository(String repository);
    public RepositoryClientBuilder withRegion(String region);
    public RepositoryClientBuilder withEndpoint(URI endpoint);
    public RepositoryClientBuilder withCredentialsOverrider(CredentialsOverrider overrider);
    public RepositoryClient build();
}
```

### 3.2 Usage Example

```java
// Pull from AWS ECR
try (RepositoryClient client = RepositoryClient.builder("aws")
        .withRepository("my-ecr-repo")
        .withRegion("us-east-1")
        .build()) {
    
    PullResult result = client.pullImage(
        "my-image:latest",
        Paths.get("/tmp/image.tar")
    );
    
    System.out.println("Image saved to: " + result.getSavedPath());
    System.out.println("Digest: " + result.getMetadata().getDigest());
}

// Same API for GCP and Alibaba
RepositoryClient.builder("gcp")...  // GCP Artifact Registry
RepositoryClient.builder("ali")...  // Alibaba ACR
```

### 3.3 Response Model

```java
@Builder
@Getter
public class PullResult {
    private final ImageMetadata metadata;  // digest, tag
    private final String savedPath;        // path to tar file
}

@Builder
@Getter
public class ImageMetadata {
    private final String digest;  // image digest
    private final String tag;     // image tag (null if pulled by digest)
}
```

## 4. Implementation Details

### 4.1 Multi-Architecture Support

The implementation handles multi-arch images (image indexes):

```java
// 1. Fetch manifest with full Accept header
Manifest manifest = client.fetchManifest(reference);

// 2. Check if it's an image index
if (manifest.isIndex()) {
    // 3. Select platform (default: first manifest)
    // TODO: Support platform selection (os/arch)
    String selectedDigest = selectPlatform(manifest.getManifests());
    
    // 4. Fetch specific manifest
    manifest = client.fetchManifest(selectedDigest);
}

// 5. Continue with regular image manifest
```

**Accept Header:**
```java
"application/vnd.oci.image.index.v1+json," +
"application/vnd.docker.distribution.manifest.list.v2+json," +
"application/vnd.oci.image.manifest.v1+json," +
"application/vnd.docker.distribution.manifest.v2+json"
```

### 4.2 Error Handling

- **401 Unauthorized**: Automatic retry with fresh auth token
- **404 Not Found**: Throws `ResourceNotFoundException`
- **Network Errors**: Retry with configurable retry policy
- **Provider Exceptions**: Mapped to MultiCloudJ common exceptions

### 4.3 Streaming and Performance

- **Streaming Downloads**: Layers downloaded as streams, not buffered in memory
- **Parallel Downloads**: Future enhancement for parallel layer downloads
- **Progress Tracking**: Future enhancement for download progress callbacks

## 5. Provider Implementation

### 5.1 Minimal Implementation Required

Each provider only needs to implement **3 abstract methods**:

```java
@AutoService(AbstractRegistry.class)
public class AwsRegistry extends AbstractRegistry {
    @Override
    protected String getRegistryEndpoint() {
        return String.format("https://%s.dkr.ecr.%s.amazonaws.com", 
                           accountId, getRegion());
    }
    
    @Override
    protected String getDockerAuthToken() throws IOException {
        // Call ECR GetAuthorizationToken API
        GetAuthorizationTokenResponse response = ecrClient.getAuthorizationToken(...);
        return extractToken(response);
    }
    
    @Override
    protected String getDockerAuthUsername() {
        return "AWS";
    }
}
```

### 5.2 Provider Discovery

Uses Java ServiceLoader pattern:
- `@AutoService(AbstractRegistry.class)` annotation
- Registered in `META-INF/services/com.salesforce.multicloudj.registry.driver.AbstractRegistry`
- Loaded dynamically at runtime based on `providerId`

## 6. Consistency with MultiCloudJ Patterns

### 6.1 Design Pattern Consistency

| Aspect | Registry | BlobStore | DocStore | PubSub |
|--------|----------|-----------|----------|--------|
| Abstract Base Class | ✅ `AbstractRegistry` | ✅ `AbstractBlobStore` | ✅ `AbstractDocStore` | ✅ `AbstractSubscription` |
| Protected Abstract Methods | ✅ | ✅ | ✅ | ✅ |
| Builder Pattern | ✅ | ✅ | ✅ | ✅ |
| ServiceLoader Discovery | ✅ | ✅ | ✅ | ✅ |
| CredentialsOverrider | ✅ | ✅ | ✅ | ✅ |

### 6.2 Code Structure Consistency

```
registry/
├── registry-client/          # Portable + Driver layer
│   ├── RepositoryClient      # Portable API
│   ├── AbstractRegistry      # Driver abstraction
│   └── OciRegistryClient     # OCI protocol implementation
├── registry-aws/             # AWS provider
├── registry-gcp/             # GCP provider
└── registry-ali/             # Alibaba provider
```

## 7. Testing Strategy

### 7.1 Unit Tests
- Provider-specific implementations
- ImageReference parsing edge cases
- Multi-arch handling
- Error scenarios

### 7.2 Conformance Tests
- Base test suite in `registry-client` module
- Each provider extends and runs conformance tests
- Ensures API compliance across providers

### 7.3 Integration Tests
- Real registry interactions (with WireMock recording/replay)
- Multi-arch image scenarios
- Authentication edge cases

## 8. Future Enhancements

### 8.1 Planned Features
- **Platform Selection**: Support for selecting specific platform (os/arch) from multi-arch images
- **Push Support**: Add image push capability
- **Image Copy**: Copy images between registries
- **Progress Callbacks**: Download progress tracking
- **Parallel Downloads**: Download layers in parallel

### 8.2 Potential Enhancements
- **Image Tagging**: Tag management operations
- **Image Listing**: List images in repository
- **Manifest Inspection**: Inspect manifest without downloading
- **Layer Caching**: Cache layers locally to avoid re-downloading

## 9. Migration and Compatibility

### 9.1 Backward Compatibility
- API designed for future extensibility
- New methods added as non-breaking changes
- Deprecation path for any breaking changes

### 9.2 Provider Addition
Adding a new provider requires:
1. Implement `AbstractRegistry` with 3 methods
2. Add `@AutoService` annotation
3. Register in ServiceLoader
4. Add provider-specific tests

## 10. Security Considerations

### 10.1 Authentication
- Credentials never logged or exposed
- Token refresh handled automatically
- Support for temporary credentials via STS

### 10.2 Network Security
- HTTPS only for registry communication
- Certificate validation
- Support for custom endpoints (for air-gapped environments)

## 11. Performance Considerations

### 11.1 Current Optimizations
- Streaming downloads (no memory buffering)
- Efficient tar file assembly
- Single-pass layer processing

### 11.2 Future Optimizations
- Parallel layer downloads
- Layer deduplication
- Incremental pulls (only download missing layers)

## 12. Documentation

### 12.1 User Documentation
- [Usage Guide](./registry-client/USAGE_GUIDE.md)
- [Cross-Cloud Pull API](./registry-client/CROSS_CLOUD_PULL_API.md)
- [Provider Implementation Guide](./PROVIDER_IMPLEMENTATION_GUIDE.md)

### 12.2 Design Documentation
- [Design Spike: Authentication Architecture](./DESIGN_SPIKE.md)
- This document (Proposed Design)

## 13. Conclusion

The proposed Registry module design:

✅ **Follows MultiCloudJ patterns**: Consistent with existing modules  
✅ **Simple and maintainable**: Minimal provider implementation required  
✅ **Extensible**: Easy to add new providers  
✅ **Production-ready**: Handles edge cases (multi-arch, authentication, errors)  
✅ **Well-documented**: Comprehensive guides for users and implementers  

The design balances simplicity with functionality, ensuring developers can easily pull images from any supported cloud provider with a unified API.

---

## Appendix A: Comparison with Other Solutions

### Docker CLI
- **Pros**: Industry standard, well-tested
- **Cons**: Provider-specific commands, no unified API

### Skopeo
- **Pros**: OCI-focused, supports multiple formats
- **Cons**: Command-line only, no Java SDK

### go-containerregistry
- **Pros**: Excellent OCI support, well-designed
- **Cons**: Go-only, not Java

### MultiCloudJ Registry
- **Pros**: Unified Java API, cloud-agnostic, consistent with MultiCloudJ ecosystem
- **Cons**: Newer, smaller community

## Appendix B: Reference Implementation

See existing implementations:
- [AWS ECR Implementation Guide](./PROVIDER_IMPLEMENTATION_GUIDE.md#aws-ecr-实现示例)
- [GCP GAR Implementation Guide](./PROVIDER_IMPLEMENTATION_GUIDE.md#gcp-gar-实现示例)
- [Alibaba ACR Implementation Guide](./PROVIDER_IMPLEMENTATION_GUIDE.md#alibaba-acr-实现示例)
