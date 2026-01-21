# MultiCloudJ Registry Design Proposal

## Table of Contents
1. [Goal](#goal)
2. [Background](#background)
3. [Proposed Approach](#proposed-approach)
4. [Provider Comparison](#provider-comparison)
5. [Comparison with go-containerregistry](#comparison-with-go-containerregistry)
6. [Implementation Details](#implementation-details)

## Goal

The goal of the MultiCloudJ Registry module is to provide a **unified, cloud-agnostic API** for pulling Docker/OCI container images from multiple cloud providers (AWS ECR, GCP Artifact Registry, Alibaba ACR) without requiring substrate-specific code.

### Key Objectives

- ✅ **Unified Pull API**: Single API across AWS ECR, GCP GAR, and Alibaba ACR
- ✅ **Multiple Pull Modes**: Support for Path, File, OutputStream, and InputStream (lazy loading)
- ✅ **Dynamic Authentication**: Automatic discovery of authentication requirements (Ping + AuthChallenge)
- ✅ **Bearer Token Exchange**: Support for OAuth2 token exchange (GCP GAR)
- ✅ **Multi-Architecture Support**: Handles multi-arch images (image indexes)
- ✅ **Docker Tar Format**: Outputs standard Docker image tar files compatible with `docker load`
- ✅ **Streaming Downloads**: Efficient streaming for large images
- ✅ **Consistent Design**: Follows MultiCloudJ's established architectural patterns

## Background

### Problem Statement

Currently, pulling Docker images from different cloud providers requires:
- Substrate-specific code for each provider
- Different authentication mechanisms (ECR tokens, GCP OAuth2, ACR tokens)
- Different endpoint formats
- Manual handling of OCI Registry API v2 protocol

### Existing Solutions

1. **Docker CLI**: Provider-specific commands, no unified API
2. **Skopeo**: Command-line only, no Java SDK
3. **go-containerregistry**: Excellent OCI support, but Go-only
4. **Cloud-specific SDKs**: AWS SDK, GCP SDK, Alibaba SDK - each with different APIs

### MultiCloudJ Approach

MultiCloudJ Registry provides a **unified Java API** that:
- Abstracts away provider-specific differences
- Handles authentication automatically
- Follows MultiCloudJ's established patterns (consistent with Blob, DocStore, PubSub)
- Supports multiple pull modes including lazy loading

## Proposed Approach

### Three-Layer Architecture

MultiCloudJ Registry follows the standard MultiCloudJ three-layer architecture:

```
┌─────────────────────────────────────────┐
│   Portable Layer (RepositoryClient)    │  ← User-facing API
│   - Builder pattern                     │
│   - Exception handling                  │
│   - Resource management                 │
├─────────────────────────────────────────┤
│   Driver Layer (AbstractRegistry)      │  ← Common implementation
│   - OCI Registry protocol              │
│   - Authentication flow                 │
│   - Docker tar building                 │
│   - Multi-arch support                  │
├─────────────────────────────────────────┤
│   Provider Layer (AwsRegistry, etc.)  │  ← Cloud-specific logic
│   - Endpoint format                     │
│   - Credential retrieval                │
│   - Exception mapping                   │
└─────────────────────────────────────────┘
```

### Layer 1: Portable Layer (RepositoryClient)

**Purpose**: User-facing API with builder pattern and exception handling.

**Components**:
- `RepositoryClient`: Main entry point for users
- `RepositoryClientBuilder`: Builder for configuration

**Responsibilities**:
- Provide clean, intuitive API
- Handle exceptions and map to MultiCloudJ common exceptions
- Manage resources (AutoCloseable)
- Validate inputs

**API**:
```java
public class RepositoryClient implements AutoCloseable {
    public static RepositoryClientBuilder builder(String providerId);
    
    // Multiple pull modes
    public PullResult pullImage(String imageRef, Path destination);
    public PullResult pullImage(String imageRef, OutputStream outputStream);
    public PullResult pullImage(String imageRef, File file);
    public PullResult pullImage(String imageRef);  // Lazy loading
}
```

**Builder Options**:
```java
RepositoryClient.builder("aws")
    .withRepository("my-repo")              // Optional
    .withRegion("us-east-1")                 // Required
    .withEndpoint(URI.create("https://...")) // Optional
    .withProxyEndpoint(URI.create("https://...")) // Optional
    .withCredentialsOverrider(creds)         // Optional
    .withRetryConfig(retryConfig)            // Optional
    .build();
```

### Layer 2: Driver Layer (AbstractRegistry)

**Purpose**: Common implementation of pull logic, shared across all providers.

**Components**:
- `AbstractRegistry`: Base class with common pull implementation
- `OciRegistryClient`: HTTP client for OCI Registry API v2
- `DockerTarBuilder`: Assembles Docker image tar files
- `RegistryPing`: Discovers authentication requirements
- `AuthChallenge`: Parses authentication challenges
- `BearerTokenExchange`: Exchanges identity tokens for Bearer Tokens
- `ImageReference`: Parses and validates image references

**Responsibilities**:
- Implement OCI Registry API v2 protocol
- Handle dynamic authentication discovery
- Build Docker tar files
- Support multi-arch images
- Provide multiple pull modes (Path, File, OutputStream, InputStream)

**Key Methods**:
```java
public abstract class AbstractRegistry {
    // Provider-specific (implemented by each provider)
    protected abstract String getRegistryEndpoint();
    protected abstract String getDockerAuthToken() throws IOException;
    protected abstract String getDockerAuthUsername();
    protected String getIdentityToken() throws IOException;  // GCP only
    
    // Common implementation (used by all providers)
    public PullResult pullImage(String imageRef, Path destination);
    public PullResult pullImage(String imageRef, OutputStream os);
    public PullResult pullImage(String imageRef);  // Lazy loading
    
    protected PullResult doPullImageUsingOciRegistry(...);
}
```

**Pull Flow**:
1. Parse image reference (`ImageReference.parse()`)
2. Ping registry to discover auth requirements (`RegistryPing.ping()`)
3. Authenticate (Basic Auth or Bearer Token exchange)
4. Fetch manifest (`GET /v2/{repo}/manifests/{ref}`)
5. Handle multi-arch image index (if present)
6. Download config blob (`GET /v2/{repo}/blobs/{digest}`)
7. Download each layer blob (streaming)
8. Build Docker tar file using `DockerTarBuilder`
9. Return `PullResult` with metadata

### Layer 3: Provider Layer (AwsRegistry, GcpRegistry, AliRegistry)

**Purpose**: Cloud-specific logic for each provider.

**Components**:
- `AwsRegistry`: AWS ECR implementation
- `GcpRegistry`: GCP Artifact Registry implementation
- `AliRegistry`: Alibaba ACR implementation

**Responsibilities**:
- Provide registry endpoint URL
- Retrieve authentication tokens
- Map provider-specific exceptions to MultiCloudJ exceptions

**Minimal Implementation Required**:

Each provider only needs to implement **3-4 abstract methods**:

```java
@AutoService(AbstractRegistry.class)
public class AwsRegistry extends AbstractRegistry {
    @Override
    protected String getRegistryEndpoint() {
        // ECR: https://{accountId}.dkr.ecr.{region}.amazonaws.com
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
    
    @Override
    protected String getIdentityToken() throws IOException {
        return null;  // ECR doesn't need identity token
    }
}
```

## Provider Comparison

### Provider-Specific Differences

| Aspect | AWS ECR | GCP Artifact Registry | Alibaba ACR |
|--------|---------|----------------------|-------------|
| **Endpoint Format** | `https://{accountId}.dkr.ecr.{region}.amazonaws.com` | `https://{location}-docker.pkg.dev/{project}/{repository}` | `https://{registryName}.{region}.aliyuncs.com` |
| **Auth Scheme** | Basic | Bearer | Basic |
| **Token Source** | `ECR.GetAuthorizationToken` API | OAuth2 access token | ACR authorization API |
| **Token Format** | Base64 encoded `AWS:token` | Bearer Token (from token server) | `username:password` |
| **Username** | `"AWS"` | N/A (Bearer Token) | Aliyun account name |
| **Identity Token** | ❌ Not needed | ✅ OAuth2 access token | ❌ Not needed |
| **Token Exchange** | ❌ No | ✅ Yes (Bearer Token exchange) | ❌ No |
| **API Call** | AWS ECR API | Google OAuth2 API | Alibaba ACR API |

### Authentication Flow Comparison

#### AWS ECR

```java
// 1. Call AWS ECR API
GetAuthorizationTokenResponse response = ecrClient.getAuthorizationToken(
    GetAuthorizationTokenRequest.builder()
        .build()
);

// 2. Extract token (base64 encoded "AWS:token")
String token = response.authorizationData().get(0).authorizationToken();

// 3. Decode to get actual token
String decoded = Base64.getDecoder().decode(token);
// Format: "AWS:actual-token"

// 4. Use Basic Auth
String authHeader = "Basic " + Base64.encode("AWS:" + actualToken);
```

**Characteristics**:
- ✅ Direct API call to ECR
- ✅ Returns Basic Auth credentials
- ✅ Token valid for 12 hours
- ❌ No Bearer Token exchange needed

#### GCP Artifact Registry

```java
// 1. Get OAuth2 access token (identity token)
GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
credentials.refreshIfExpired();
String identityToken = credentials.getAccessToken().getTokenValue();

// 2. Exchange identity token for Bearer Token
// Registry returns WWW-Authenticate: Bearer realm="https://oauth2.googleapis.com/token"
AuthChallenge challenge = RegistryPing.ping(registryEndpoint);
// challenge.scheme = "Bearer"
// challenge.realm = "https://oauth2.googleapis.com/token"

// 3. POST to token server
String bearerToken = BearerTokenExchange.getBearerToken(
    challenge, 
    identityToken, 
    scopes
);

// 4. Use Bearer Token
String authHeader = "Bearer " + bearerToken;
```

**Characteristics**:
- ✅ Uses Application Default Credentials (ADC)
- ✅ Requires Bearer Token exchange
- ✅ Token valid for 1 hour
- ✅ Two-step authentication (identity token → Bearer Token)

#### Alibaba ACR

```java
// 1. Call Alibaba ACR API
GetAuthorizationTokenRequest request = GetAuthorizationTokenRequest.builder()
    .instanceId(registryName)
    .build();
GetAuthorizationTokenResponse response = acrClient.getAuthorizationToken(request);

// 2. Extract credentials
String tempUsername = response.tempUsername();
String authorizationToken = response.authorizationToken();

// 3. Use Basic Auth
String authHeader = "Basic " + Base64.encode(tempUsername + ":" + authorizationToken);
```

**Characteristics**:
- ✅ Direct API call to ACR
- ✅ Returns Basic Auth credentials
- ✅ Token valid for 1 hour
- ❌ No Bearer Token exchange needed

### Endpoint Format Comparison

#### AWS ECR
```
https://{accountId}.dkr.ecr.{region}.amazonaws.com
```
**Example**: `https://123456789.dkr.ecr.us-east-1.amazonaws.com`

**Components**:
- `accountId`: AWS account ID (12 digits)
- `region`: AWS region (e.g., `us-east-1`)

#### GCP Artifact Registry
```
https://{location}-docker.pkg.dev/{project}/{repository}
```
**Example**: `https://us-central1-docker.pkg.dev/my-project/my-repo`

**Components**:
- `location`: GCP location (e.g., `us-central1`)
- `project`: GCP project ID
- `repository`: Repository name

**Note**: Repository is part of the endpoint, not just the path.

#### Alibaba ACR
```
https://{registryName}.{region}.aliyuncs.com
```
**Example**: `https://my-registry.cn-hangzhou.aliyuncs.com`

**Components**:
- `registryName`: ACR registry name
- `region`: Alibaba Cloud region (e.g., `cn-hangzhou`)

### Implementation Differences

#### AwsRegistry

```java
public class AwsRegistry extends AbstractRegistry {
    private final EcrClient ecrClient;
    
    @Override
    protected String getRegistryEndpoint() {
        String accountId = extractAccountId();  // From credentials or config
        return String.format("https://%s.dkr.ecr.%s.amazonaws.com", 
                           accountId, getRegion());
    }
    
    @Override
    protected String getDockerAuthToken() throws IOException {
        GetAuthorizationTokenResponse response = ecrClient.getAuthorizationToken(
            GetAuthorizationTokenRequest.builder().build()
        );
        String encoded = response.authorizationData().get(0).authorizationToken();
        // Decode base64 "AWS:token" to get actual token
        String decoded = new String(Base64.getDecoder().decode(encoded));
        return decoded.split(":")[1];  // Extract token part
    }
    
    @Override
    protected String getDockerAuthUsername() {
        return "AWS";
    }
}
```

#### GcpRegistry

```java
public class GcpRegistry extends AbstractRegistry {
    private final GoogleCredentials credentials;
    
    @Override
    protected String getRegistryEndpoint() {
        // Repository is part of endpoint for GCP
        return String.format("https://%s-docker.pkg.dev/%s/%s",
                           getLocation(), getProject(), getRepository());
    }
    
    @Override
    protected String getDockerAuthToken() throws IOException {
        // For GCP, we use Bearer Token (not Basic Auth)
        // This is handled by getIdentityToken() + BearerTokenExchange
        throw new UnsupportedOperationException("GCP uses Bearer Token, not Basic Auth");
    }
    
    @Override
    protected String getDockerAuthUsername() {
        // Not used for Bearer Token
        return "oauth2accesstoken";
    }
    
    @Override
    protected String getIdentityToken() throws IOException {
        // Get OAuth2 access token
        credentials.refreshIfExpired();
        return credentials.getAccessToken().getTokenValue();
    }
}
```

#### AliRegistry

```java
public class AliRegistry extends AbstractRegistry {
    private final AcsClient acsClient;
    
    @Override
    protected String getRegistryEndpoint() {
        return String.format("https://%s.%s.aliyuncs.com",
                           getRegistryName(), getRegion());
    }
    
    @Override
    protected String getDockerAuthToken() throws IOException {
        GetAuthorizationTokenRequest request = GetAuthorizationTokenRequest.builder()
            .instanceId(getRegistryName())
            .build();
        GetAuthorizationTokenResponse response = acsClient.getAuthorizationToken(request);
        return response.authorizationToken();
    }
    
    @Override
    protected String getDockerAuthUsername() {
        // Get from ACR API response
        GetAuthorizationTokenResponse response = acsClient.getAuthorizationToken(...);
        return response.tempUsername();
    }
}
```

### Exception Mapping

Each provider maps its exceptions to MultiCloudJ common exceptions:

| Provider Exception | MultiCloudJ Exception |
|-------------------|----------------------|
| AWS: `SdkException` | `AwsSdkException` |
| GCP: `GoogleJsonResponseException` | `GcpSdkException` |
| Alibaba: `ClientException` | `AliSdkException` |

## Comparison with go-containerregistry

### API Signature Comparison

#### go-containerregistry

```go
// Function signature
func Pull(src string, opt ...Option) (v1.Image, error)

// Input:
//   - src: string - Image reference (e.g., "gcr.io/project/image:tag")
//   - opt: ...Option - Optional configuration

// Output:
//   - v1.Image: Structured image object (interface)
//   - error: Error if pull fails

// Usage:
img, err := crane.Pull("gcr.io/project/image:tag",
    crane.WithAuth(auth),
    crane.WithPlatform(platform),
    crane.WithContext(ctx),
)
```

#### MultiCloudJ Registry

```java
// Method signatures (multiple overloads)
public PullResult pullImage(String imageRef, Path destination)
public PullResult pullImage(String imageRef, OutputStream outputStream)
public PullResult pullImage(String imageRef, File file)
public PullResult pullImage(String imageRef)  // Lazy loading

// Input:
//   - imageRef: String - Image reference (e.g., "my-image:latest")
//   - destination: Optional - Where to save tar file

// Output:
//   - PullResult: Wrapper with metadata + savedPath/inputStream

// Usage:
RepositoryClient client = RepositoryClient.builder("gcp")
    .withRepository("project/image")
    .withRegion("us-central1")
    .build();
    
PullResult result = client.pullImage("image:tag", Paths.get("image.tar"));
```

### Detailed Comparison Table

| Aspect | go-containerregistry | MultiCloudJ Registry |
|--------|---------------------|---------------------|
| **Function Name** | `Pull` | `pullImage` |
| **Input Parameters** | `src string, opt ...Option` | `imageRef String, destination? (Path/File/OutputStream)` |
| **Output Type** | `v1.Image` (interface) | `PullResult` (class) |
| **Return Value** | Structured image object | Wrapper with metadata + tar |
| **Lazy Loading** | ✅ Yes (per layer) | ✅ Yes (entire tar stream) |
| **Access Components** | ✅ Yes (layers, config, manifest) | ❌ No (only tar stream) |
| **Multiple Modes** | ❌ No (always returns Image) | ✅ Yes (4 modes) |
| **Tar File** | ❌ No (need `crane.Save()`) | ✅ Yes (built automatically) |
| **Immediate Save** | ❌ No (two-step: Pull + Save) | ✅ Yes (one-step) |
| **Image Manipulation** | ✅ Yes (mutate, extract, etc.) | ❌ No (download only) |

### Design Philosophy Comparison

| Aspect | go-containerregistry | MultiCloudJ Registry |
|--------|---------------------|---------------------|
| **Primary Use Case** | Image manipulation (mutate, extract, inspect) | Image download (pull to tar) |
| **Flexibility** | High (structured access) | Medium (tar file focus) |
| **Simplicity** | Medium (need to understand Image interface) | High (direct tar file) |
| **Language Idioms** | Go interfaces and lazy loading | Java streams and builders |
| **API Design** | Functional (returns object) | Imperative (saves to destination) |

### Options Comparison

#### go-containerregistry Options

```go
crane.Pull("my-image:latest",
    crane.WithAuth(auth),                    // ✅ 认证
    crane.WithPlatform(platform),             // ✅ 平台选择
    crane.WithContext(ctx),                  // ✅ 超时/取消
    crane.WithUserAgent("MyApp/1.0"),        // ✅ User-Agent
    crane.WithTransport(transport),           // ✅ Transport
    crane.Insecure,                          // ✅ 非 TLS
    crane.WithJobs(8),                       // ✅ 并发数
)
```

#### MultiCloudJ Registry Options

```java
RepositoryClient.builder("aws")
    .withRepository("my-repo")              // ✅ repository
    .withRegion("us-east-1")                 // ✅ region
    .withEndpoint(uri)                       // ✅ endpoint
    .withProxyEndpoint(uri)                  // ✅ 代理
    .withCredentialsOverrider(creds)         // ✅ 认证
    .withRetryConfig(retryConfig)            // ✅ 重试
    .build();
```

**Coverage**:
- ✅ Both support authentication
- ✅ Both support custom endpoint
- ❌ go-cr has platform selection (MultiCloudJ TODO)
- ❌ go-cr has context/timeout (MultiCloudJ registry doesn't have timeout, but blob has `withSocketTimeout`)
- ✅ MultiCloudJ has retry config (go-cr has built-in)
- ✅ MultiCloudJ has proxy support (go-cr doesn't)

### Workflow Comparison

#### go-containerregistry

```go
// Step 1: Pull image (returns structured object)
img, err := crane.Pull("my-image:latest")

// Step 2: Access components (if needed)
layers, _ := img.Layers()
config, _ := img.ConfigFile()
manifest, _ := img.Manifest()

// Step 3: Save to tar (separate step)
err = crane.Save(img, "tag", "image.tar")
```

#### MultiCloudJ Registry

```java
// Option 1: One-step pull and save
PullResult result = client.pullImage("my-image:latest", Paths.get("image.tar"));

// Option 2: Lazy loading (similar to go-cr)
PullResult result = client.pullImage("my-image:latest");
InputStream is = result.getInputStream();  // Data streams on-demand
```

## Implementation Details

### Authentication Implementation

#### Dynamic Authentication Discovery

The registry client uses a **Ping → Challenge → Exchange** flow:

```java
// 1. Ping registry
AuthChallenge challenge = RegistryPing.ping(registryEndpoint);
// Sends: GET /v2/
// Receives: 401 WWW-Authenticate: Bearer realm="https://oauth2.googleapis.com/token"

// 2. Parse challenge
// challenge.scheme = "Bearer"
// challenge.realm = "https://oauth2.googleapis.com/token"
// challenge.service = "pkg.dev"

// 3. Select auth scheme
if (challenge.getScheme().equalsIgnoreCase("Basic")) {
    return authProvider.getBasicAuthHeader();
} else if (challenge.getScheme().equalsIgnoreCase("Bearer")) {
    String identityToken = authProvider.getIdentityToken();
    String bearerToken = BearerTokenExchange.getBearerToken(challenge, identityToken, scopes);
    return "Bearer " + bearerToken;
}
```

#### RegistryPing Implementation

```java
public class RegistryPing {
    public static AuthChallenge ping(String registryEndpoint) throws IOException {
        String pingUrl = registryEndpoint;
        if (!pingUrl.endsWith("/")) {
            pingUrl += "/";
        }
        if (!pingUrl.endsWith("/v2/")) {
            pingUrl += "v2/";
        }
        
        HttpURLConnection conn = (HttpURLConnection) new URL(pingUrl).openConnection();
        conn.setRequestMethod("GET");
        
        int responseCode = conn.getResponseCode();
        if (responseCode == 200) {
            return null;  // No authentication needed
        } else if (responseCode == 401) {
            String wwwAuth = conn.getHeaderField("WWW-Authenticate");
            return AuthChallenge.parse(wwwAuth);
        } else {
            throw new IOException("Unexpected response: " + responseCode);
        }
    }
}
```

#### BearerTokenExchange Implementation

```java
public class BearerTokenExchange {
    public static String getBearerToken(
            AuthChallenge challenge, 
            String identityToken, 
            List<String> scopes) throws IOException {
        
        String tokenUrl = challenge.getRealm();
        // Build request: POST /token
        // grant_type=refresh_token
        // service={service}
        // scope={scopes}
        // access_token={identityToken}
        
        HttpURLConnection conn = (HttpURLConnection) new URL(tokenUrl).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        
        String params = buildTokenRequestParams(challenge, identityToken, scopes);
        conn.setDoOutput(true);
        conn.getOutputStream().write(params.getBytes());
        
        int responseCode = conn.getResponseCode();
        if (responseCode == 200) {
            String response = readResponse(conn.getInputStream());
            return parseBearerToken(response);
        } else {
            throw new IOException("Token exchange failed: " + responseCode);
        }
    }
}
```

### Pull Image Implementation

#### Complete Pull Flow

```java
protected PullResult doPullImageUsingOciRegistry(String imageRef, Path destination) throws IOException {
    // 1. Parse image reference
    ImageReference ref = ImageReference.parse(imageRef);
    String repo = this.repository != null ? this.repository : ref.getRepository();
    String reference = ref.getReference();
    
    // 2. Get registry endpoint and auth
    String registryEndpoint = getRegistryEndpoint();
    OciRegistryClient.AuthProvider authProvider = createAuthProvider();
    
    // 3. Create OCI Registry client
    OciRegistryClient client = new OciRegistryClient(registryEndpoint, repo, authProvider);
    
    try {
        // 4. Fetch manifest
        OciRegistryClient.Manifest manifest = client.fetchManifest(reference);
        
        // 5. Handle multi-arch image index
        if (manifest.isIndex()) {
            String selectedDigest = selectPlatform(manifest.getManifests());
            manifest = client.fetchManifest(selectedDigest);
        }
        
        // 6. Download config
        String configJson = downloadConfig(client, manifest.configDigest);
        
        // 7. Build Docker tar file
        DockerTarBuilder tarBuilder = new DockerTarBuilder(destination);
        try {
            tarBuilder.addVersion();
            tarBuilder.addConfig(configId, configJson);
            
            // 8. Download and add each layer
            for (String layerDigest : manifest.layerDigests) {
                try (InputStream layerStream = client.downloadBlob(layerDigest)) {
                    tarBuilder.addLayer(layerId, layerStream);
                }
            }
            
            tarBuilder.addManifest(buildManifestJson(repo, reference, manifest));
        } finally {
            tarBuilder.close();
        }
        
        // 9. Return result
        return PullResult.builder()
            .metadata(ImageMetadata.builder()
                .digest(manifest.configDigest)
                .tag(ref.isDigest() ? null : reference)
                .build())
            .savedPath(destination.toString())
            .build();
    } finally {
        client.close();
    }
}
```

#### Lazy Loading Implementation

```java
protected PullResult doPullImageUsingOciRegistry(String imageRef) throws IOException {
    // Create pipe for streaming
    PipedInputStream pipedInputStream = new PipedInputStream(1024 * 1024);
    PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);
    
    // Start background thread to build tar
    Thread writerThread = new Thread(() -> {
        try {
            // Fetch manifest, download layers, build tar
            DockerTarBuilder tarBuilder = new DockerTarBuilder(pipedOutputStream, false);
            // ... build tar ...
            tarBuilder.close();
            pipedOutputStream.close();
        } catch (IOException e) {
            // Handle error
        }
    }, "ImagePull-StreamWriter");
    writerThread.setDaemon(true);
    writerThread.start();
    
    // Return immediately with InputStream
    return PullResult.builder()
        .metadata(metadata)
        .inputStream(pipedInputStream)
        .build();
}
```

**Key Points**:
- Background thread builds tar file
- Data streams on-demand as `InputStream` is read
- No data is downloaded until `InputStream.read()` is called
- Similar to go-containerregistry's lazy loading

### Multi-Architecture Support

```java
// 1. Fetch manifest with Accept header for index
Manifest manifest = client.fetchManifest(reference);

// 2. Check if it's an image index
if (manifest.isIndex()) {
    // 3. Select platform (default: first manifest)
    JsonArray manifests = manifest.getManifests();
    JsonObject firstManifest = manifests.get(0).getAsJsonObject();
    String selectedDigest = firstManifest.getAsJsonObject("digest").getAsString();
    
    // 4. Fetch specific platform manifest
    manifest = client.fetchManifest(selectedDigest);
}

// 5. Continue with regular image manifest
```

**Accept Header**:
```
application/vnd.oci.image.index.v1+json,
application/vnd.docker.distribution.manifest.list.v2+json,
application/vnd.oci.image.manifest.v1+json,
application/vnd.docker.distribution.manifest.v2+json
```

**Future Enhancement**: Support platform selection (os/arch) instead of defaulting to first manifest.

### Docker Tar Format

The `DockerTarBuilder` creates standard Docker image tar files:

```
image.tar
├── VERSION                    # Legacy Docker format version
├── manifest.json              # Docker manifest
├── sha256_abc123/
│   ├── json                   # Config file
│   └── layer.tar              # Layer data
└── sha256_def456/
    └── layer.tar              # Layer data
```

**manifest.json Format**:
```json
[{
  "Config": "sha256_abc123.json",
  "RepoTags": ["my-repo:latest"],
  "Layers": [
    "sha256_abc123/layer.tar",
    "sha256_def456/layer.tar"
  ]
}]
```

### Error Handling

- **401 Unauthorized**: Automatic retry with fresh auth token
- **404 Not Found**: Throws `ResourceNotFoundException`
- **Network Errors**: Retry with configurable retry policy
- **Provider Exceptions**: Mapped to MultiCloudJ common exceptions

### Performance Optimizations

- **Streaming Downloads**: Layers downloaded as streams, not buffered in memory
- **Single-Pass Processing**: Tar file built in one pass
- **Lazy Loading**: Data downloaded only when needed (InputStream mode)
- **Future**: Parallel layer downloads, layer caching

### Provider Discovery

Uses Java ServiceLoader pattern:
- `@AutoService(AbstractRegistry.class)` annotation
- Registered in `META-INF/services/com.salesforce.multicloudj.registry.driver.AbstractRegistry`
- Loaded dynamically at runtime based on `providerId`

## Summary

The MultiCloudJ Registry module provides:

✅ **Unified API** across AWS ECR, GCP GAR, and Alibaba ACR  
✅ **Multiple Pull Modes** including lazy loading  
✅ **Dynamic Authentication** with automatic discovery  
✅ **Bearer Token Support** for OAuth2-based registries  
✅ **Multi-Arch Support** for platform-specific images  
✅ **Docker Tar Format** compatible with `docker load`  
✅ **Consistent Design** following MultiCloudJ patterns  
✅ **Minimal Provider Implementation** (only 3-4 methods)  

The design balances simplicity with functionality, ensuring developers can easily pull images from any supported cloud provider with a unified, flexible API.
