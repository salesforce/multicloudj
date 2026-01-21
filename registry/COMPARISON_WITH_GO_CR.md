# Comparison: MultiCloudJ Registry vs go-containerregistry

## Executive Summary

While both libraries provide container image pulling capabilities, they have **fundamental design differences**:

- **go-containerregistry**: Returns structured `v1.Image` objects for manipulation
- **MultiCloudJ Registry**: Returns tar files/streams for direct download

## API Signature Comparison

### go-containerregistry Pull API

```go
// Function signature
func Pull(src string, opt ...Option) (v1.Image, error)

// Input:
//   - src: string - Image reference
//     Examples: "gcr.io/project/image:tag", "ubuntu:latest"
//   - opt: ...Option - Optional configuration
//     - WithAuth(authn.Authenticator)
//     - WithPlatform(platform)
//     - WithTransport(transport)
//     - etc.

// Output:
//   - v1.Image: Structured image object (interface)
//   - error: Error if pull fails

// Example:
img, err := crane.Pull("gcr.io/project/image:tag")
if err != nil {
    log.Fatal(err)
}
// img is a v1.Image - NOT a tar file
```

### MultiCloudJ Registry Pull API

```java
// Method signatures (multiple overloads)
public PullResult pullImage(String imageRef, Path destination)
public PullResult pullImage(String imageRef, OutputStream outputStream)
public PullResult pullImage(String imageRef, File file)
public PullResult pullImage(String imageRef)  // Lazy loading

// Input:
//   - imageRef: String - Image reference
//     Examples: "my-image:latest", "my-image@sha256:abc123"
//   - destination: Optional - Where to save tar file
//     - Path: Save to file system
//     - File: Save to file
//     - OutputStream: Write to stream
//     - None: Return InputStream for lazy loading

// Output:
//   - PullResult: Wrapper object containing:
//     - metadata: ImageMetadata (digest, tag)
//     - savedPath: String (if saved to file, null otherwise)
//     - inputStream: InputStream (if lazy loading, null otherwise)

// Example:
PullResult result = client.pullImage("my-image:latest", Paths.get("image.tar"));
// Tar file is built and saved automatically
```

## Detailed Comparison Table

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

## Return Type Comparison

### go-containerregistry: v1.Image Interface

```go
type Image interface {
    // Access layers
    Layers() ([]Layer, error)
    LayerByDigest(hash.Hash) (Layer, error)
    LayerByDiffID(hash.Hash) (Layer, error)
    
    // Access config
    ConfigFile() (*ConfigFile, error)
    RawConfigFile() ([]byte, error)
    ConfigName() (hash.Hash, error)
    
    // Access manifest
    Manifest() (*Manifest, error)
    RawManifest() ([]byte, error)
    
    // Metadata
    MediaType() (types.MediaType, error)
    Size() (int64, error)
    Digest() (hash.Hash, error)
}
```

**Characteristics:**
- Structured object with methods
- Components accessible individually
- Lazy-loaded (data fetched on-demand)
- NOT a tar file
- Requires `crane.Save()` to create tar

### MultiCloudJ Registry: PullResult Class

```java
@Builder
@Getter
public class PullResult {
    private final ImageMetadata metadata;  // digest, tag
    private final String savedPath;        // Path to tar file (if saved)
    private final InputStream inputStream; // Tar stream (if lazy loading)
}

@Builder
@Getter
public class ImageMetadata {
    private final String digest;  // Image digest
    private final String tag;    // Image tag (null if pulled by digest)
}
```

**Characteristics:**
- Wrapper object with metadata
- Tar file/stream included
- Components NOT directly accessible
- Lazy-loaded (tar streamed on-demand)
- Tar file built automatically

## Workflow Comparison

### go-containerregistry Workflow

```go
// Step 1: Pull image (returns structured object)
img, err := crane.Pull("gcr.io/project/image:tag")
if err != nil {
    log.Fatal(err)
}

// Step 2: Access components (if needed)
layers, _ := img.Layers()
config, _ := img.ConfigFile()
manifest, _ := img.Manifest()

// Step 3: Manipulate image (optional)
newImg, _ := mutate.Config(img, newConfig)
newImg, _ := mutate.AppendLayers(newImg, newLayer)

// Step 4: Save to tar (separate step, if needed)
err = crane.Save(img, "tag", "image.tar")
```

**Key Points:**
- Two-step process: Pull → Save
- Image object is structured and manipulatable
- Components accessible before saving
- Tar file creation is separate operation

### MultiCloudJ Registry Workflow

#### Option 1: Immediate Save (One-Step)

```java
// Pull and save in one step
PullResult result = client.pullImage(
    "my-image:latest",
    Paths.get("/tmp/image.tar")
);

System.out.println("Saved to: " + result.getSavedPath());
System.out.println("Digest: " + result.getMetadata().getDigest());
```

**Key Points:**
- One-step process: Pull + Save
- Tar file built automatically
- No access to individual components
- Simple and direct

#### Option 2: Lazy Loading (Similar to go-cr)

```java
// Pull and get InputStream (lazy loading)
PullResult result = client.pullImage("my-image:latest");
InputStream is = result.getInputStream();

// Data streams on-demand as InputStream is read
try (is) {
    byte[] buffer = new byte[8192];
    int bytesRead;
    while ((bytesRead = is.read(buffer)) != -1) {
        // Process data as it streams...
        processChunk(buffer, bytesRead);
    }
}
// Tar file is built in background thread as InputStream is read
```

**Key Points:**
- Similar lazy loading to go-cr
- Tar file built in background
- Data streams on-demand
- No access to individual components

## Design Philosophy Comparison

| Aspect | go-containerregistry | MultiCloudJ Registry |
|--------|---------------------|---------------------|
| **Primary Use Case** | Image manipulation (mutate, extract, inspect) | Image download (pull to tar) |
| **Flexibility** | High (structured access) | Medium (tar file focus) |
| **Simplicity** | Medium (need to understand Image interface) | High (direct tar file) |
| **Language Idioms** | Go interfaces and lazy loading | Java streams and builders |
| **API Design** | Functional (returns object) | Imperative (saves to destination) |

## Use Case Scenarios

### When to Use go-containerregistry

✅ **Best for:**
- Image manipulation (mutate, extract, rebase)
- Inspecting image components (layers, config, manifest)
- Building custom image processing pipelines
- When you need structured access to image components

❌ **Not ideal for:**
- Simple download-to-tar scenarios (requires two steps)
- When you only need tar file output

### When to Use MultiCloudJ Registry

✅ **Best for:**
- Simple image download to tar file
- Cross-cloud image pulling (AWS ECR, GCP GAR, Alibaba ACR)
- When you need one-step pull-and-save
- Java applications requiring container image download

❌ **Not ideal for:**
- Image manipulation (mutate, extract, rebase)
- Inspecting individual image components
- When you need structured access to layers/config/manifest

## Implementation Differences

### go-containerregistry Implementation

```go
// Pull returns remoteImage struct that implements v1.Image
func Pull(src string, opt ...Option) (v1.Image, error) {
    ref, err := name.ParseReference(src, o.Name...)
    if err != nil {
        return nil, err
    }
    return remote.Image(ref, o.Remote...)
}

// remote.Image returns *remoteImage
// remoteImage implements v1.Image interface
// Components are lazy-loaded (fetched on-demand)
```

**Key Implementation Details:**
- Returns `*remoteImage` struct
- Implements `v1.Image` interface
- Components fetched lazily (on method call)
- No tar file building in Pull operation

### MultiCloudJ Registry Implementation

```java
// pullImage builds tar file and returns PullResult
public PullResult pullImage(String imageRef, Path destination) {
    // 1. Parse image reference
    // 2. Fetch manifest
    // 3. Download config and layers
    // 4. Build Docker tar file
    // 5. Save to destination
    // 6. Return PullResult with metadata
}

// Lazy loading mode uses PipedInputStream/PipedOutputStream
public PullResult pullImage(String imageRef) {
    // 1. Create PipedInputStream/PipedOutputStream
    // 2. Start background thread to build tar
    // 3. Return PullResult with InputStream immediately
    // 4. Tar built in background as InputStream is read
}
```

**Key Implementation Details:**
- Builds tar file automatically
- Returns `PullResult` wrapper
- Components NOT accessible (only tar stream)
- Lazy loading uses background thread + PipedStream

## Summary

### Similarities

✅ Both support lazy loading  
✅ Both use OCI Registry API v2  
✅ Both handle authentication automatically  
✅ Both support multi-arch images  

### Differences

| Feature | go-containerregistry | MultiCloudJ Registry |
|---------|---------------------|---------------------|
| **Return Type** | `v1.Image` (structured) | `PullResult` (tar wrapper) |
| **Input** | Image ref only | Image ref + optional destination |
| **Output** | Image object | Tar file/stream |
| **Components** | Accessible | Not accessible |
| **Tar File** | Separate Save() call | Built automatically |
| **Use Case** | Image manipulation | Image download |

### Conclusion

**go-containerregistry** and **MultiCloudJ Registry** serve different purposes:

- **go-containerregistry**: Designed for **image manipulation** - returns structured objects for mutation, extraction, and inspection
- **MultiCloudJ Registry**: Designed for **image download** - returns tar files/streams for direct use

Both are valid designs, optimized for their respective use cases. MultiCloudJ Registry prioritizes simplicity and direct tar file output, while go-containerregistry prioritizes flexibility and structured access.
