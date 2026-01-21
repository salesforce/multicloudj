# Platform Selection and Image Object in go-containerregistry

## Platform Selection (平台选择)

### What is Platform Selection?

**Platform Selection** is the ability to choose a specific **operating system and architecture** from a **multi-architecture image** (also called an "image index" or "manifest list").

### Multi-Architecture Images

A multi-architecture image contains multiple platform-specific images:

```
Image Index (manifest list)
├── linux/amd64    → Image for Linux on x86_64
├── linux/arm64    → Image for Linux on ARM64
├── windows/amd64  → Image for Windows on x86_64
└── darwin/arm64   → Image for macOS on Apple Silicon
```

### Platform Structure

```go
type Platform struct {
    Architecture string   // e.g., "amd64", "arm64"
    OS           string   // e.g., "linux", "windows", "darwin"
    OSVersion    string   // e.g., "10.0.19041.388"
    OSFeatures   []string // e.g., ["win32k"]
    Variant      string   // e.g., "v7", "v8"
}
```

**Examples**:
- `linux/amd64` - Linux on x86_64
- `linux/arm64` - Linux on ARM64
- `windows/amd64` - Windows on x86_64
- `darwin/arm64` - macOS on Apple Silicon

### How go-containerregistry Uses Platform Selection

```go
// Without platform selection (default: linux/amd64)
img, err := crane.Pull("my-image:latest")
// If image is multi-arch, automatically selects linux/amd64

// With platform selection
platform := &v1.Platform{
    OS:           "linux",
    Architecture: "arm64",
}
img, err := crane.Pull("my-image:latest", crane.WithPlatform(platform))
// Explicitly selects linux/arm64 from multi-arch image
```

### Why Platform Selection is Needed

1. **Cross-Platform Development**: Build images for multiple platforms
2. **Target Specific Platform**: Pull the correct image for your target platform
3. **CI/CD**: Build and test on different architectures

**Example Scenario**:
```go
// Building on Mac (darwin/arm64) but targeting Linux (linux/amd64)
platform := &v1.Platform{
    OS:           "linux",
    Architecture: "amd64",
}
img, err := crane.Pull("my-app:latest", crane.WithPlatform(platform))
// Pulls the Linux x86_64 version, not the macOS version
```

### MultiCloudJ Registry Status

**Current Implementation**:
```java
// Currently defaults to first manifest in index
if (manifest.isIndex()) {
    // TODO: Select platform (default to linux/amd64 for now)
    // For now, select the first manifest in the index
    JsonObject firstManifest = manifests.get(0).getAsJsonObject();
    String selectedDigest = firstManifest.getAsJsonObject("digest").getAsString();
    manifest = client.fetchManifest(selectedDigest);
}
```

**Future Enhancement**:
```java
// Future: Support platform selection
RepositoryClient client = RepositoryClient.builder("aws")
    .withRegion("us-east-1")
    .withPlatform(Platform.builder()
        .os("linux")
        .architecture("arm64")
        .build())
    .build();
```

## Context/Timeout in go-containerregistry

### What is Context?

**Context** in Go is used for:
- **Timeout**: Cancel operation after a certain time
- **Cancellation**: Cancel operation on demand
- **Deadline**: Set a deadline for operation completion

### go-containerregistry WithContext

```go
// Set timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

img, err := crane.Pull("my-image:latest", crane.WithContext(ctx))
// Operation will be cancelled after 30 seconds
```

**Use Cases**:
- Prevent hanging on slow networks
- Cancel long-running operations
- Set deadlines for operations

### MultiCloudJ Registry: Does it have Timeout?

**Registry Module**: ❌ **No timeout support currently**

**Blob Module**: ✅ **Has timeout support**
```java
BucketClient.builder("aws")
    .withSocketTimeout(Duration.ofSeconds(60))  // ✅ Socket timeout
    .withIdleConnectionTimeout(Duration.ofMinutes(10))  // ✅ Idle timeout
    .build();
```

**Why Registry doesn't have timeout?**
- Registry operations are typically faster (just metadata + layers)
- Can be added in the future if needed
- Java uses `Future`/`CompletableFuture` for async operations (different pattern)

**Future Enhancement**:
```java
// Future: Add timeout support
RepositoryClient client = RepositoryClient.builder("aws")
    .withRegion("us-east-1")
    .withTimeout(Duration.ofSeconds(30))  // TODO
    .build();
```

## Image Object in go-containerregistry

### What is v1.Image?

`v1.Image` is a **structured, in-memory object** that represents a container image. It's **NOT a tar file** - it's an interface with methods to access image components.

### v1.Image Interface

```go
type Image interface {
    // Access layers
    Layers() ([]Layer, error)
    LayerByDigest(Hash) (Layer, error)
    LayerByDiffID(Hash) (Layer, error)
    
    // Access config
    ConfigFile() (*ConfigFile, error)
    RawConfigFile() ([]byte, error)
    ConfigName() (Hash, error)
    
    // Access manifest
    Manifest() (*Manifest, error)
    RawManifest() ([]byte, error)
    
    // Metadata
    MediaType() (types.MediaType, error)
    Size() (int64, error)
    Digest() (Hash, error)
}
```

### Do Users Need to Access Components?

#### Common Use Case: Pull and Save (Simple)

**Most users** just want to pull and save:

```go
// Step 1: Pull image
img, err := crane.Pull("my-image:latest")
if err != nil {
    log.Fatal(err)
}

// Step 2: Save to tar
err = crane.Save(img, "tag", "image.tar")
if err != nil {
    log.Fatal(err)
}
```

**This is the most common use case** - users don't need to access layers/config/manifest.

#### Advanced Use Case: Image Manipulation

**Some users** need to access components for manipulation:

```go
// Pull image
img, err := crane.Pull("my-image:latest")

// Access config to modify
config, _ := img.ConfigFile()
config.Config.Env = append(config.Config.Env, "NEW_VAR=value")

// Create new image with modified config
newImg, _ := mutate.Config(img, config)

// Access layers to extract files
layers, _ := img.Layers()
layer := layers[0]
reader, _ := layer.Uncompressed()
// Extract files from layer...

// Access manifest to inspect
manifest, _ := img.Manifest()
fmt.Println("Layers:", len(manifest.Layers))
```

**Use Cases for Component Access**:
- ✅ **Image Mutation**: Modify config, add/remove layers
- ✅ **File Extraction**: Extract files from layers
- ✅ **Image Inspection**: Inspect image structure
- ✅ **Custom Processing**: Process layers individually
- ✅ **Image Comparison**: Compare images

### Why go-containerregistry Returns Image Object?

**Design Philosophy**:
- **Flexibility First**: Return structured object for maximum flexibility
- **Lazy Loading**: Components loaded on-demand (not all at once)
- **Manipulation Support**: Enable image mutation and processing
- **Separation of Concerns**: Pull (get image) vs Save (write tar) are separate

**Trade-off**:
- ✅ High flexibility for advanced users
- ❌ Two-step process for simple use case (Pull + Save)

### MultiCloudJ Registry Approach

**Design Philosophy**:
- **Simplicity First**: Direct tar file output for common use case
- **One-Step**: Pull and save in single operation
- **Focus on Download**: Optimized for downloading images

**Trade-off**:
- ✅ Simple for common use case (one-step)
- ❌ No component access for advanced use cases

## Summary

### Platform Selection

| Aspect | go-containerregistry | MultiCloudJ Registry |
|--------|---------------------|---------------------|
| **Support** | ✅ Yes (`WithPlatform`) | ❌ No (TODO: future) |
| **Default** | `linux/amd64` | First manifest in index |
| **Use Case** | Cross-platform development | N/A (not supported yet) |

### Context/Timeout

| Aspect | go-containerregistry | MultiCloudJ Registry | MultiCloudJ Blob |
|--------|---------------------|---------------------|------------------|
| **Support** | ✅ Yes (`WithContext`) | ❌ No | ✅ Yes (`withSocketTimeout`) |
| **Mechanism** | `context.Context` | N/A | `Duration` |
| **Use Case** | Timeout, cancellation | N/A | Network timeout |

### Image Object vs Tar File

| Aspect | go-containerregistry | MultiCloudJ Registry |
|--------|---------------------|---------------------|
| **Return Type** | `v1.Image` (structured object) | `PullResult` (tar wrapper) |
| **Common Use** | Pull → Save (two steps) | Pull (one step, saves tar) |
| **Advanced Use** | Access components for manipulation | ❌ Not supported |
| **Flexibility** | High | Medium |
| **Simplicity** | Medium | High |

### User Needs

**Most Users (80-90%)**:
- Just want to pull and save image
- Don't need component access
- Prefer simple, one-step API

**Advanced Users (10-20%)**:
- Need image manipulation
- Need component access
- Prefer flexible, structured API

**Conclusion**: Both designs are valid - go-containerregistry prioritizes flexibility, MultiCloudJ prioritizes simplicity for the common use case.
