# MultiCloudJ Registry Options

## Registry Builder Options

MultiCloudJ Registry 使用 **Builder 模式**提供配置选项，类似于 go-containerregistry 的 Options，但实现方式不同。

### RepositoryClientBuilder 支持的选项

```java
RepositoryClient client = RepositoryClient.builder("aws")
    .withRepository("my-repo")              // ✅ 指定 repository
    .withRegion("us-east-1")                 // ✅ 指定 region
    .withEndpoint(URI.create("https://...")) // ✅ 自定义 endpoint
    .withProxyEndpoint(URI.create("https://...")) // ✅ 代理 endpoint
    .withCredentialsOverrider(creds)         // ✅ 凭证覆盖
    .withRetryConfig(retryConfig)            // ✅ 重试配置
    .build();
```

### 详细选项列表

| Option | 类型 | 用途 | 是否必需 | 说明 |
|--------|------|------|---------|------|
| `withRepository` | `String` | 指定 repository 名称 | ❌ 可选 | 如果不指定，从 imageRef 解析 |
| `withRegion` | `String` | 指定 region | ✅ 必需 | 不同 provider 需要不同的 region |
| `withEndpoint` | `URI` | 自定义 endpoint | ❌ 可选 | 覆盖默认的 registry endpoint |
| `withProxyEndpoint` | `URI` | 代理 endpoint | ❌ 可选 | 用于代理场景 |
| `withCredentialsOverrider` | `CredentialsOverrider` | 凭证覆盖 | ❌ 可选 | 覆盖默认凭证 |
| `withRetryConfig` | `RetryConfig` | 重试配置 | ❌ 可选 | 配置重试策略 |

### AbstractRegistry.Builder 支持的选项（底层）

```java
// AbstractRegistry.Builder 还支持：
public B withProperties(Properties properties)  // ✅ 通用属性配置
```

**注意**: `withProperties` 在 `AbstractRegistry.Builder` 中存在，但 `RepositoryClientBuilder` 没有暴露这个方法。

## 对比：Registry vs Blob Options

### Registry Options（较少）

```java
RepositoryClient.builder("aws")
    .withRepository("my-repo")           // ✅
    .withRegion("us-east-1")             // ✅
    .withEndpoint(uri)                   // ✅
    .withProxyEndpoint(uri)               // ✅
    .withCredentialsOverrider(creds)      // ✅
    .withRetryConfig(retryConfig)        // ✅
    .build();
```

### Blob Options（更多）

```java
BucketClient.builder("aws")
    .withBucket("my-bucket")             // ✅
    .withRegion("us-east-1")             // ✅
    .withEndpoint(uri)                   // ✅
    .withProxyEndpoint(uri)               // ✅
    .withMaxConnections(100)              // ✅ blob 特有
    .withSocketTimeout(Duration.ofSeconds(60))  // ✅ blob 特有
    .withIdleConnectionTimeout(Duration.ofMinutes(10))  // ✅ blob 特有
    .withCredentialsOverrider(creds)     // ✅
    .withRetryConfig(retryConfig)        // ✅
    .build();
```

## 对比：Registry vs go-containerregistry Options

### go-containerregistry Options

```go
img, err := crane.Pull("my-image:latest",
    crane.WithAuth(auth),                    // ✅ 认证
    crane.WithPlatform(platform),             // ✅ 平台选择
    crane.WithContext(ctx),                  // ✅ 超时/取消
    crane.WithUserAgent("MyApp/1.0"),        // ✅ User-Agent
    crane.WithTransport(transport),           // ✅ Transport
    crane.Insecure,                          // ✅ 非 TLS
    crane.WithJobs(8),                       // ✅ 并发数
)
```

### MultiCloudJ Registry Options

```java
RepositoryClient client = RepositoryClient.builder("aws")
    .withRepository("my-repo")              // ✅ repository
    .withRegion("us-east-1")                 // ✅ region
    .withEndpoint(uri)                       // ✅ endpoint
    .withProxyEndpoint(uri)                  // ✅ 代理
    .withCredentialsOverrider(creds)        // ✅ 认证（对应 WithAuth）
    .withRetryConfig(retryConfig)            // ✅ 重试（go-cr 没有）
    .build();
```

## 功能覆盖对比

| 功能 | go-containerregistry | MultiCloudJ Registry | MultiCloudJ Blob |
|------|---------------------|---------------------|------------------|
| **认证** | ✅ `WithAuth` | ✅ `withCredentialsOverrider` | ✅ `withCredentialsOverrider` |
| **Endpoint** | ✅ `WithTransport` | ✅ `withEndpoint` | ✅ `withEndpoint` |
| **代理** | ❌ 无 | ✅ `withProxyEndpoint` | ✅ `withProxyEndpoint` |
| **重试** | ❌ 无（内置） | ✅ `withRetryConfig` | ✅ `withRetryConfig` |
| **平台选择** | ✅ `WithPlatform` | ❌ 无（TODO） | N/A |
| **超时/取消** | ✅ `WithContext` | ❌ 无 | ✅ `withSocketTimeout` |
| **User-Agent** | ✅ `WithUserAgent` | ❌ 无 | ❌ 无 |
| **并发数** | ✅ `WithJobs` | ❌ 无 | ❌ 无 |
| **连接池** | ❌ 无 | ❌ 无 | ✅ `withMaxConnections` |
| **空闲超时** | ❌ 无 | ❌ 无 | ✅ `withIdleConnectionTimeout` |

## 使用示例

### 基础用法

```java
// 最简单的用法（只指定必需参数）
RepositoryClient client = RepositoryClient.builder("aws")
    .withRegion("us-east-1")
    .build();

PullResult result = client.pullImage("my-image:latest", Paths.get("image.tar"));
```

### 完整配置

```java
// 完整配置示例
RetryConfig retryConfig = RetryConfig.builder()
    .maxRetries(3)
    .initialRetryDelay(Duration.ofSeconds(1))
    .build();

CredentialsOverrider creds = new CredentialsOverrider.Builder(CredentialsType.SESSION)
    .withSessionCredentials(sessionCreds)
    .build();

RepositoryClient client = RepositoryClient.builder("gcp")
    .withRepository("my-project/my-repo")
    .withRegion("us-central1")
    .withEndpoint(URI.create("https://custom-endpoint.com"))
    .withProxyEndpoint(URI.create("https://proxy.example.com"))
    .withCredentialsOverrider(creds)
    .withRetryConfig(retryConfig)
    .build();

PullResult result = client.pullImage("my-image:latest", Paths.get("image.tar"));
```

## 总结

### Registry 有 Options 吗？

**✅ 有！** MultiCloudJ Registry 使用 **Builder 模式**提供配置选项：

1. **基础配置**:
   - `withRepository` - Repository 名称
   - `withRegion` - Region（必需）

2. **网络配置**:
   - `withEndpoint` - 自定义 endpoint
   - `withProxyEndpoint` - 代理 endpoint

3. **认证配置**:
   - `withCredentialsOverrider` - 凭证覆盖

4. **重试配置**:
   - `withRetryConfig` - 重试策略

### 与 go-containerregistry 的对比

- **相同点**: 都支持认证、endpoint 配置
- **不同点**: 
  - go-cr 有平台选择、Context 超时、User-Agent
  - MultiCloudJ 有重试配置、代理支持
  - MultiCloudJ 使用 Builder 模式（类型安全）
  - go-cr 使用 Functional Options（更灵活）

### 与 Blob 的对比

- **Registry 选项较少**: 6 个选项
- **Blob 选项较多**: 9 个选项（多了连接池、超时配置）

这是因为 Registry 主要关注镜像拉取，而 Blob 需要更细粒度的网络配置。
