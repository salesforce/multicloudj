# MultiCloudJ Options vs go-containerregistry Options

## 设计模式对比

### go-containerregistry: Functional Options 模式

go-containerregistry 使用 **Functional Options** 模式（函数式选项模式）：

```go
// 函数签名
func Pull(src string, opt ...Option) (v1.Image, error)

// Option 是函数类型
type Option func(*Options)

// 使用方式
img, err := crane.Pull("my-image:latest",
    crane.WithAuth(auth),
    crane.WithPlatform(platform),
    crane.WithContext(ctx),
)
```

### MultiCloudJ: Builder 模式

MultiCloudJ 使用 **Builder 模式**（建造者模式）：

```java
// Builder 模式
RepositoryClient client = RepositoryClient.builder("aws")
    .withRepository("my-repo")
    .withRegion("us-east-1")
    .withEndpoint(uri)
    .withCredentialsOverrider(creds)
    .withRetryConfig(retryConfig)
    .build();
```

## MultiCloudJ Registry 的 Builder Options

### RepositoryClientBuilder 支持的选项

```java
public class RepositoryClientBuilder {
    // 基础配置
    public RepositoryClientBuilder withRepository(String repository)
    public RepositoryClientBuilder withRegion(String region)
    
    // 网络配置
    public RepositoryClientBuilder withEndpoint(URI endpoint)
    public RepositoryClientBuilder withProxyEndpoint(URI proxyEndpoint)
    
    // 认证配置
    public RepositoryClientBuilder withCredentialsOverrider(CredentialsOverrider overrider)
    
    // 重试配置
    public RepositoryClientBuilder withRetryConfig(RetryConfig retryConfig)
    
    // 构建
    public RepositoryClient build()
}
```

## MultiCloudJ Blob 的 Builder Options

### BlobBuilder 支持的选项（更丰富）

```java
public class BlobBuilder {
    // 基础配置
    public BlobBuilder withBucket(String bucket)
    public BlobBuilder withRegion(String region)
    
    // 网络配置
    public BlobBuilder withEndpoint(URI endpoint)
    public BlobBuilder withProxyEndpoint(URI proxyEndpoint)
    public BlobBuilder withMaxConnections(Integer maxConnections)
    public BlobBuilder withSocketTimeout(Duration socketTimeout)
    public BlobBuilder withIdleConnectionTimeout(Duration idleConnectionTimeout)
    
    // 认证配置
    public BlobBuilder withCredentialsOverrider(CredentialsOverrider overrider)
    
    // 重试配置
    public BlobBuilder withRetryConfig(RetryConfig retryConfig)
    
    // 构建
    public BucketClient build()
}
```

## 详细对比表

### go-containerregistry Options

| Option | 类型 | 用途 | 对应 MultiCloudJ |
|--------|------|------|------------------|
| `WithTransport` | `http.RoundTripper` | 自定义 HTTP Transport | ❌ 无直接对应（在底层实现中） |
| `Insecure` | - | 允许非 TLS 连接 | ❌ 无（通过 endpoint 配置） |
| `WithPlatform` | `*v1.Platform` | 指定平台（多架构） | ❌ 无（TODO: 未来支持） |
| `WithAuthFromKeychain` | `authn.Keychain` | 使用 Keychain 认证 | `withCredentialsOverrider` |
| `WithAuth` | `authn.Authenticator` | 直接提供认证 | `withCredentialsOverrider` |
| `WithUserAgent` | `string` | 设置 User-Agent | ❌ 无 |
| `WithNondistributable` | - | 允许非分发层 | ❌ 无 |
| `WithContext` | `context.Context` | 设置 Context（超时、取消） | ❌ 无（Java 使用 Future/CompletableFuture） |
| `WithJobs` | `int` | 设置并发任务数 | ❌ 无 |
| `WithNoClobber` | `bool` | 避免覆盖现有标签 | ❌ 无（主要用于 push） |

### MultiCloudJ Registry Builder Options

| Option | 类型 | 用途 | 对应 go-cr |
|--------|------|------|------------|
| `withRepository` | `String` | 指定 repository | 从 imageRef 解析 |
| `withRegion` | `String` | 指定 region | ❌ 无（从 imageRef 解析） |
| `withEndpoint` | `URI` | 自定义 endpoint | `WithTransport` (部分) |
| `withProxyEndpoint` | `URI` | 代理 endpoint | ❌ 无 |
| `withCredentialsOverrider` | `CredentialsOverrider` | 凭证覆盖 | `WithAuth` / `WithAuthFromKeychain` |
| `withRetryConfig` | `RetryConfig` | 重试配置 | ❌ 无（内置重试） |

### MultiCloudJ Blob Builder Options

| Option | 类型 | 用途 | 对应 go-cr |
|--------|------|------|------------|
| `withBucket` | `String` | 指定 bucket | N/A (blob 特有) |
| `withRegion` | `String` | 指定 region | N/A (blob 特有) |
| `withEndpoint` | `URI` | 自定义 endpoint | `WithTransport` (部分) |
| `withProxyEndpoint` | `URI` | 代理 endpoint | ❌ 无 |
| `withMaxConnections` | `Integer` | 最大连接数 | `WithTransport` (部分) |
| `withSocketTimeout` | `Duration` | Socket 超时 | `WithContext` (部分) |
| `withIdleConnectionTimeout` | `Duration` | 空闲连接超时 | `WithTransport` (部分) |
| `withCredentialsOverrider` | `CredentialsOverrider` | 凭证覆盖 | `WithAuth` / `WithAuthFromKeychain` |
| `withRetryConfig` | `RetryConfig` | 重试配置 | ❌ 无（内置重试） |

## 使用示例对比

### go-containerregistry

```go
// 使用多个 Options
img, err := crane.Pull("gcr.io/project/image:tag",
    crane.WithAuth(auth),
    crane.WithPlatform(&v1.Platform{
        Architecture: "arm64",
        OS:           "linux",
    }),
    crane.WithContext(ctx),
    crane.WithUserAgent("MyApp/1.0"),
)
```

### MultiCloudJ Registry

```java
// 使用 Builder 模式
RepositoryClient client = RepositoryClient.builder("gcp")
    .withRepository("project/image")
    .withRegion("us-central1")
    .withEndpoint(URI.create("https://custom-endpoint.com"))
    .withCredentialsOverrider(creds)
    .withRetryConfig(retryConfig)
    .build();

PullResult result = client.pullImage("image:tag");
```

### MultiCloudJ Blob

```java
// 使用 Builder 模式（更多选项）
BucketClient client = BucketClient.builder("aws")
    .withBucket("my-bucket")
    .withRegion("us-east-1")
    .withEndpoint(URI.create("https://s3.amazonaws.com"))
    .withProxyEndpoint(URI.create("https://proxy.com"))
    .withMaxConnections(100)
    .withSocketTimeout(Duration.ofSeconds(60))
    .withIdleConnectionTimeout(Duration.ofMinutes(10))
    .withCredentialsOverrider(creds)
    .withRetryConfig(retryConfig)
    .build();
```

## 设计模式差异

### Functional Options 模式（go-containerregistry）

**优点：**
- ✅ 灵活：可以传递任意数量的选项
- ✅ 可扩展：添加新选项不影响现有代码
- ✅ 函数式：符合 Go 的编程风格
- ✅ 可选参数：所有参数都是可选的

**缺点：**
- ❌ 类型安全：运行时检查，编译时无法验证
- ❌ 文档：需要查看函数签名才能知道有哪些选项

### Builder 模式（MultiCloudJ）

**优点：**
- ✅ 类型安全：编译时检查，IDE 自动补全
- ✅ 可读性：方法名清晰表达意图
- ✅ 链式调用：流畅的 API
- ✅ 不可变：build() 后对象不可变

**缺点：**
- ❌ 代码量：需要为每个选项写方法
- ❌ 扩展性：添加新选项需要修改 Builder 类

## 功能覆盖对比

### go-containerregistry 有但 MultiCloudJ 没有的

1. **WithPlatform** - 多架构镜像平台选择
   - go-cr: ✅ 支持
   - MultiCloudJ: ❌ 不支持（TODO: 未来支持）

2. **WithContext** - 超时和取消
   - go-cr: ✅ 支持（context.Context）
   - MultiCloudJ: ❌ 不支持（Java 使用 Future/CompletableFuture）

3. **WithUserAgent** - 自定义 User-Agent
   - go-cr: ✅ 支持
   - MultiCloudJ: ❌ 不支持

4. **WithJobs** - 并发任务数
   - go-cr: ✅ 支持
   - MultiCloudJ: ❌ 不支持

### MultiCloudJ 有但 go-containerregistry 没有的

1. **withRetryConfig** - 详细的重试配置
   - go-cr: ❌ 无（内置简单重试）
   - MultiCloudJ: ✅ 支持（RetryConfig 对象）

2. **withProxyEndpoint** - 代理 endpoint
   - go-cr: ❌ 无
   - MultiCloudJ: ✅ 支持

3. **withMaxConnections** - 最大连接数（blob）
   - go-cr: ❌ 无
   - MultiCloudJ: ✅ 支持（blob 模块）

4. **withSocketTimeout** - Socket 超时（blob）
   - go-cr: ❌ 无（通过 context）
   - MultiCloudJ: ✅ 支持（blob 模块）

5. **withIdleConnectionTimeout** - 空闲连接超时（blob）
   - go-cr: ❌ 无
   - MultiCloudJ: ✅ 支持（blob 模块）

## 总结

### 相同点

✅ 都支持配置认证  
✅ 都支持自定义 endpoint  
✅ 都使用可选参数模式  

### 不同点

| 特性 | go-containerregistry | MultiCloudJ |
|------|---------------------|-------------|
| **模式** | Functional Options | Builder Pattern |
| **类型安全** | 运行时检查 | 编译时检查 |
| **平台选择** | ✅ 支持 | ❌ 不支持（TODO） |
| **超时控制** | ✅ Context | ❌ 无（Java Future） |
| **重试配置** | ❌ 简单内置 | ✅ 详细配置 |
| **连接池配置** | ❌ 无 | ✅ 支持（blob） |
| **代理支持** | ❌ 无 | ✅ 支持 |

### 设计理念

- **go-containerregistry**: 函数式、灵活、简洁
- **MultiCloudJ**: 面向对象、类型安全、详细配置

两种设计都合理，符合各自语言的编程习惯！
