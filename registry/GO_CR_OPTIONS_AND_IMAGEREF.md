# go-containerregistry Options and Image Reference Parsing

## go-containerregistry 的 Option 类型

### Available Options

go-containerregistry 的 `Pull(src string, opt ...Option)` 支持以下 Option：

#### 1. **WithTransport** - 自定义 HTTP Transport
```go
func WithTransport(t http.RoundTripper) Option
```
**用途**: 覆盖默认的 HTTP transport
**示例**:
```go
transport := &http.Transport{
    TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}
img, err := crane.Pull("my-image:latest", crane.WithTransport(transport))
```

#### 2. **Insecure** - 允许非 TLS 连接
```go
func Insecure(o *Options)
```
**用途**: 允许使用 HTTP（非 HTTPS）连接，允许自签名证书
**示例**:
```go
img, err := crane.Pull("localhost:5000/my-image:latest", crane.Insecure)
```

#### 3. **WithPlatform** - 指定平台（多架构镜像）
```go
func WithPlatform(platform *v1.Platform) Option
```
**用途**: 从多架构镜像中选择特定平台
**示例**:
```go
platform := &v1.Platform{
    Architecture: "arm64",
    OS:           "linux",
}
img, err := crane.Pull("my-image:latest", crane.WithPlatform(platform))
```

#### 4. **WithAuthFromKeychain** - 使用 Keychain 认证
```go
func WithAuthFromKeychain(keys authn.Keychain) Option
```
**用途**: 使用自定义 Keychain 来查找凭证
**示例**:
```go
keychain := authn.NewMultiKeychain(
    authn.DefaultKeychain,
    google.Keychain,
)
img, err := crane.Pull("gcr.io/project/image:tag", 
    crane.WithAuthFromKeychain(keychain))
```

#### 5. **WithAuth** - 直接提供 Authenticator
```go
func WithAuth(auth authn.Authenticator) Option
```
**用途**: 直接提供认证信息
**示例**:
```go
auth := &authn.Basic{
    Username: "user",
    Password: "pass",
}
img, err := crane.Pull("my-image:latest", crane.WithAuth(auth))
```

#### 6. **WithUserAgent** - 设置 User-Agent
```go
func WithUserAgent(ua string) Option
```
**用途**: 设置 HTTP 请求的 User-Agent 头
**示例**:
```go
img, err := crane.Pull("my-image:latest", 
    crane.WithUserAgent("MyApp/1.0"))
```

#### 7. **WithNondistributable** - 允许非分发层
```go
func WithNondistributable() Option
```
**用途**: 允许推送/拉取非分发层（foreign layers）
**示例**:
```go
img, err := crane.Pull("my-image:latest", crane.WithNondistributable())
```

#### 8. **WithContext** - 设置 Context
```go
func WithContext(ctx context.Context) Option
```
**用途**: 设置请求的 context（用于取消、超时等）
**示例**:
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
img, err := crane.Pull("my-image:latest", crane.WithContext(ctx))
```

#### 9. **WithJobs** - 设置并发任务数
```go
func WithJobs(jobs int) Option
```
**用途**: 设置并发下载的层数（默认是 GOMAXPROCS）
**示例**:
```go
img, err := crane.Pull("my-image:latest", crane.WithJobs(8))
```

#### 10. **WithNoClobber** - 避免覆盖现有标签
```go
func WithNoClobber(noclobber bool) Option
```
**用途**: 避免覆盖已存在的标签（主要用于 push 操作）
**示例**:
```go
img, err := crane.Pull("my-image:latest", crane.WithNoClobber(true))
```

### Options 结构

```go
type Options struct {
    Name      []name.Option    // name 包的选项（解析相关）
    Remote    []remote.Option  // remote 包的选项（网络相关）
    Platform  *v1.Platform     // 平台选择
    Keychain  authn.Keychain   // 认证 Keychain
    Transport http.RoundTripper // HTTP Transport
    
    auth      authn.Authenticator
    insecure  bool
    jobs      int
    noclobber bool
    ctx       context.Context
}
```

## 为什么 go-containerregistry 只需要 src string？

### Image Reference 格式

go-containerregistry 的 `src` 参数就是完整的 image reference，格式如下：

```
[registry/][repository/]image[:tag|@digest]
```

### 解析示例

go-containerregistry 使用 `name.ParseReference()` 来解析 image reference，可以自动提取所有信息：

#### 示例 1: 完整格式
```go
src := "gcr.io/my-project/my-image:v1.0"
// 解析后：
//   Registry:   "gcr.io"
//   Repository: "my-project/my-image"
//   Tag:        "v1.0"
```

#### 示例 2: Docker Hub 格式（自动补全）
```go
src := "ubuntu:latest"
// 解析后（自动补全）：
//   Registry:   "index.docker.io" (默认)
//   Repository: "library/ubuntu" (library 是默认命名空间)
//   Tag:        "latest" (默认)
// 实际等价于: "index.docker.io/library/ubuntu:latest"
```

#### 示例 3: 使用 Digest
```go
src := "gcr.io/project/image@sha256:abc123..."
// 解析后：
//   Registry:   "gcr.io"
//   Repository: "project/image"
//   Digest:     "sha256:abc123..."
```

#### 示例 4: 只有镜像名（全部使用默认值）
```go
src := "nginx"
// 解析后（自动补全）：
//   Registry:   "index.docker.io" (默认)
//   Repository: "library/nginx" (library 是默认命名空间)
//   Tag:        "latest" (默认)
// 实际等价于: "index.docker.io/library/nginx:latest"
```

### 解析过程

```go
// 在 crane.Pull() 中
func Pull(src string, opt ...Option) (v1.Image, error) {
    o := makeOptions(opt...)
    // 解析 image reference
    ref, err := name.ParseReference(src, o.Name...)
    // ref 包含所有信息：
    //   - ref.Context() -> Repository (包含 registry 和 repository)
    //   - ref.Identifier() -> tag 或 digest
    //   - ref.Name() -> 完整名称
    
    return remote.Image(ref, o.Remote...)
}
```

### name.ParseReference 提取的信息

```go
type Reference interface {
    Context() Repository  // 获取 Repository（包含 registry 和 repository）
    Identifier() string   // 获取 tag 或 digest
    Name() string        // 获取完整名称
    Scope(string) string // 获取 scope
}

type Repository interface {
    RegistryStr() string  // 获取 registry（如 "gcr.io"）
    RepositoryStr() string // 获取 repository（如 "project/image"）
}
```

### 为什么不需要单独传 registry/repository？

**原因 1: Image Reference 包含所有信息**
- `src` 字符串已经包含了 registry、repository、tag/digest 的所有信息
- 通过解析可以自动提取这些信息

**原因 2: 自动默认值补全**
- 如果缺少 registry，默认使用 `index.docker.io` (Docker Hub)
- 如果缺少 repository 命名空间，默认使用 `library/`
- 如果缺少 tag，默认使用 `latest`

**原因 3: 符合 Docker/OCI 标准**
- Docker 和 OCI 标准都使用这种格式的 image reference
- 用户习惯使用这种格式（如 `docker pull ubuntu:latest`）

### 对比 MultiCloudJ Registry

#### go-containerregistry
```go
// 只需要 image reference，所有信息都在字符串中
img, err := crane.Pull("gcr.io/project/image:tag")
// 或者
img, err := crane.Pull("123456789.dkr.ecr.us-east-1.amazonaws.com/repo:tag")
```

#### MultiCloudJ Registry
```java
// 需要分别指定 provider、repository、region
RepositoryClient client = RepositoryClient.builder("gcp")
    .withRepository("project/image")
    .withRegion("us-central1")
    .build();
    
PullResult result = client.pullImage("image:tag");
```

**为什么 MultiCloudJ 需要分开指定？**

1. **多云抽象**: MultiCloudJ 需要知道是哪个云提供商（aws/gcp/ali）
2. **Provider 特定配置**: 不同 provider 需要不同的配置（如 region、endpoint）
3. **Builder 模式**: 使用 Builder 模式来配置 provider 特定的参数
4. **Java 习惯**: Java 更倾向于显式配置而非隐式解析

### 两种设计的对比

| 特性 | go-containerregistry | MultiCloudJ Registry |
|------|---------------------|---------------------|
| **输入格式** | 单个字符串（完整 image reference） | 分开指定（provider + repository + region + imageRef） |
| **信息提取** | 自动解析 image reference | 显式配置各个参数 |
| **默认值** | 自动补全（Docker Hub、library、latest） | 需要显式指定 |
| **Provider 识别** | 从 registry 域名推断 | 需要显式指定 provider ID |
| **灵活性** | 高（支持任意 registry） | 中（需要支持的 provider） |
| **类型安全** | 中（字符串解析） | 高（Builder 模式，编译时检查） |

### 总结

**go-containerregistry 只需要 src string 的原因：**

1. ✅ **Image reference 包含所有信息**: registry、repository、tag/digest 都在字符串中
2. ✅ **自动解析**: `name.ParseReference()` 可以提取所有信息
3. ✅ **自动补全**: 缺少的部分使用默认值（Docker Hub、library、latest）
4. ✅ **符合标准**: 遵循 Docker/OCI 标准的 image reference 格式
5. ✅ **简洁**: 用户只需要一个字符串，不需要分开配置

**MultiCloudJ Registry 需要分开指定的原因：**

1. ✅ **多云抽象**: 需要知道是哪个云提供商
2. ✅ **Provider 配置**: 不同 provider 需要不同的配置（region、endpoint）
3. ✅ **类型安全**: Builder 模式提供编译时类型检查
4. ✅ **Java 习惯**: Java 更倾向于显式配置

两种设计都是合理的，各有优势！
