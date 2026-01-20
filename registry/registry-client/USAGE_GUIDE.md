# Registry Pull API 使用指南

## 快速开始

### 最简单的使用方式

```java
import com.salesforce.multicloudj.registry.client.RepositoryClient;
import com.salesforce.multicloudj.registry.driver.PullResult;
import java.nio.file.Paths;

// 1. 创建客户端（需要提供：providerId, repository, region）
try (RepositoryClient client = RepositoryClient.builder("aws")  // 或 "gcp", "ali"
        .withRepository("my-repo")
        .withRegion("us-east-1")
        .build()) {
    
    // 2. 拉取镜像（需要提供：imageRef, destination）
    PullResult result = client.pullImage(
        "my-image:latest",                    // 镜像引用
        Paths.get("/tmp/my-image.tar")        // 保存路径
    );
    
    // 3. 使用结果
    System.out.println("镜像已保存到: " + result.getSavedPath());
    System.out.println("镜像摘要: " + result.getMetadata().getDigest());
}
```

## 用户需要提供的内容

### 1. 创建客户端时（必需参数）

#### 必需参数
- **`providerId`** - 云提供商 ID
  - `"aws"` - AWS ECR
  - `"gcp"` - GCP Artifact Registry
  - `"ali"` - Alibaba ACR

- **`repository`** - 仓库名称
  - AWS ECR: `"my-ecr-repo"`
  - GCP: `"my-gar-repo"`
  - Alibaba: `"my-acr-repo"`

- **`region`** - 区域
  - AWS: `"us-east-1"`, `"us-west-2"` 等
  - GCP: `"us-central1"`, `"asia-east1"` 等
  - Alibaba: `"cn-hangzhou"`, `"cn-beijing"` 等

#### 可选参数
- **`withEndpoint(URI)`** - 自定义端点（测试/开发用）
- **`withProxyEndpoint(URI)`** - 代理端点
- **`withCredentialsOverrider(CredentialsOverrider)`** - 自定义凭证
- **`withRetryConfig(RetryConfig)`** - 重试配置

### 2. 调用 pullImage 时（必需参数）

- **`imageRef`** - 镜像引用（String）
  - 格式：`"name:tag"` 或 `"name@digest"`
  - 示例：
    - `"my-image:latest"`
    - `"my-image:v1.0"`
    - `"my-image@sha256:abc123def456..."`

- **`destination`** - 保存路径（Path）
  - 示例：
    - `Paths.get("/tmp/image.tar")`
    - `Paths.get("/data/images/my-image.tar")`
    - `Paths.get("output/image.tar.gz")` （支持 .tar.gz）

### 3. 环境配置（认证凭证）

Docker 认证会自动处理，但需要提供云提供商的凭证：

#### AWS ECR
```bash
# 方式 1: 环境变量
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_SESSION_TOKEN=xxx  # 如果使用临时凭证

# 方式 2: ~/.aws/credentials 文件
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

#### GCP Artifact Registry
```bash
# 方式 1: 服务账号 JSON 文件
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# 方式 2: gcloud CLI
gcloud auth application-default login
```

#### Alibaba ACR
```bash
# 环境变量
export ALIBABA_CLOUD_ACCESS_KEY_ID=xxx
export ALIBABA_CLOUD_ACCESS_KEY_SECRET=xxx
```

## 完整示例

### 示例 1: 从 AWS ECR 拉取镜像

```java
import com.salesforce.multicloudj.registry.client.RepositoryClient;
import com.salesforce.multicloudj.registry.driver.PullResult;
import java.nio.file.Paths;

public class AwsEcrExample {
    public static void main(String[] args) {
        // 用户提供：providerId, repository, region
        try (RepositoryClient client = RepositoryClient.builder("aws")
                .withRepository("my-ecr-repo")
                .withRegion("us-east-1")
                .build()) {
            
            // 用户提供：imageRef, destination
            PullResult result = client.pullImage(
                "my-app:latest",
                Paths.get("/tmp/my-app.tar")
            );
            
            System.out.println("成功！镜像保存到: " + result.getSavedPath());
        }
    }
}
```

### 示例 2: 从 GCP Artifact Registry 拉取镜像

```java
// 用户提供：providerId, repository, region
try (RepositoryClient client = RepositoryClient.builder("gcp")
        .withRepository("my-gar-repo")
        .withRegion("us-central1")
        .build()) {
    
    // 用户提供：imageRef, destination
    PullResult result = client.pullImage(
        "my-service:v1.0",
        Paths.get("/tmp/my-service.tar")
    );
    
    System.out.println("成功！镜像保存到: " + result.getSavedPath());
}
```

### 示例 3: 从 Alibaba ACR 拉取镜像

```java
// 用户提供：providerId, repository, region
try (RepositoryClient client = RepositoryClient.builder("ali")
        .withRepository("my-acr-repo")
        .withRegion("cn-hangzhou")
        .build()) {
    
    // 用户提供：imageRef, destination
    PullResult result = client.pullImage(
        "my-image:latest",
        Paths.get("/tmp/my-image.tar")
    );
    
    System.out.println("成功！镜像保存到: " + result.getSavedPath());
}
```

### 示例 4: 动态选择云提供商

```java
public void pullFromAnyCloud(String providerId, String repo, String region, 
                            String imageRef, Path destination) {
    // 用户提供：providerId, repository, region
    try (RepositoryClient client = RepositoryClient.builder(providerId)
            .withRepository(repo)
            .withRegion(region)
            .build()) {
        
        // 用户提供：imageRef, destination
        PullResult result = client.pullImage(imageRef, destination);
        System.out.println("从 " + providerId + " 拉取成功！");
    }
}

// 使用
pullFromAnyCloud("aws", "my-repo", "us-east-1", "my-image:latest", 
                 Paths.get("/tmp/image.tar"));
pullFromAnyCloud("gcp", "my-repo", "us-central1", "my-image:latest", 
                 Paths.get("/tmp/image.tar"));
```

### 示例 5: 使用自定义凭证

```java
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;

// 创建自定义凭证
StsCredentials credentials = new StsCredentials(
    "ACCESS_KEY_ID",
    "SECRET_ACCESS_KEY",
    "SESSION_TOKEN"
);

CredentialsOverrider overrider = CredentialsOverrider.builder()
    .type(CredentialsOverrider.Type.SESSION)
    .sessionCredentials(credentials)
    .build();

// 使用自定义凭证
try (RepositoryClient client = RepositoryClient.builder("aws")
        .withRepository("my-repo")
        .withRegion("us-east-1")
        .withCredentialsOverrider(overrider)  // 自定义凭证
        .build()) {
    
    PullResult result = client.pullImage("my-image:latest", 
                                        Paths.get("/tmp/image.tar"));
}
```

## 参数总结表

| 参数 | 类型 | 必需 | 说明 | 示例 |
|------|------|------|------|------|
| **创建客户端** |
| `providerId` | String | ✅ | 云提供商 ID | `"aws"`, `"gcp"`, `"ali"` |
| `repository` | String | ✅ | 仓库名称 | `"my-repo"` |
| `region` | String | ✅ | 区域 | `"us-east-1"` |
| `endpoint` | URI | ❌ | 自定义端点 | `URI.create("https://custom-endpoint.com")` |
| `proxyEndpoint` | URI | ❌ | 代理端点 | `URI.create("http://proxy:8080")` |
| `credentialsOverrider` | CredentialsOverrider | ❌ | 自定义凭证 | 见示例 5 |
| `retryConfig` | RetryConfig | ❌ | 重试配置 | `RetryConfig.builder()...` |
| **调用 pullImage** |
| `imageRef` | String | ✅ | 镜像引用 | `"my-image:latest"` |
| `destination` | Path | ✅ | 保存路径 | `Paths.get("/tmp/image.tar")` |

## 注意事项

1. **认证凭证**: 确保已配置对应云提供商的凭证（环境变量或配置文件）
2. **网络访问**: 确保可以访问对应的 registry 端点
3. **权限**: 确保凭证有拉取镜像的权限
4. **路径**: destination 路径的父目录必须存在
5. **资源管理**: 使用 try-with-resources 确保客户端正确关闭

## 常见问题

**Q: 如果我没有设置凭证会怎样？**
A: 会抛出异常，提示无法获取认证 token。请按照上面的环境配置设置凭证。

**Q: 可以同时支持多个云提供商吗？**
A: 可以，创建不同的 `RepositoryClient` 实例即可。

**Q: 拉取的镜像格式是什么？**
A: 标准的 Docker 镜像 tar 格式，可以用 `docker load` 加载。
