# 跨云 Pull API 使用指南

## 概述

MultiCloudJ Registry 提供了统一的跨云 pull API，支持从 AWS ECR、GCP Artifact Registry 和 Alibaba ACR 拉取 Docker 镜像，使用相同的 API 接口。

## 核心设计

- **统一的 API**: 所有云提供商使用相同的 `RepositoryClient.pullImage()` 方法
- **统一的协议**: 所有 provider 都使用标准的 OCI Registry (Docker Registry API v2) 协议
- **自动认证**: Docker 认证在 driver 层自动处理，用户无需关心
- **Provider 自动发现**: 通过 `providerId` 自动选择正确的实现

## 使用示例

### 1. 从 AWS ECR 拉取镜像

```java
import com.salesforce.multicloudj.registry.client.RepositoryClient;
import com.salesforce.multicloudj.registry.driver.PullResult;
import java.nio.file.Paths;

// 创建客户端（指定 providerId = "aws"）
try (RepositoryClient client = RepositoryClient.builder("aws")
        .withRepository("my-ecr-repo")
        .withRegion("us-east-1")
        .build()) {
    
    // 拉取镜像（API 与其他云提供商完全相同）
    PullResult result = client.pullImage(
        "my-image:latest",
        Paths.get("/tmp/aws-image.tar")
    );
    
    System.out.println("Image saved to: " + result.getSavedPath());
    System.out.println("Image digest: " + result.getMetadata().getDigest());
}
```

### 2. 从 GCP Artifact Registry 拉取镜像

```java
// 创建客户端（指定 providerId = "gcp"）
try (RepositoryClient client = RepositoryClient.builder("gcp")
        .withRepository("my-gar-repo")
        .withRegion("us-central1")
        .build()) {
    
    // 相同的 API 调用
    PullResult result = client.pullImage(
        "my-image:v1.0",
        Paths.get("/tmp/gcp-image.tar")
    );
    
    System.out.println("Image saved to: " + result.getSavedPath());
}
```

### 3. 从 Alibaba ACR 拉取镜像

```java
// 创建客户端（指定 providerId = "ali"）
try (RepositoryClient client = RepositoryClient.builder("ali")
        .withRepository("my-acr-repo")
        .withRegion("cn-hangzhou")
        .build()) {
    
    // 相同的 API 调用
    PullResult result = client.pullImage(
        "my-image:latest",
        Paths.get("/tmp/ali-image.tar")
    );
    
    System.out.println("Image saved to: " + result.getSavedPath());
}
```

### 4. 动态选择云提供商

```java
public void pullImageFromAnyCloud(String providerId, String imageRef, Path destination) {
    try (RepositoryClient client = RepositoryClient.builder(providerId)
            .withRepository(getRepositoryForProvider(providerId))
            .withRegion(getRegionForProvider(providerId))
            .build()) {
        
        // 统一的 API，无需关心底层是哪个云提供商
        PullResult result = client.pullImage(imageRef, destination);
        System.out.println("Successfully pulled from " + providerId);
    }
}
```

## API 说明

### RepositoryClient.builder(providerId)

创建客户端构建器，支持的 `providerId`:
- `"aws"` - AWS ECR
- `"gcp"` - GCP Artifact Registry  
- `"ali"` - Alibaba ACR

### pullImage(imageRef, destination)

统一的 pull 方法：
- **imageRef**: 镜像引用，格式 `"name:tag"` 或 `"name@digest"`
  - 示例: `"my-image:latest"`, `"my-image@sha256:abc123..."`
- **destination**: 保存路径 (`Path` 对象)
- **返回值**: `PullResult` 包含镜像元数据和保存路径

## 认证配置

Docker 认证在 driver 层自动处理，用户只需提供云提供商的凭证：

### AWS ECR
```bash
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
export AWS_SESSION_TOKEN=xxx  # 如果使用临时凭证
```

### GCP Artifact Registry
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
# 或使用: gcloud auth application-default login
```

### Alibaba ACR
```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID=xxx
export ALIBABA_CLOUD_ACCESS_KEY_SECRET=xxx
```

## 架构优势

1. **代码复用**: Pull 逻辑在 `AbstractRegistry` 中实现一次，所有 provider 共享
2. **统一协议**: 所有 provider 使用相同的 OCI Registry API v2 协议
3. **易于扩展**: 添加新的云提供商只需实现 3 个方法：
   - `getRegistryEndpoint()` - 端点 URL
   - `getDockerAuthToken()` - 认证 token
   - `getDockerAuthUsername()` - 认证用户名
4. **用户友好**: 统一的 API，无需学习不同云提供商的差异

## 实现细节

- **协议**: OCI Registry (Docker Registry API v2)
- **认证**: HTTP Basic Auth (自动处理)
- **格式**: Docker 镜像 tar 文件
- **流式传输**: 支持大镜像，避免内存溢出
