# Provider 实现指南

## 概述

每个 provider（AWS ECR、GCP GAR、Alibaba ACR）只需要实现 **3 个抽象方法**，即可完成跨云 pull 功能。Pull 逻辑已经在 `AbstractRegistry` 中统一实现。

## 每个 Provider 需要实现的 3 个方法

### 1. `getRegistryEndpoint()` - 返回 Registry 端点 URL

### 2. `getDockerAuthToken()` - 获取 Docker 认证 Token

### 3. `getDockerAuthUsername()` - 获取 Docker 认证用户名

### 4. `getException(Throwable)` - 异常映射（继承自 SdkService）

---

## AWS ECR 实现示例

```java
package com.salesforce.multicloudj.registry.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenRequest;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenResponse;
import software.amazon.awssdk.core.auth.policy.Action;
import software.amazon.awssdk.core.auth.policy.Policy;
import software.amazon.awssdk.core.auth.policy.Principal;
import software.amazon.awssdk.core.auth.policy.Resource;
import software.amazon.awssdk.core.auth.policy.Statement;

import java.io.IOException;
import java.util.Base64;

@AutoService(AbstractRegistry.class)
public class AwsRegistry extends AbstractRegistry {
    private static final String PROVIDER_ID = "aws";
    private final EcrClient ecrClient;
    private final String accountId; // 从 AWS credentials 或 STS 获取

    public AwsRegistry(Builder builder) {
        super(builder);
        // 初始化 ECR 客户端
        this.ecrClient = EcrClient.builder()
            .region(software.amazon.awssdk.regions.Region.of(builder.getRegion()))
            .build();
        // 获取 accountId（从 STS GetCallerIdentity 或配置中获取）
        this.accountId = getAccountId();
    }

    @Override
    protected String getRegistryEndpoint() {
        // ECR 端点格式: https://{accountId}.dkr.ecr.{region}.amazonaws.com
        return String.format("https://%s.dkr.ecr.%s.amazonaws.com", 
                            accountId, getRegion());
    }

    @Override
    protected String getDockerAuthToken() throws IOException {
        try {
            // 调用 ECR GetAuthorizationToken API
            GetAuthorizationTokenRequest request = GetAuthorizationTokenRequest.builder()
                .build();
            
            GetAuthorizationTokenResponse response = ecrClient.getAuthorizationToken(request);
            
            // ECR 返回的是 base64 编码的 "AWS:token" 格式
            String authToken = response.authorizationData().get(0).authorizationToken();
            
            // 解码 base64，提取 token 部分（去掉 "AWS:" 前缀）
            String decoded = new String(Base64.getDecoder().decode(authToken));
            // decoded 格式: "AWS:actual-token-here"
            return decoded.substring(4); // 去掉 "AWS:" 前缀
        } catch (Exception e) {
            throw new IOException("Failed to get ECR authorization token", e);
        }
    }

    @Override
    protected String getDockerAuthUsername() {
        // ECR 使用 "AWS" 作为用户名
        return "AWS";
    }

    @Override
    public Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> getException(Throwable t) {
        // 映射 AWS 异常到 MultiCloudJ 异常
        if (t instanceof software.amazon.awssdk.services.ecr.model.EcrException) {
            // 根据错误码映射
            return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
        }
        return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    private String getAccountId() {
        // 从 STS GetCallerIdentity 或配置中获取 accountId
        // 这里简化处理，实际应该调用 STS API
        return "123456789012"; // 示例
    }

    public static class Builder extends AbstractRegistry.Builder<AwsRegistry, Builder> {
        @Override
        public Builder self() {
            return this;
        }

        @Override
        public AwsRegistry build() {
            return new AwsRegistry(this);
        }

        @Override
        public String getProviderId() {
            return PROVIDER_ID;
        }
    }
}
```

---

## GCP Artifact Registry 实现示例

```java
package com.salesforce.multicloudj.registry.gcp;

import com.google.auto.service.AutoService;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;

import java.io.IOException;

@AutoService(AbstractRegistry.class)
public class GcpRegistry extends AbstractRegistry {
    private static final String PROVIDER_ID = "gcp";
    private final GoogleCredentials credentials;
    private final String projectId; // 从 properties 或 credentials 获取
    private final String location; // 对应 region

    public GcpRegistry(Builder builder) {
        super(builder);
        this.location = builder.getRegion();
        // 从 properties 获取 projectId
        this.projectId = builder.getProperties().getProperty("gcp.projectId");
        
        // 获取 GoogleCredentials
        com.google.auth.Credentials creds = GcpCredentialsProvider.getCredentials(
            builder.getCredentialsOverrider());
        if (!(creds instanceof GoogleCredentials)) {
            throw new RuntimeException("GCP credentials must be GoogleCredentials");
        }
        this.credentials = (GoogleCredentials) creds;
    }

    @Override
    protected String getRegistryEndpoint() {
        // GAR 端点格式: https://{location}-docker.pkg.dev/{project}/{repository}
        return String.format("https://%s-docker.pkg.dev/%s/%s", 
                            location, projectId, getRepository());
    }

    @Override
    protected String getDockerAuthToken() throws IOException {
        try {
            // GCP 使用 OAuth2 access token
            // GoogleCredentials 会自动处理 token 刷新
            credentials.refreshIfExpired();
            return credentials.getAccessToken().getTokenValue();
        } catch (Exception e) {
            throw new IOException("Failed to get GCP access token", e);
        }
    }

    @Override
    protected String getDockerAuthUsername() {
        // GCP Artifact Registry 使用 "oauth2accesstoken" 作为用户名
        return "oauth2accesstoken";
    }

    @Override
    public Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> getException(Throwable t) {
        // 映射 GCP 异常到 MultiCloudJ 异常
        if (t instanceof com.google.api.gax.rpc.ApiException) {
            com.google.api.gax.rpc.ApiException apiException = (com.google.api.gax.rpc.ApiException) t;
            // 根据状态码映射
            return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
        }
        return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractRegistry.Builder<GcpRegistry, Builder> {
        @Override
        public Builder self() {
            return this;
        }

        @Override
        public GcpRegistry build() {
            return new GcpRegistry(this);
        }

        @Override
        public String getProviderId() {
            return PROVIDER_ID;
        }
    }
}
```

---

## Alibaba ACR 实现示例

```java
package com.salesforce.multicloudj.registry.ali;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.cr.model.v20160607.GetAuthorizationTokenRequest;
import com.aliyuncs.cr.model.v20160607.GetAuthorizationTokenResponse;
import com.aliyuncs.profile.DefaultProfile;

import java.io.IOException;

@AutoService(AbstractRegistry.class)
public class AliRegistry extends AbstractRegistry {
    private static final String PROVIDER_ID = "ali";
    private final IAcsClient acsClient;
    private final String registryName; // 从 properties 获取

    public AliRegistry(Builder builder) {
        super(builder);
        this.registryName = builder.getProperties().getProperty("ali.registryName");
        
        // 初始化 Aliyun 客户端
        DefaultProfile profile = DefaultProfile.getProfile(
            builder.getRegion(),
            getAccessKeyId(),
            getAccessKeySecret()
        );
        this.acsClient = new DefaultAcsClient(profile);
    }

    @Override
    protected String getRegistryEndpoint() {
        // ACR 端点格式: https://{registryName}.{region}.aliyuncs.com
        return String.format("https://%s.%s.aliyuncs.com", 
                            registryName, getRegion());
    }

    @Override
    protected String getDockerAuthToken() throws IOException {
        try {
            // 调用 Aliyun ACR GetAuthorizationToken API
            GetAuthorizationTokenRequest request = new GetAuthorizationTokenRequest();
            request.setRegionId(getRegion());
            
            GetAuthorizationTokenResponse response = acsClient.getAcsResponse(request);
            
            // 返回 token
            return response.getAuthorizationToken();
        } catch (Exception e) {
            throw new IOException("Failed to get Aliyun ACR token", e);
        }
    }

    @Override
    protected String getDockerAuthUsername() {
        // ACR 使用 Aliyun 账号名作为用户名
        // 可以从 credentials 或配置中获取
        return getAliyunAccountName();
    }

    @Override
    public Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> getException(Throwable t) {
        // 映射 Aliyun 异常到 MultiCloudJ 异常
        return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    private String getAccessKeyId() {
        // 从环境变量或 credentials 获取
        return System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
    }

    private String getAccessKeySecret() {
        // 从环境变量或 credentials 获取
        return System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
    }

    private String getAliyunAccountName() {
        // 从配置或 API 获取账号名
        return "aliyun-account-name";
    }

    public static class Builder extends AbstractRegistry.Builder<AliRegistry, Builder> {
        @Override
        public Builder self() {
            return this;
        }

        @Override
        public AliRegistry build() {
            return new AliRegistry(this);
        }

        @Override
        public String getProviderId() {
            return PROVIDER_ID;
        }
    }
}
```

---

## 实现要点总结

### 1. `getRegistryEndpoint()` 实现要点

| Provider | 端点格式 | 示例 |
|----------|---------|------|
| AWS ECR | `https://{accountId}.dkr.ecr.{region}.amazonaws.com` | `https://123456789012.dkr.ecr.us-east-1.amazonaws.com` |
| GCP GAR | `https://{location}-docker.pkg.dev/{project}/{repository}` | `https://us-central1-docker.pkg.dev/my-project/my-repo` |
| Alibaba ACR | `https://{registryName}.{region}.aliyuncs.com` | `https://my-registry.cn-hangzhou.aliyuncs.com` |

### 2. `getDockerAuthToken()` 实现要点

| Provider | Token 获取方式 | 注意事项 |
|----------|---------------|---------|
| AWS ECR | 调用 `ECR.GetAuthorizationToken` API | 返回的是 base64 编码的 "AWS:token"，需要解码并提取 token |
| GCP GAR | 从 `GoogleCredentials.getAccessToken()` 获取 | 自动处理 token 刷新 |
| Alibaba ACR | 调用 `ACR.GetAuthorizationToken` API | 直接返回 token |

### 3. `getDockerAuthUsername()` 实现要点

| Provider | 用户名 | 说明 |
|----------|--------|------|
| AWS ECR | `"AWS"` | 固定值 |
| GCP GAR | `"oauth2accesstoken"` | 固定值 |
| Alibaba ACR | Aliyun 账号名 | 需要从配置或 API 获取 |

### 4. 其他必需实现

- **`@AutoService(AbstractRegistry.class)`**: 用于 ServiceLoader 自动发现
- **`Builder` 类**: 继承 `AbstractRegistry.Builder`，实现 `build()` 和 `self()` 方法
- **`getException(Throwable)`**: 映射 provider 特定异常到 MultiCloudJ 异常

---

## 实现检查清单

- [ ] 实现 `getRegistryEndpoint()` - 返回正确的端点 URL
- [ ] 实现 `getDockerAuthToken()` - 调用云提供商 API 获取 token
- [ ] 实现 `getDockerAuthUsername()` - 返回正确的用户名
- [ ] 实现 `getException(Throwable)` - 异常映射
- [ ] 添加 `@AutoService(AbstractRegistry.class)` 注解
- [ ] 实现 `Builder` 类
- [ ] 在 `pom.xml` 中添加必要的依赖（SDK、auto-service 等）
- [ ] 在 `META-INF/services/com.salesforce.multicloudj.registry.driver.AbstractRegistry` 中注册（如果不用 @AutoService）

---

## 依赖要求

### AWS ECR
```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>ecr</artifactId>
</dependency>
```

### GCP GAR
```xml
<dependency>
    <groupId>com.google.auth</groupId>
    <artifactId>google-auth-library-oauth2-http</artifactId>
</dependency>
```

### Alibaba ACR
```xml
<dependency>
    <groupId>com.aliyun</groupId>
    <artifactId>aliyun-java-sdk-core</artifactId>
</dependency>
<dependency>
    <groupId>com.aliyun</groupId>
    <artifactId>aliyun-java-sdk-cr</artifactId>
</dependency>
```

---

## 总结

每个 provider 只需要实现 **3 个核心方法** + **异常映射** + **Builder 类**，Pull 逻辑已经在 `AbstractRegistry` 中统一实现，所有 provider 共享相同的 OCI Registry 协议实现。
