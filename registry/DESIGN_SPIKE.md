# Design Spike: Registry Authentication Architecture

## 背景 (Background)

Registry 模块需要支持跨云 Docker 镜像拉取功能，涉及 AWS ECR、GCP GAR 和 Alibaba ACR。核心挑战是如何设计认证抽象层，使得：
1. OCI Registry HTTP Client 层不关心具体是哪个云提供商
2. 各云提供商的认证逻辑可以独立实现
3. 设计符合 MultiCloudJ codebase 的整体架构模式

## 问题陈述 (Problem Statement)

如何设计 Registry 认证抽象层，使其既能满足功能需求，又能保持与 MultiCloudJ 其他模块（blob、docstore、pubsub）的设计一致性？

## 设计方案比较

### 方案 A: 当前实现 - 抽象基类模式 (Current Implementation)

**设计概述：**
使用 `AbstractRegistry` 抽象基类，通过 `protected abstract` 方法让各云提供商实现认证逻辑。

**代码结构：**

```java
// 核心模型
public class ImageReference {
    private final String registry;
    private final String repository;
    private final String reference; // tag or digest
}

// 抽象基类
public abstract class AbstractRegistry {
    // 各云提供商需要实现的抽象方法
    protected abstract String getDockerAuthToken() throws IOException;
    protected abstract String getDockerAuthUsername();
    
    // 通用实现：组合 token 和 username 成 HTTP header
    protected final String getDockerAuthHeader() throws IOException {
        String username = getDockerAuthUsername();
        String token = getDockerAuthToken();
        String credentials = username + ":" + token;
        String encoded = Base64.getEncoder()
            .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }
    
    // OCI Registry Client 使用 lambda 传递认证逻辑
    protected PullResult doPullImageUsingOciRegistry(String imageRef, Path destination) {
        OciRegistryClient.AuthProvider authProvider = () -> getDockerAuthHeader();
        OciRegistryClient client = new OciRegistryClient(registryEndpoint, repo, authProvider);
        // ...
    }
}

// 内部接口（仅用于 OCI Client）
class OciRegistryClient {
    interface AuthProvider {
        String getAuthHeader() throws IOException;
    }
}

// 云提供商实现示例（AWS ECR）
public class AwsRegistry extends AbstractRegistry {
    @Override
    protected String getDockerAuthToken() throws IOException {
        // 调用 ECR GetAuthorizationToken API
        GetAuthorizationTokenResponse response = ecrClient.getAuthorizationToken(...);
        // 解码并提取 token
        return extractToken(response);
    }
    
    @Override
    protected String getDockerAuthUsername() {
        return "AWS"; // ECR 固定用户名
    }
}
```

**优点：**
- ✅ **与 codebase 一致**：与 `AbstractBlobStore`、`AbstractDocStore`、`AbstractSubscription` 的设计模式完全一致
- ✅ **简单直接**：认证逻辑直接集成在 Registry 实现中，无需额外的接口层
- ✅ **易于理解**：继承关系清晰，符合面向对象设计
- ✅ **减少抽象层**：没有额外的接口和类，代码更简洁
- ✅ **类型安全**：编译时检查，IDE 支持更好

**缺点：**
- ❌ **耦合度较高**：认证逻辑与 Registry 实现耦合
- ❌ **测试稍复杂**：需要 mock 整个 AbstractRegistry 才能测试认证逻辑
- ❌ **灵活性较低**：无法在运行时动态替换认证提供者

---

### 方案 B: 独立接口模式 (Interface-based Design)

**设计概述：**
使用独立的 `RegistryAuthProvider` 接口和 `RegistryAuth` 类，通过组合模式实现认证抽象。

**代码结构：**

```java
// 核心模型
public class ImageReference {
    private final String registry;
    private final String repository;
    private final String reference; // tag or digest
}

// 认证模型
public class RegistryAuth {
    private final String authorizationHeader; // e.g. "Basic xxx" or "Bearer yyy"
    
    public RegistryAuth(String authorizationHeader) {
        this.authorizationHeader = authorizationHeader;
    }
    
    public String getAuthorizationHeader() {
        return authorizationHeader;
    }
}

// 认证提供者接口
public interface RegistryAuthProvider {
    RegistryAuth getAuth(ImageReference image) throws IOException;
}

// 抽象基类
public abstract class AbstractRegistry {
    private final RegistryAuthProvider authProvider;
    
    protected AbstractRegistry(Builder<?, ?> builder) {
        this.authProvider = createAuthProvider();
    }
    
    // 各云提供商需要实现
    protected abstract RegistryAuthProvider createAuthProvider();
    
    protected PullResult doPullImageUsingOciRegistry(String imageRef, Path destination) {
        ImageReference ref = ImageReference.parse(imageRef);
        RegistryAuth auth = authProvider.getAuth(ref);
        OciRegistryClient client = new OciRegistryClient(
            registryEndpoint, 
            repo, 
            () -> auth.getAuthorizationHeader()
        );
        // ...
    }
}

// 云提供商实现示例（AWS ECR）
public class AwsRegistry extends AbstractRegistry {
    @Override
    protected RegistryAuthProvider createAuthProvider() {
        return new AwsRegistryAuthProvider(ecrClient);
    }
    
    private static class AwsRegistryAuthProvider implements RegistryAuthProvider {
        private final EcrClient ecrClient;
        
        @Override
        public RegistryAuth getAuth(ImageReference image) throws IOException {
            // 调用 ECR GetAuthorizationToken API
            GetAuthorizationTokenResponse response = ecrClient.getAuthorizationToken(...);
            String token = extractToken(response);
            String credentials = "AWS:" + token;
            String encoded = Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
            return new RegistryAuth("Basic " + encoded);
        }
    }
}
```

**优点：**
- ✅ **解耦设计**：认证逻辑与 Registry 实现分离，职责更清晰
- ✅ **易于测试**：可以独立测试 `RegistryAuthProvider`，无需 mock 整个 Registry
- ✅ **更灵活**：可以在运行时动态替换认证提供者
- ✅ **符合 SOLID 原则**：单一职责原则，接口隔离原则
- ✅ **可扩展性**：更容易支持新的认证方式（如 OAuth2、JWT 等）

**缺点：**
- ❌ **与 codebase 不一致**：其他模块（blob、docstore、pubsub）都没有使用这种独立接口模式
- ❌ **增加抽象层**：需要额外的 `RegistryAuth` 类和 `RegistryAuthProvider` 接口
- ❌ **代码复杂度**：更多的类和接口，增加了代码量
- ❌ **可能过度设计**：对于当前需求可能过于复杂

---

## 详细比较分析

### 1. 与现有 Codebase 的一致性

| 模块 | 认证处理方式 | 设计模式 |
|------|------------|---------|
| `AbstractBlobStore` | `CredentialsOverrider` + 子类实现 | 抽象基类 + protected abstract |
| `AbstractDocStore` | `CredentialsOverrider` + 子类实现 | 抽象基类 + protected abstract |
| `AbstractSubscription` | `CredentialsOverrider` + 子类实现 | 抽象基类 + protected abstract |
| `AbstractRegistry` (方案 A) | `protected abstract` 方法 | 抽象基类 + protected abstract ✅ |
| `AbstractRegistry` (方案 B) | 独立 `RegistryAuthProvider` 接口 | 组合模式 ❌ |

**结论：** 方案 A 与现有 codebase 完全一致，方案 B 会打破一致性。

### 2. 代码复杂度

**方案 A：**
- 类数量：`AbstractRegistry` + 各云实现类
- 接口数量：1 个内部接口（`OciRegistryClient.AuthProvider`，仅用于 lambda）
- 抽象层数：2 层（抽象基类 → 实现类）

**方案 B：**
- 类数量：`AbstractRegistry` + `RegistryAuth` + 各云实现类 + 各云的 AuthProvider 实现
- 接口数量：2 个（`RegistryAuthProvider` + `OciRegistryClient.AuthProvider`）
- 抽象层数：3 层（抽象基类 → 实现类 → AuthProvider）

**结论：** 方案 A 更简单，代码量更少。

### 3. 可测试性

**方案 A：**
- 需要 mock `AbstractRegistry` 来测试认证逻辑
- 或者直接测试子类实现

**方案 B：**
- 可以独立测试 `RegistryAuthProvider`
- 更容易进行单元测试

**结论：** 方案 B 在可测试性上略胜一筹，但差距不大（因为方案 A 也可以直接测试子类）。

### 4. 可扩展性

**方案 A：**
- 添加新的认证方式需要修改 `AbstractRegistry` 或创建新的抽象方法
- 但当前需求（Basic Auth）已经足够

**方案 B：**
- 更容易添加新的认证方式（OAuth2、JWT 等）
- 但当前需求可能不需要这种灵活性

**结论：** 方案 B 在可扩展性上更好，但可能过度设计。

### 5. 维护成本

**方案 A：**
- 符合现有模式，新团队成员更容易理解
- 维护成本低

**方案 B：**
- 需要维护额外的抽象层
- 与现有模式不一致，可能造成困惑

**结论：** 方案 A 维护成本更低。

---

## 推荐方案

### 推荐：方案 A（当前实现）

**理由：**

1. **一致性优先**：与 MultiCloudJ 其他模块的设计模式完全一致，保持 codebase 的统一性
2. **简单有效**：对于当前需求（Basic Auth），抽象基类模式已经足够
3. **易于维护**：符合团队已有的设计习惯，降低学习成本
4. **性能考虑**：更少的抽象层，运行时开销更小

### 何时考虑方案 B

如果未来出现以下需求，可以考虑重构为方案 B：
- 需要支持多种认证方式（OAuth2、JWT、API Key 等）
- 需要在运行时动态切换认证提供者
- 认证逻辑变得非常复杂，需要独立管理

---

## 实施建议

### 当前实现（方案 A）的改进点

1. **文档完善**：在 `PROVIDER_IMPLEMENTATION_GUIDE.md` 中明确说明设计选择
2. **代码注释**：在 `AbstractRegistry` 中添加注释，说明为什么使用抽象方法而不是独立接口
3. **测试覆盖**：确保各云提供商的认证逻辑有充分的单元测试

### 如果未来需要重构到方案 B

1. **保持向后兼容**：通过适配器模式，让旧的抽象方法调用新的接口
2. **渐进式迁移**：先添加新接口，再逐步迁移各云实现
3. **充分测试**：确保重构不影响现有功能

---

## 相关文档

- [Provider 实现指南](./PROVIDER_IMPLEMENTATION_GUIDE.md)
- [跨云 Pull API](./registry-client/CROSS_CLOUD_PULL_API.md)
- [设计文档 - 架构层次](../../documentation/design/layers.md)

---

## 决策记录

**决策日期：** 2024-XX-XX  
**决策者：** [待填写]  
**状态：** ✅ 已采用方案 A（当前实现）  
**理由：** 与 codebase 整体设计模式一致，简单有效，易于维护
