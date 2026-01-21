# ECR, GAR, ACR Token 格式对比

## 1. AWS ECR (Elastic Container Registry)

### Token 格式
```
Authorization: Basic base64(AWS:ecr-token)
```

### 获取方式
```java
// 调用 AWS ECR API
GetAuthorizationTokenRequest request = GetAuthorizationTokenRequest.builder()
    .build();
GetAuthorizationTokenResponse response = ecrClient.getAuthorizationToken(request);

// 返回格式
{
  "authorizationData": [{
    "authorizationToken": "base64(AWS:ecr-token)",  // 已 base64 编码
    "expiresAt": "2024-01-01T12:00:00Z",
    "proxyEndpoint": "https://123456789012.dkr.ecr.us-east-1.amazonaws.com"
  }]
}

// 解码后得到
username: "AWS"
password: "eyJQVURJTiI6IkFXUyIsIlZFU...<临时token>"
```

### 特点
- ✅ **直接使用 Basic Auth**，不需要 Bearer Token 交换
- Token 有效期：**12 小时**
- Username 固定为 `"AWS"`
- Password 是临时 token（base64 编码的 JSON）

### 在 HTTP 请求中的使用
```http
GET /v2/my-repo/manifests/latest HTTP/1.1
Host: 123456789012.dkr.ecr.us-east-1.amazonaws.com
Authorization: Basic QVdTOmV5SlBWRElJNk...  # base64(AWS:ecr-token)
```

---

## 2. GCP Artifact Registry (GAR)

### Token 格式（两步流程）

#### Step 1: 获取 OAuth2 Access Token
```java
// 从 GoogleCredentials 获取 OAuth2 token
GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
credentials.refreshIfExpired();
AccessToken oauth2Token = credentials.getAccessToken();

// OAuth2 token 格式
"ya29.c.b0Aaek..."  // Google OAuth2 access token
```

#### Step 2: 交换 Bearer Token
```java
// 用 OAuth2 token 交换 Bearer Token
GET https://oauth2.googleapis.com/token?service=us-central1-docker.pkg.dev&scope=repository:my-repo:pull
Authorization: Bearer ya29.c.b0Aaek...  // OAuth2 token

// 响应
{
  "token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",  // Bearer Token
  "expires_in": 3600
}
```

#### Step 3: 使用 Bearer Token
```http
GET /v2/my-repo/manifests/latest HTTP/1.1
Host: us-central1-docker.pkg.dev
Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...  # Bearer Token
```

### 特点
- ❌ **不能直接用 Basic Auth**
- ✅ **必须先用 OAuth2 token 交换 Bearer Token**
- OAuth2 token 有效期：**1 小时**
- Bearer Token 有效期：**1 小时**
- 需要 Ping 发现认证方式（返回 Bearer challenge）

### 完整流程
```
1. Ping registry → 401 + WWW-Authenticate: Bearer realm="https://oauth2.googleapis.com/token"
2. 获取 OAuth2 token (从 GoogleCredentials)
3. 交换 Bearer Token: GET https://oauth2.googleapis.com/token?service=...&scope=...
4. 使用 Bearer Token 访问 registry
```

---

## 3. Alibaba Cloud ACR (Container Registry)

### Token 格式（两种可能）

#### 方式 A: Basic Auth（常用）
```
Authorization: Basic base64(username:password)
```

```java
// 直接使用账号密码
String username = "your-aliyun-username";
String password = "your-aliyun-password";  // 或 access key secret
String auth = "Basic " + base64(username + ":" + password);
```

#### 方式 B: Bearer Token（某些场景）
```
Authorization: Bearer <aliyun-token>
```

```java
// 调用 Aliyun API 获取 token
// 类似 AWS ECR，但格式可能不同
```

### 特点
- ✅ **通常使用 Basic Auth**（username + password/access key）
- 可能支持 Bearer Token（取决于配置）
- 需要 Ping 来确认认证方式

---

## 对比总结

| Registry | 认证方式 | Token 格式 | 获取方式 | 有效期 |
|----------|---------|-----------|---------|--------|
| **AWS ECR** | Basic Auth | `Basic base64(AWS:ecr-token)` | AWS ECR API `GetAuthorizationToken` | 12 小时 |
| **GCP GAR** | Bearer Token | `Bearer <token>` | OAuth2 token → 交换 Bearer Token | 1 小时 |
| **Ali ACR** | Basic Auth (常用) | `Basic base64(username:password)` | 直接使用账号密码 | 永久/手动刷新 |

---

## 关键区别

### 1. 格式不同
```java
// ECR
"Basic " + base64("AWS:" + ecrToken)

// GAR  
"Bearer " + bearerToken  // 不是 Basic！

// ACR
"Basic " + base64(username + ":" + password)
```

### 2. 获取流程不同

**ECR:**
```
AWS Credentials → ECR API → Basic Auth Token → 直接使用
```

**GAR:**
```
Google Credentials → OAuth2 Token → 交换 Bearer Token → 使用 Bearer Token
```

**ACR:**
```
Aliyun Credentials → Basic Auth → 直接使用
```

### 3. 是否需要 Token 交换

- **ECR**: ❌ 不需要交换，直接使用 Basic Auth
- **GAR**: ✅ **必须交换**，OAuth2 → Bearer Token
- **ACR**: ❌ 通常不需要交换，直接用 Basic Auth

---

## 为什么 GAR 需要交换？

GCP 使用 **OAuth2 标准**，要求：
1. 先获取 OAuth2 Access Token（证明身份）
2. 用 OAuth2 token 向 Token 服务器交换 Registry Bearer Token（获得访问权限）
3. 用 Bearer Token 访问 registry

这是 OAuth2 的标准流程，更安全但更复杂。

---

## 在你的代码中如何实现

```java
// AbstractRegistry.getDockerAuthToken() - 各子类实现不同

// ECR 实现
@Override
protected String getDockerAuthToken() {
    // 调用 AWS ECR API
    return ecrClient.getAuthorizationToken().authorizationToken();
    // 返回: base64(AWS:ecr-token) 的一部分（password部分）
}

// GAR 实现  
@Override
protected String getDockerAuthToken() {
    // 返回 OAuth2 token（用于交换 Bearer Token）
    return googleCredentials.getAccessToken().getTokenValue();
    // 注意：这个不能直接用作 Basic Auth 的 password！
}

// ACR 实现
@Override
protected String getDockerAuthToken() {
    // 返回 password 或 access key secret
    return aliyunPassword;
}
```

**关键点：**
- ECR 和 ACR 的 token 可以直接用作 Basic Auth 的 password
- **GAR 的 OAuth2 token 不能直接用作 Basic Auth**，必须先交换 Bearer Token
