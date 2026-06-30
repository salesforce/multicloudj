---
layout: default
title: How to STS
nav_order: 1
parent: Usage Guides
---
# STS (Security Token Service)

The `StsClient` class in the `multicloudj` library provides a portable interface for interacting with cloud provider security token services such as AWS STS, GCP IAM Credentials, or any other compatible implementation. It allows you to obtain temporary credentials, access tokens, and caller identity information in a cloud-neutral way.

---

## Overview

The `StsClient` is built on top of provider-specific implementations of `AbstractSts`. Each provider registers its implementation and is selected dynamically at runtime.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Assume Role** | ✅ Supported | ✅ Supported | ✅ Supported | Core feature for temporary credentials for role/service account |
| **Get Caller Identity** | ✅ Supported | ✅ Supported | ✅ Supported | Returns identity information of the current caller |
| **Get Access Token** | ✅ Supported | ✅ Supported | ✅ Supported | Get credentials using default env configs |
| **Assume Role with Web Identity** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Exchange a web identity token (e.g. OIDC) for temporary credentials |

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Region Support** | ✅ Supported | ✅ Supported | ✅ Supported | All providers support region-specific operations |
| **Endpoint Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom endpoint configuration |
| **Proxy Support** | ✅ Supported | ✅ Supported | ✅ Supported | Explicit proxy endpoint or system/environment proxy values |

### Provider IDs

| Provider | Provider ID |
|----------|-------------|
| AWS | `aws` |
| GCP (Google Cloud Platform) | `gcp` |
| Alibaba Cloud | `ali` |

### Provider-Specific Notes

**GCP (Google Cloud Platform)**
- Uses Google's OAuth 2.0 access tokens for credentials and ID tokens for Caller Identity

**Alibaba Cloud**
- `getAssumeRoleWithWebIdentityCredentials` is not yet supported and throws `UnSupportedOperationException`

---

## Creating a Client

```java
StsClient stsClient = StsClient.builder("aws")
    .withRegion("us-west-2")
    .build();
```

Optionally, you can set a custom endpoint:

```java
URI endpoint = URI.create("https://sts.custom-endpoint.com");
StsClient stsClient = StsClient.builder("aws")
    .withRegion("us-west-2")
    .withEndpoint(endpoint)
    .build();
```

### Proxy Configuration

You can route requests through an explicit proxy endpoint, or instruct the client to pick up proxy settings from system properties or environment variables:

```java
StsClient stsClient = StsClient.builder("aws")
    .withRegion("us-west-2")
    .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
    .build();

// Or use system property / environment variable proxy values
StsClient stsClient2 = StsClient.builder("aws")
    .withRegion("us-west-2")
    .withUseSystemPropertyProxyValues(true)
    .withUseEnvironmentVariableProxyValues(true)
    .build();
```

---

## Getting Caller Identity

Retrieve the caller identity associated with the current credentials:

```java
CallerIdentity identity = stsClient.getCallerIdentity();
System.out.println("User ID: " + identity.getUserId());
System.out.println("Cloud resource name: " + identity.getCloudResourceName());
System.out.println("Account ID: " + identity.getAccountId());
```

`CallerIdentity` exposes `getUserId()`, `getCloudResourceName()`, and `getAccountId()`. The `cloudResourceName` is the provider-native identifier of the caller (for example, the IAM ARN on AWS).

You can also pass a `GetCallerIdentityRequest` to set an optional target audience (`aud`), which is used by some providers (e.g. GCP) when generating an identity token:

```java
GetCallerIdentityRequest request = GetCallerIdentityRequest.builder()
    .aud("https://my-audience.example.com")
    .build();
CallerIdentity identity = stsClient.getCallerIdentity(request);
```

---

## Getting an Access Token

Use this when you need an OAuth2-style token (provider support may vary):

```java
GetAccessTokenRequest request = GetAccessTokenRequest.newBuilder()
    .withDurationSeconds(3600)
    .build();
StsCredentials token = stsClient.getAccessToken(request);
System.out.println("Security token: " + token.getSecurityToken());
```

`StsCredentials` exposes `getAccessKeyId()`, `getAccessKeySecret()`, and `getSecurityToken()`. For OAuth2-style access tokens, the token value is returned in `getSecurityToken()`.

---

## Assuming a Role

To assume a different identity (e.g., for cross-account access):

```java
AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
    .withRole("arn:aws:iam::123456789012:role/example-role")
    .withSessionName("example-session")
    .withExpiration(3600)
    .build();
StsCredentials credentials = stsClient.getAssumeRoleCredentials(request);
System.out.println("Access Key ID: " + credentials.getAccessKeyId());
System.out.println("Security Token: " + credentials.getSecurityToken());
```

---

## Assuming a Role with Web Identity

Exchange a web identity token (such as an OIDC token) for temporary credentials. Supported on AWS and GCP:

```java
AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
    .role("arn:aws:iam::123456789012:role/example-role")
    .webIdentityToken("<oidc-token>")
    .sessionName("example-session")
    .expiration(3600)
    .build();
StsCredentials credentials = stsClient.getAssumeRoleWithWebIdentityCredentials(request);
System.out.println("Access Key ID: " + credentials.getAccessKeyId());
```

---

## Error Handling

All errors are translated to `SubstrateSdkException` subclasses by the underlying driver. The client will automatically map exceptions to meaningful runtime errors based on the provider:

```java
try {
    CallerIdentity identity = stsClient.getCallerIdentity();
} catch (SubstrateSdkException e) {
    // Handle known errors: AccessDenied, Timeout, etc.
    e.printStackTrace();
}
```
