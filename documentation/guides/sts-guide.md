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
| **Get Access Token** | ✅ Supported | ✅ Supported | ✅ Supported | Get credentials using default env configs|

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Region Support** | ✅ Supported | ✅ Supported | ✅ Supported | All providers support region-specific operations |
| **Endpoint Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom endpoint configuration |

### Provider-Specific Notes

**GCP (Google Cloud Platform)**
- Uses Google's OAuth 2.0 access tokens for credentials and ID tokens for Caller Identity

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

---

## Getting Caller Identity

Retrieve the caller identity associated with the current credentials:

```java
CallerIdentity identity = stsClient.getCallerIdentity();
System.out.println("Caller: " + identity.getArn());
```

---

## Getting an Access Token

Use this when you need an OAuth2-style token (provider support may vary):

```java
GetAccessTokenRequest request = new GetAccessTokenRequest();
StsCredentials token = stsClient.getAccessToken(request);
System.out.println("Access Token: " + token.getAccessToken());
```

---

## Assuming a Role

To assume a different identity (e.g., for cross-account access):

```java
AssumedRoleRequest request = new AssumedRoleRequest();
request.setRoleArn("arn:aws:iam::123456789012:role/example-role");
request.setSessionName("example-session");
StsCredentials credentials = stsClient.getAssumeRoleCredentials(request);
System.out.println("Temporary Credentials: " + credentials.getAccessKeyId());
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
