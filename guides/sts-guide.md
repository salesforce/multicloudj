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

### Cloud Native Auth (CNA) Features

Cloud Native Auth is a special case that goes beyond the standard `StsClient` API — see the [Cloud Native Auth (CNA)](#cloud-native-auth-cna) section below for details.

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Sign identity** (`StsUtilities`) | ✅ Supported | ✅ Supported | ✅ Supported | Produce a portable `signedIdentity` proving the caller's own cloud identity |
| **Validate identity** (`StsVerifier`) | ✅ Supported | ✅ Supported | ✅ Supported | Verify a `signedIdentity` and recover the caller's `CallerIdentity` |
| **Custom signed headers** | ✅ Supported | ✅ Supported | ✅ Supported | Bind extra context (tenant, request source, ...) into the signed proof and assert it on validation |

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

**AWS**
- For Cloud Native Auth, the `signedIdentity` is a presigned `GetCallerIdentity` URL (SigV4); the verifier replays it against AWS STS. See [AWS](#aws) under Cloud Native Auth.

**GCP (Google Cloud Platform)**
- Uses Google's OAuth 2.0 access tokens for credentials and ID tokens for Caller Identity
- For Cloud Native Auth, the `signedIdentity` is a service-account JWT verified offline against Google-managed public keys. See [GCP](#gcp) under Cloud Native Auth.

**Alibaba Cloud**
- `getAssumeRoleWithWebIdentityCredentials` is not yet supported and throws `UnSupportedOperationException`
- For Cloud Native Auth, the `signedIdentity` is a fully signed `GetCallerIdentity` URL; the verifier replays it against Alibaba Cloud STS. See [Alibaba Cloud](#alibaba-cloud) under Cloud Native Auth.

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

## Cloud Native Auth (CNA)

Cloud Native Auth is a special case that sits alongside the standard `StsClient` API. It lets a workload prove **its own** cloud identity to another service **without any shared secret**. Instead of `StsClient`, CNA uses two dedicated portable types:

- `StsUtilities` — the **client (signing) side**. It signs the caller's own cloud credentials into a portable, self-contained `signedIdentity` string.
- `StsVerifier` — the **server (validation) side**. It takes a `signedIdentity`, proves it against the substrate, and returns the verified `CallerIdentity` (the caller's cloud resource name, user id, and account).

The two halves can run in different processes, services, or even different machines: the client sends the `signedIdentity` string as its proof of identity, and the server validates it. Optional custom headers are signed by the client and asserted by the verifier, so the server can bind extra context (a request source, a tenant id, a federation target, ...) into the signed proof and reject anything that does not match.

### Sign (client side)

Build an `StsUtilities` for the provider and produce a `SignedAuthRequest`. Pass `null` for the request when you only need a bare identity assertion (no service request to hash):

```java
Map<String, String> customHeaders = new LinkedHashMap<>();
customHeaders.put("x-request-source", "cloud-native-auth-example");
customHeaders.put("x-tenant-id", "tenant-42");

StsUtilities signer = StsUtilities.builder("gcp")
    .withRegion("us-east-2")
    .build();

SignOptions signOptions = SignOptions.builder()
    .withCustomHeaders(customHeaders)
    .build();

SignedAuthRequest signed = signer.newCloudNativeAuthSignedRequest(null, signOptions);
String signedIdentity = signed.getSignedIdentity();
// Send signedIdentity to the server as the caller's proof of identity.
```

`SignOptions` (all optional) controls how the request is signed:

| Option | Comments |
|--------|----------|
| `withCustomHeader(name, value)` / `withCustomHeaders(map)` | Extra headers signed into the request; the verifier can assert them on validation |
| `withExcludeRequestHashHeader(boolean)` | Omit the request payload hash header from the signed request |
| `withExcludeContentTypeHeader(boolean)` | Omit the content type header from the signed request |
| `withActionInQueryString(boolean)` | Sign the STS action/version as URL query parameters over an empty body, so the request validates on replay from URL and headers alone |

### Validate (server side)

Build an `StsVerifier` for the same provider and validate the `signedIdentity`. Supply `ValidateOptions` with the custom headers you expect; validation fails if any expected header is missing or has a different value:

```java
StsVerifier verifier = StsVerifier.builder("gcp")
    .withRegion("us-east-2")
    .build();

ValidateOptions validateOptions = ValidateOptions.builder()
    .withExpectedCustomHeaders(customHeaders)
    .build();

CallerIdentity identity = verifier.validateSignedAuthRequest(signedIdentity, validateOptions);
System.out.println("Cloud resource name: " + identity.getCloudResourceName());
System.out.println("User ID: " + identity.getUserId());
System.out.println("Account ID: " + identity.getAccountId());
```

Use the single-argument `validateSignedAuthRequest(signedIdentity)` overload when there are no custom headers to assert.

### Provider-Specific Details

The signing and validation APIs are identical across providers, but the mechanism behind the `signedIdentity` differs. This affects what the verifier needs at validation time (network access, credentials) and how custom headers must be named.

#### AWS

- The `signedIdentity` is a presigned `GetCallerIdentity` request URL rooted at an AWS STS endpoint, signed with SigV4. Its query string carries the STS action, version, and every signed request header.
- The verifier **replays** the presigned request against AWS STS to validate the SigV4 signature and parse the returned identity, so it requires outbound network access to AWS STS. The default AWS credential chain must resolve for the caller producing the signature.
- Before replaying, the verifier requires an `https` scheme and confirms the host is a genuine AWS STS endpoint (`sts.amazonaws.com`, regional `sts.<region>.amazonaws.com`, FIPS variants, and the China partition `.amazonaws.com.cn`). A crafted `signedIdentity` cannot redirect the replay to an attacker-controlled server.
- Custom header names must be **lowercase**, because AWS SigV4 signs canonical (lowercase) header names.

#### GCP

- The `signedIdentity` is a JWT produced by a service account via the IAM `SignJwt` API.
- The verifier validates the JWT **offline** by verifying its RSA signature against the service account's Google-managed public keys (published as a JWKS document at `https://www.googleapis.com/service_accounts/v1/metadata/jwk/{serviceAccountEmail}`). It selects the key by the JWT `kid`, enforces the `exp`/`iat` time bounds with a small clock-skew tolerance, and returns the identity from the token's `iss`/`sub` claims. Public keys are cached to avoid repeated fetches.

#### Alibaba Cloud

- The `signedIdentity` is a fully signed `GetCallerIdentity` STS URL whose query string carries the action, version, every signed parameter, and the RPC signature.
- The verifier **replays** the request against Alibaba Cloud STS to validate the signature and parse the returned identity, so it requires outbound network access to Alibaba Cloud STS. Because Alibaba Cloud RPC signatures cover the HTTP method, the request is replayed with the same `GET` method used to sign it.
- Before replaying, the verifier requires an `https` scheme and confirms the host is a genuine Alibaba Cloud STS endpoint (`sts.aliyuncs.com`, regional `sts.<region>.aliyuncs.com`, and VPC variants). A crafted `signedIdentity` cannot redirect the replay to an attacker-controlled server.

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
