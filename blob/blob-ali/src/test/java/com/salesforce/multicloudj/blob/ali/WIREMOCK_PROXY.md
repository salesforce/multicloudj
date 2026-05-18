# WireMock Proxy Configuration for Ali OSS Tests

## Problem

The Ali OSS SDK uses **virtual-hosted style** URLs by default. For bucket operations, the SDK sends requests to `bucketName.oss-cn-shanghai.aliyuncs.com` rather than including the bucket name in the URL path.

When WireMock acts as a MITM proxy in recording mode, it intercepts the HTTPS request and forwards it to the recording target endpoint (`oss-cn-shanghai.aliyuncs.com`). During this forwarding, WireMock's internal HTTP client replaces the `Host` header with the recording target hostname. The OSS server at the base endpoint then cannot identify the bucket, computes the V4 signature with resource path `/` instead of `/bucketName/`, and returns `SignatureDoesNotMatch`.

This only affects operations that target a specific bucket (e.g., `createBucket`, `putObject`). Service-level operations like `listBuckets` work because they use the base endpoint with resource path `/` on both sides.

## Root Cause

1. OSS SDK signs the request with resource path `/bucketName/`
2. SDK sends HTTPS request to `bucketName.oss-cn-shanghai.aliyuncs.com`
3. WireMock MITM intercepts via CONNECT tunnel
4. WireMock forwards to recording target `oss-cn-shanghai.aliyuncs.com`, replacing the `Host` header
5. OSS server sees `Host: oss-cn-shanghai.aliyuncs.com` and path `/`, computes signature with `/`
6. Signature mismatch: client signed `/bucketName/`, server computed `/`

## Solution

Two changes work together to fix this:

### 1. `preserveHostHeader(true)` in WireMock configuration (`TestsUtil.java`)

This tells WireMock to preserve the original `Host` header when forwarding proxied requests. The OSS server receives `Host: bucketName.oss-cn-shanghai.aliyuncs.com`, correctly identifies the bucket, and computes a matching signature.

### 2. Native HTTP proxy via `ClientBuilderConfiguration` (IT test harnesses)

The IT tests configure the OSS SDK's built-in proxy support:

```java
clientConfig.setProxyHost(TestsUtil.WIREMOCK_HOST);
clientConfig.setProxyPort(port + 1); // WireMock's HTTP port
```

This uses standard HTTP CONNECT tunneling through WireMock's HTTP port (`port + 1`). The SDK also needs trust-all SSL configuration since WireMock terminates TLS with a self-signed certificate:

```java
clientConfig.setVerifySSLEnable(false);
clientConfig.setX509TrustManagers(new X509TrustManager[] {
    (X509TrustManager) TestsUtil.createTrustAllManager()[0]
});
```

## Why AWS and GCP Are Not Affected

- **AWS** uses `pathStyleAccessEnabled(true)` with `endpointOverride`, so bucket names go in the URL path (`s3.us-west-2.amazonaws.com/bucketName/key`). The `Host` header is always the base endpoint.
- **GCP** uses a single fixed endpoint (`storage.googleapis.com`) with bucket names in the URL path.

Neither provider uses virtual-hosted style subdomains, so `preserveHostHeader(true)` is a no-op for them.

## Approaches That Do Not Work

- **SLD/path-style** (`setSLDEnabled(true)`): OSS rejects path-style requests with "The bucket you are attempting to access must be addressed using OSS third level domain."
- **Socket-level redirect** (custom `SSLConnectionSocketFactory`): Redirects the TCP connection to WireMock but still results in MITM header modification.
- **Reflection-based proxy scheme change**: Apache HttpClient 4.x ignores the scheme on the proxy `HttpHost`.
