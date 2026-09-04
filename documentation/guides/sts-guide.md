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
| **Circuit Breaker** | ✅ Supported | ✅ Supported | ✅ Supported | Optional, provider-agnostic; disabled by default (see [Circuit Breaker](#circuit-breaker-optional)) |

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

---

## Circuit Breaker (optional)

`StsClient` can guard every provider call with a circuit breaker. It is **disabled by default** — if you never call `withCircuitBreakerConfig(...)`, the client behaves exactly as before. When enabled, the breaker protects your application from hammering an unhealthy token service: after enough failures it "opens" and rejects calls immediately for a cool-down window, then probes for recovery before closing again.

Only **retryable** failures (those signalling an unhealthy dependency, e.g. throttling or timeouts) count toward opening the breaker. Caller errors such as an invalid argument are non-retryable and never trip it.

```java
CircuitBreakerConfig breakerConfig = CircuitBreakerConfig.builder()
    .withFailureRateThreshold(30f)                             // open at ≥30% failures
    .withSlowCallRateThreshold(10f)                            // open at ≥10% slow calls
    .withSlowCallDurationThreshold(Duration.ofSeconds(120))    // a call ≥120s counts as slow
    .withMinimumNumberOfCalls(100)                             // evaluate only after 100 calls
    .withSlidingWindowSize(600)                                // 600-second time-based window
    .withWaitDurationInOpenState(Duration.ofSeconds(1))        // stay open 1s before probing
    .withPermittedNumberOfCallsInHalfOpenState(200)            // trial calls while half-open
    .build();

StsClient stsClient = StsClient.builder("aws")
    .withRegion("us-west-2")
    .withCircuitBreakerConfig(breakerConfig)
    .build();
```

When the breaker is open, calls fail fast with a non-retryable `CircuitBreakerOpenException`:

```java
try {
    CallerIdentity identity = stsClient.getCallerIdentity();
} catch (CircuitBreakerOpenException e) {
    // Breaker is open — back off and retry only after the wait duration elapses.
} catch (SubstrateSdkException e) {
    // Other provider errors.
}
```

### Configuration reference

The values shown above are **recommended starting points for a high-throughput workload**, not the defaults. Tune them to your own call volume and latency profile.

| Option | Meaning |
|--------|---------|
| `withFailureRateThreshold` | Percentage (0–100) of recorded failures at or above which the breaker opens. |
| `withSlowCallRateThreshold` | Percentage (0–100) of slow calls at or above which the breaker opens. |
| `withSlowCallDurationThreshold` | A call taking at least this long is counted as slow. |
| `withSlidingWindowSize` | Size of the time-based sliding window, in seconds. |
| `withMinimumNumberOfCalls` | Minimum recorded calls before the failure/slow rate is evaluated. |
| `withWaitDurationInOpenState` | How long the breaker stays open before transitioning to half-open. |
| `withPermittedNumberOfCallsInHalfOpenState` | Number of trial calls permitted while half-open. |
