---
layout: default
title: Client-side Circuit Breaking
nav_order: 10
parent: Usage Guides
---

# Use your application's circuit breaker with MultiCloudJ

Circuit breaking is a concern of the consuming application or its infrastructure. Consumers choose
their own approach, such as Resilience4j, Failsafe, MicroProfile Fault Tolerance, or a service-mesh
policy. MultiCloudJ does not select a framework, implement a circuit breaker, or add a fault
tolerance library as an SDK dependency.

MultiCloudJ owns the cloud abstraction below that layer: normalized
[exceptions](../design/errors.html) and configuration of provider-native retries. This guide
explains how consumers can use those existing signals with a resilience solution of their choice.

| Responsibility | Owner |
| --- | --- |
| Cloud-agnostic exception types and retryability signals | MultiCloudJ |
| Configuration of provider-native retries through `RetryConfig` | MultiCloudJ |
| Circuit breaker framework, failure policy, thresholds, lifecycle, and fallback | Consuming application or its infrastructure |

The optional [Resilience4j](https://resilience4j.readme.io/docs/circuitbreaker) snippets below are
**consumer-application code**, illustrating one way to wrap the public SDK calls. They are not
MultiCloudJ APIs, SDK defaults, or a recommendation to choose Resilience4j over other options.

## Configure SDK retries and application circuit breaking separately

MultiCloudJ's existing
[`RetryConfig`](https://github.com/salesforce/multicloudj/blob/main/multicloudj-common/src/main/java/com/salesforce/multicloudj/common/retries/RetryConfig.java)
exposes retry settings that provider implementations translate to their native SDKs. Both
`BucketClient.builder(...)` and `StsClient.builder(...)` accept `withRetryConfig(...)`.
STS retry configuration requires MultiCloudJ 0.4.15 or later.

| Mechanism | Purpose |
| --- | --- |
| `RetryConfig` | Configure attempts, backoff, and supported timeouts within an SDK call |
| `SubstrateSdkException.isRetryable()` | Describe whether a failed call may succeed if retried; this flag does not perform retries |
| Application circuit breaker | Retain outcomes across calls and temporarily reject calls to a failing dependency |

An application can use SDK retries with or without a circuit breaker. Choose the resilience
framework or service-mesh policy that fits your application; the SDK does not select one.
Provider support for individual settings differs: Alibaba STS applies `attemptTimeout` to
request connect/read timeouts but does not apply `totalTimeout` as an overall deadline.

Define this retry configuration once and reuse it in the Blob and STS examples below. The sample
timing values are in milliseconds; tune them for your workload.

```java
import com.salesforce.multicloudj.common.retries.RetryConfig;

RetryConfig retryConfig = RetryConfig.builder()
    .mode(RetryConfig.Mode.EXPONENTIAL)
    .maxAttempts(3)
    .initialDelayMillis(100L)
    .multiplier(2.0)
    .maxDelayMillis(1000L)
    .attemptTimeout(2000L)
    .totalTimeout(10000L)
    .build();
```

## Optional example: add a dependency to your application

Add the circuit breaker library to **your application's** Maven dependencies, alongside the
MultiCloudJ client and provider modules you already use. This example uses Resilience4j 2.4.0,
which [targets Java 17](https://github.com/resilience4j/resilience4j/blob/v2.4.0/build.gradle):

```xml
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-circuitbreaker</artifactId>
    <version>2.4.0</version>
</dependency>
```

## Use the exception taxonomy to choose your failure policy

The SDK's normalized exception types let consumers distinguish dependency failure signals, such as
`DeadlineExceededException`, `ResourceExhaustedException`, and `UnknownException`, from caller or
resource errors such as `InvalidArgumentException` and `ResourceNotFoundException`. Consumers own
the decision about which failures should affect their breaker.

`SubstrateSdkException.isRetryable()` indicates whether an operation might succeed on retry.
The optional application example uses that existing signal as a starting policy:

| Exception or failure | Default example policy |
| --- | --- |
| `DeadlineExceededException` | Count as a failure |
| `ResourceExhaustedException` | Count as a failure |
| `UnknownException` | Count as a failure; investigate the underlying cause |
| `InvalidArgumentException`, `ResourceNotFoundException` | Ignore in breaker metrics; propagate to the caller |
| Other non-retryable SDK exceptions, including authorization failures | Ignore in breaker metrics; propagate to the caller |
| Exceptions outside `SubstrateSdkException` | Ignore in breaker metrics; propagate to the caller |

Provider retryability hints can override the exception type's default. For example, a
`ResourceExhaustedException` marked non-retryable is ignored by this policy. Retryability is
not a guarantee that the dependency is unhealthy: permanent quotas, permissions, and unknown
failures need application-specific decisions. Inspect causes and telemetry when tuning the policy.

Use both a recording predicate and an ignore predicate. In Resilience4j, an exception that does
not match `recordException` otherwise counts as a successful call. Ignoring it explicitly prevents
caller errors from diluting the dependency's failure rate. Ignored exceptions still propagate.

```java
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;

static boolean isDependencyFailure(Throwable failure) {
    return failure instanceof SubstrateSdkException
        && ((SubstrateSdkException) failure).isRetryable();
}

static CircuitBreaker newCircuitBreaker(String name) {
    CircuitBreakerConfig config = CircuitBreakerConfig.custom()
        .slidingWindowSize(10)
        .minimumNumberOfCalls(5)
        .failureRateThreshold(50)
        .waitDurationInOpenState(Duration.ofSeconds(30))
        .permittedNumberOfCallsInHalfOpenState(3)
        .recordException(failure -> isDependencyFailure(failure))
        .ignoreException(failure -> !isDependencyFailure(failure))
        .build();
    return CircuitBreaker.of(name, config);
}
```

The helper above belongs in the consuming application; it is not provided by MultiCloudJ.
These values demonstrate a small count-based window; choose them for your traffic and recovery
objectives. With this configuration, the failure rate is evaluated after five recorded calls.
A rate of at least 50% opens the breaker. Successful and failed recorded calls contribute to the
window; ignored exceptions do not.

## Optional application example: wrap Blob operations

Build and reuse your client and breaker at application startup. Keep separate breaker instances
for dependencies whose failures should be isolated, such as different services, regions, or
endpoints. Give each instance a useful name for metrics.

```java
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;

// providerId, region, bucketName, and key come from application configuration.
// Reuse retryConfig from the shared retry configuration above.
CircuitBreaker blobBreaker = newCircuitBreaker("blob-metadata");
try (BucketClient bucketClient = BucketClient.builder(providerId)
        .withRegion(region)
        .withBucket(bucketName)
        .withRetryConfig(retryConfig)
        .build()) {
    try {
        BlobMetadata metadata = blobBreaker.executeSupplier(
            () -> bucketClient.getMetadata(key, null));
        // Use metadata in the application.
    } catch (CallNotPermittedException openCircuit) {
        // No SDK call was made. Return an explicit unavailable result or use a safe cache.
        throw openCircuit;
    }
}
```

The try-with-resources block represents the client lifetime; put repeated operations inside it.
`maxAttempts(3)` includes the initial request, allowing at most two retries when the native SDK
considers the failure retryable. Timeout support and enforcement depend on the provider; for
example, the Alibaba Blob implementation does not apply `totalTimeout` as an overall deadline.
Omit `withRetryConfig(...)` to retain the native SDK retry defaults.
See the [Blob guide](blobstore-guide.html) for client configuration and other operations.

## Optional application example: wrap STS operations

Use a separate breaker for credential acquisition so that a Blob outage does not block STS calls.
Create and reuse the following objects in the application component that obtains credentials,
using the same `retryConfig` defined above:

```java
import com.salesforce.multicloudj.sts.client.StsClient;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;

CircuitBreaker stsBreaker = newCircuitBreaker("sts-assume-role");
StsClient stsClient = StsClient.builder(providerId)
    .withRegion(region)
    .withRetryConfig(retryConfig)
    .build();
AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
    .withRole(role)
    .withSessionName("circuit-breaker-example")
    .withExpiration(900)
    .build();

StsCredentials credentials = stsBreaker.executeSupplier(
    () -> stsClient.getAssumeRoleCredentials(request));
```

Supply the role identifier or service account and source credentials required by your provider.
See the [STS guide](sts-guide.html). Let `CallNotPermittedException` reach your application's
unavailable handling, as in the Blob example. Never fabricate credentials or return expired
credentials as a fallback. Do not log credential contents.

## Recovery, retries, and operation boundaries

- **Closed:** Calls run normally and the breaker records their outcomes.
- **Open:** Calls fail immediately with Resilience4j's `CallNotPermittedException`, before invoking
  MultiCloudJ. This is an application-library exception, not a MultiCloudJ exception.
- **Half-open:** After the 30-second wait, the next call starts a probe window of three calls.
  Successful probes close the breaker; a failure rate meeting the threshold reopens it.
  The example uses the library's default of transitioning on a new call, without a background timer.

Keep breaker instances alive across requests. Creating a new breaker per request loses the
failure history. Monitor state changes, rejected calls, failure rates, and ignored exceptions;
do not automatically treat every rejected call as another backend failure.

Provider SDK retry and timeout settings, including supported `RetryConfig` settings, apply inside
each wrapped SDK call. The breaker sees the final result after those retries, not each network
attempt, and it does not cancel a
call already in progress. Configure timeouts and retry budgets separately. Adding an application
retry loop can multiply attempts; do not retry an open circuit in a tight loop. Retrying writes
also requires an idempotency strategy.

Wrap the work whose completion you need to measure. The examples use synchronous, eager calls.
For a download stream or lazy iterator, creating the stream or iterator may finish before the
network work does; consumption must be inside the protected operation. For asynchronous calls,
use your library's completion-stage integration so failures on completion are recorded instead
of treating future creation as success. Check how that integration handles wrapper exceptions
before adapting the synchronous predicate.
