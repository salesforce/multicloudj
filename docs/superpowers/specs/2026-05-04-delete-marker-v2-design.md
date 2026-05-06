# Delete Marker v2 — Exception-Path Design

## Summary

Surface delete marker information through `ResourceNotFoundException` exception metadata, not through `BlobMetadata` on the success path. The v1 approach (reading `response.deleteMarker()` from successful responses) was dead code — AWS and Ali only return `x-amz-delete-marker` / `x-oss-delete-marker` on error responses (404/405), never on 200.

Revert all v1 `BlobMetadata.deleteMarker` changes. Implement exception-path enrichment for AWS and GCP.

## Motivation

When a caller does `getMetadata(key)` on a deleted object, all providers throw `ResourceNotFoundException`. Callers currently cannot distinguish "object was deleted" from "object never existed." This distinction matters for audit, cleanup tooling, and user-facing error messages.

## Design

### Part 1: Generic metadata on `SubstrateSdkException`

Add to `SubstrateSdkException`:

```java
private final Map<String, String> metadata;
```

- All existing constructors initialize `metadata` to an empty unmodifiable map.
- Add a fluent method: `public SubstrateSdkException withMetadata(String key, String value)` — adds an entry and returns `this`.
- Add a getter: `public String getMetadata(String key)` — returns the value or null.
- Add a getter: `public Map<String, String> getAllMetadata()` — returns unmodifiable view.

### Part 2: Update `ExceptionHandler.handleAndPropagate` to preserve enriched exceptions

`BucketClient` methods catch `Throwable` and call `handleAndPropagate`, which always creates a **new** exception instance — losing any metadata set by the provider layer. Fix: add a guard at the top that rethrows `t` directly when it's already an instance of the target exception type.

```java
public static void handleAndPropagate(
    Class<? extends SubstrateSdkException> exceptionClass, Throwable t)
    throws SubstrateSdkException {
  // Preserve enriched exceptions thrown directly by provider implementations
  if (exceptionClass != null && exceptionClass.isInstance(t)) {
    throw (SubstrateSdkException) t;
  }
  // ... existing if-else chain unchanged ...
}
```

This benefits both AWS (Part 3) and GCP (Part 4), which both throw `ResourceNotFoundException` directly from their BlobStore layer.

### Part 3: AWS enrichment at `AwsBlobStore` layer (sync + async)

Handle delete marker detection in `AwsBlobStore` itself, not in the common exception layer.

Add a private helper method:

```java
private void throwIfDeleteMarker(S3Exception e) {
    if (e.statusCode() == 404) {
        String header = e.awsErrorDetails()
            .sdkHttpResponse()
            .firstMatchingHeader("x-amz-delete-marker")
            .orElse(null);
        if ("true".equals(header)) {
            throw new ResourceNotFoundException(e)
                .withMetadata("deleteMarker", "true");
        }
    }
}
```

Wrap `doGetMetadata` and all `doDownload` overloads in try-catch for `S3Exception`, calling `throwIfDeleteMarker(e)` before rethrowing. Non-delete-marker exceptions propagate normally.

The same pattern applies in `AwsAsyncBlobStore` for the async path.

### Part 4: GCP enrichment at `GcpBlobStore` layer

In `GcpBlobStore.getRequiredBlob(BlobId)`, when `storage.get(blobId)` returns null:

1. Make a second call: `storage.get(blobId, BlobGetOption.softDeleted(true))`
2. If that returns non-null, the object is soft-deleted — throw `ResourceNotFoundException` with `metadata("deleteMarker", "true")`
3. If that also returns null, the object never existed — throw plain `ResourceNotFoundException`

The second call only happens on the not-found path, so no performance impact on the happy path. The thrown `ResourceNotFoundException` is preserved through `BucketClient` thanks to the Part 2 guard in `handleAndPropagate`.

### Part 5: Revert v1 changes

- Remove `boolean deleteMarker` field from `BlobMetadata.java`
- Revert `.deleteMarker(...)` additions in `AwsTransformer.java`, `GcpTransformer.java`, `AliTransformer.java`
- Remove associated test methods

### Caller Experience

Uniform across all providers:

```java
try {
    bucketClient.getMetadata(key, versionId);
} catch (ResourceNotFoundException e) {
    if ("true".equals(e.getMetadata("deleteMarker"))) {
        // object was deleted
    } else {
        // object never existed
    }
}
```

### Files Changed

| File | Change |
|------|--------|
| `multicloudj-common/.../SubstrateSdkException.java` | Add `Map<String, String> metadata` field, `withMetadata()`, `getMetadata()` |
| `multicloudj-common/.../ExceptionHandler.java` | Add guard to rethrow enriched exceptions that are already the target type |
| `blob/blob-client/.../BlobMetadata.java` | Remove `deleteMarker` field |
| `blob/blob-aws/.../AwsBlobStore.java` | Add `throwIfDeleteMarker` helper; wrap `doGetMetadata`/`doDownload` in try-catch |
| `blob/blob-aws/.../async/AwsAsyncBlobStore.java` | Same pattern for async path |
| `blob/blob-aws/.../AwsTransformer.java` | Revert v1 `.deleteMarker()` additions |
| `blob/blob-gcp/.../GcpBlobStore.java` | In `getRequiredBlob`, check `softDeleted(true)` on null, enrich exception |
| `blob/blob-gcp/.../GcpTransformer.java` | Revert v1 `.deleteMarker()` addition |
| `blob/blob-ali/.../AliTransformer.java` | Revert v1 `.deleteMarker()` additions |

### Testing

- **SubstrateSdkException**: Test `withMetadata`/`getMetadata` round-trip, empty metadata by default, immutability.
- **ExceptionHandler**: Test that an enriched `ResourceNotFoundException` with metadata is rethrown as-is (not re-wrapped). Also test that non-matching exception types still go through the normal chain.
- **AWS unit tests**: Mock `S3Exception` with 404 + `x-amz-delete-marker: true` header, verify `AwsBlobStore.doGetMetadata` throws `ResourceNotFoundException` with `deleteMarker=true` metadata. Also test the false/absent case (exception without metadata).
- **GCP unit tests**: Mock `storage.get()` returning null, then `storage.get(blobId, softDeleted(true))` returning a blob — verify exception has `deleteMarker=true`. Also test both-null case.
- **Revert tests**: Remove all v1 delete marker test methods from transformer tests.

### Out of Scope

- Alibaba: Ali SDK's `ServiceException` does not expose HTTP response headers. Will revisit when feasible.
- List versions API: Separate feature, not part of this work.
