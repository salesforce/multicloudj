# Delete Marker v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface delete marker information through `ResourceNotFoundException` exception metadata instead of the (dead-code) v1 `BlobMetadata.deleteMarker` field.

**Architecture:** Add a generic metadata `Map<String, String>` to `SubstrateSdkException`. Provider-level BlobStore classes (AWS, GCP) detect delete markers on the error path and throw enriched `ResourceNotFoundException`. A guard in `ExceptionHandler.handleAndPropagate` preserves these enriched exceptions through `BucketClient`/`AsyncBucketClient`. Revert all v1 `BlobMetadata.deleteMarker` changes.

**Tech Stack:** Java 11, Maven, Lombok, Mockito 5, JUnit 5, AWS SDK v2, GCP Cloud Storage SDK

**Spec:** `docs/superpowers/specs/2026-05-04-delete-marker-v2-design.md`

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `multicloudj-common/src/main/java/.../SubstrateSdkException.java` | Modify | Add `metadata` map, `withMetadata()`, `getMetadata()`, `getAllMetadata()` |
| `multicloudj-common/src/test/java/.../SubstrateSdkExceptionTest.java` | Modify | Add metadata tests |
| `multicloudj-common/src/main/java/.../ExceptionHandler.java` | Modify | Add guard to preserve already-typed exceptions |
| `multicloudj-common/src/test/java/.../ExceptionHandlerTest.java` | Modify | Add guard tests |
| `blob/blob-client/src/main/java/.../BlobMetadata.java` | Modify | Remove `deleteMarker` field |
| `blob/blob-aws/src/main/java/.../AwsBlobStore.java` | Modify | Add `throwIfDeleteMarker` helper, wrap methods |
| `blob/blob-aws/src/test/java/.../AwsBlobStoreTest.java` | Modify | Add delete marker tests |
| `blob/blob-aws/src/main/java/.../async/AwsAsyncBlobStore.java` | Modify | Add async delete marker enrichment |
| `blob/blob-aws/src/main/java/.../AwsTransformer.java` | Modify | Revert v1 `.deleteMarker()` additions |
| `blob/blob-aws/src/test/java/.../AwsTransformerTest.java` | Modify | Remove v1 delete marker tests |
| `blob/blob-gcp/src/main/java/.../GcpBlobStore.java` | Modify | Enrich `getRequiredBlob` with `softDeleted(true)` check |
| `blob/blob-gcp/src/test/java/.../GcpBlobStoreTest.java` | Modify | Add delete marker tests |
| `blob/blob-gcp/src/main/java/.../GcpTransformer.java` | Modify | Revert v1 `.deleteMarker()` addition |
| `blob/blob-gcp/src/test/java/.../GcpTransformerTest.java` | Modify | Remove v1 delete marker tests |
| `blob/blob-ali/src/main/java/.../AliTransformer.java` | Modify | Revert v1 `.deleteMarker()` additions |
| `blob/blob-ali/src/test/java/.../AliTransformerTest.java` | Modify | Remove v1 delete marker tests |

---

### Task 1: Add metadata support to SubstrateSdkException

**Files:**
- Modify: `multicloudj-common/src/main/java/com/salesforce/multicloudj/common/exceptions/SubstrateSdkException.java`
- Modify: `multicloudj-common/src/test/java/com/salesforce/multicloudj/common/exceptions/SubstrateSdkExceptionTest.java`

- [ ] **Step 1: Write failing tests for metadata support**

Add these tests to `SubstrateSdkExceptionTest.java`:

```java
import java.util.Map;

@Test
public void testMetadataEmptyByDefault() {
  SubstrateSdkException exception = new SubstrateSdkException("test");
  assertNull(exception.getMetadata("anyKey"));
  assertTrue(exception.getAllMetadata().isEmpty());
}

@Test
public void testWithMetadataRoundTrip() {
  SubstrateSdkException exception = new SubstrateSdkException("test")
      .withMetadata("deleteMarker", "true");
  assertEquals("true", exception.getMetadata("deleteMarker"));
  assertEquals(Map.of("deleteMarker", "true"), exception.getAllMetadata());
}

@Test
public void testWithMetadataReturnsSameInstance() {
  SubstrateSdkException exception = new SubstrateSdkException("test");
  SubstrateSdkException same = exception.withMetadata("key", "value");
  assertSame(exception, same);
}

@Test
public void testGetAllMetadataReturnsUnmodifiableView() {
  SubstrateSdkException exception = new SubstrateSdkException("test")
      .withMetadata("key", "value");
  Map<String, String> metadata = exception.getAllMetadata();
  assertThrows(UnsupportedOperationException.class, () -> metadata.put("other", "val"));
}

@Test
public void testGetMetadataReturnsNullForMissingKey() {
  SubstrateSdkException exception = new SubstrateSdkException("test")
      .withMetadata("key", "value");
  assertNull(exception.getMetadata("nonexistent"));
}
```

Also add these imports at the top of the file:

```java
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Map;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl multicloudj-common -Dtest=SubstrateSdkExceptionTest`
Expected: FAIL — `withMetadata`, `getMetadata`, `getAllMetadata` methods don't exist yet.

- [ ] **Step 3: Implement metadata support in SubstrateSdkException**

Replace the full contents of `SubstrateSdkException.java` with:

```java
package com.salesforce.multicloudj.common.exceptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SubstrateSdkException extends RuntimeException {

  private Map<String, String> metadata;

  public SubstrateSdkException() {
    super();
  }

  public SubstrateSdkException(String message, Throwable cause) {
    super(message, cause);
  }

  public SubstrateSdkException(String message) {
    super(message);
  }

  public SubstrateSdkException(Throwable cause) {
    super(cause);
  }

  @SuppressWarnings("unchecked")
  public <T extends SubstrateSdkException> T withMetadata(String key, String value) {
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    this.metadata.put(key, value);
    return (T) this;
  }

  public String getMetadata(String key) {
    if (this.metadata == null) {
      return null;
    }
    return this.metadata.get(key);
  }

  public Map<String, String> getAllMetadata() {
    if (this.metadata == null) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(this.metadata);
  }
}
```

Key design choices:
- `metadata` is lazily initialized (null until first `withMetadata` call) to avoid allocating a map for the common case where no metadata is set.
- `withMetadata` returns `<T extends SubstrateSdkException>` so callers can chain on subtypes like `ResourceNotFoundException` without casting.
- `getAllMetadata()` returns an unmodifiable view.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl multicloudj-common -Dtest=SubstrateSdkExceptionTest`
Expected: PASS — all existing + new tests pass.

- [ ] **Step 5: Commit**

```bash
git add multicloudj-common/src/main/java/com/salesforce/multicloudj/common/exceptions/SubstrateSdkException.java multicloudj-common/src/test/java/com/salesforce/multicloudj/common/exceptions/SubstrateSdkExceptionTest.java
git commit -m "feat(common): add metadata support to SubstrateSdkException"
```

---

### Task 2: Add guard in ExceptionHandler to preserve enriched exceptions

**Files:**
- Modify: `multicloudj-common/src/main/java/com/salesforce/multicloudj/common/exceptions/ExceptionHandler.java`
- Modify: `multicloudj-common/src/test/java/com/salesforce/multicloudj/common/exceptions/ExceptionHandlerTest.java`

- [ ] **Step 1: Write failing tests for the guard**

Add these tests to `ExceptionHandlerTest.java`:

```java
@Test
public void testPreservesEnrichedResourceNotFoundException() {
  ResourceNotFoundException enriched = new ResourceNotFoundException("deleted");
  enriched.withMetadata("deleteMarker", "true");

  ResourceNotFoundException thrown = assertThrows(
      ResourceNotFoundException.class,
      () -> ExceptionHandler.handleAndPropagate(ResourceNotFoundException.class, enriched));

  // Must be the SAME instance, not a re-wrapped copy
  assertSame(enriched, thrown);
  assertEquals("true", thrown.getMetadata("deleteMarker"));
}

@Test
public void testPreservesEnrichedSubtypeException() {
  UnAuthorizedException enriched = new UnAuthorizedException("no access");
  enriched.withMetadata("reason", "expired");

  UnAuthorizedException thrown = assertThrows(
      UnAuthorizedException.class,
      () -> ExceptionHandler.handleAndPropagate(UnAuthorizedException.class, enriched));

  assertSame(enriched, thrown);
  assertEquals("expired", thrown.getMetadata("reason"));
}

@Test
public void testNonMatchingTypeStillWraps() {
  // t is a plain Throwable, exceptionClass is ResourceNotFoundException
  // Guard should NOT fire — Throwable is not a ResourceNotFoundException
  Throwable t = new Throwable("not found");
  assertThrows(
      ResourceNotFoundException.class,
      () -> ExceptionHandler.handleAndPropagate(ResourceNotFoundException.class, t));
}
```

Also add these imports:

```java
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl multicloudj-common -Dtest=ExceptionHandlerTest`
Expected: `testPreservesEnrichedResourceNotFoundException` FAILS — the current code creates `new ResourceNotFoundException(t)`, losing metadata. `assertSame` fails because it's a different instance.

- [ ] **Step 3: Add the guard to handleAndPropagate**

In `ExceptionHandler.java`, add the guard at the top of the method, before the null check:

```java
public static void handleAndPropagate(
    Class<? extends SubstrateSdkException> exceptionClass, Throwable t)
    throws SubstrateSdkException {
  if (exceptionClass != null && exceptionClass.isInstance(t)) {
    throw (SubstrateSdkException) t;
  }
  if (exceptionClass == null) {
    throw new UnknownException(t);
  } else if (ResourceAlreadyExistsException.class.isAssignableFrom(exceptionClass)) {
    // ... rest of existing chain unchanged ...
```

The full method body becomes:
```java
public static void handleAndPropagate(
    Class<? extends SubstrateSdkException> exceptionClass, Throwable t)
    throws SubstrateSdkException {
  if (exceptionClass != null && exceptionClass.isInstance(t)) {
    throw (SubstrateSdkException) t;
  }
  if (exceptionClass == null) {
    throw new UnknownException(t);
  } else if (ResourceAlreadyExistsException.class.isAssignableFrom(exceptionClass)) {
    throw new ResourceAlreadyExistsException(t);
  } else if (ResourceNotFoundException.class.isAssignableFrom(exceptionClass)) {
    throw new ResourceNotFoundException(t);
  } else if (ResourceConflictException.class.isAssignableFrom(exceptionClass)) {
    throw new ResourceConflictException(t);
  } else if (UnAuthorizedException.class.isAssignableFrom(exceptionClass)) {
    throw new UnAuthorizedException(t);
  } else if (ResourceExhaustedException.class.isAssignableFrom(exceptionClass)) {
    throw new ResourceExhaustedException(t);
  } else if (InvalidArgumentException.class.isAssignableFrom(exceptionClass)) {
    throw new InvalidArgumentException(t);
  } else if (FailedPreconditionException.class.isAssignableFrom(exceptionClass)) {
    throw new FailedPreconditionException(t);
  } else if (TransactionFailedException.class.isAssignableFrom(exceptionClass)) {
    throw new TransactionFailedException(t);
  } else if (UnknownException.class.isAssignableFrom(exceptionClass)) {
    throw new UnknownException(t);
  } else if (SubstrateSdkException.class.isAssignableFrom(exceptionClass)) {
    throw (SubstrateSdkException) t;
  } else {
    throw new UnknownException(t);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl multicloudj-common -Dtest=ExceptionHandlerTest`
Expected: PASS — all existing + new tests pass.

- [ ] **Step 5: Commit**

```bash
git add multicloudj-common/src/main/java/com/salesforce/multicloudj/common/exceptions/ExceptionHandler.java multicloudj-common/src/test/java/com/salesforce/multicloudj/common/exceptions/ExceptionHandlerTest.java
git commit -m "feat(common): preserve enriched exceptions in ExceptionHandler guard"
```

---

### Task 3: Revert v1 BlobMetadata.deleteMarker and all transformer changes

**Files:**
- Modify: `blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/driver/BlobMetadata.java`
- Modify: `blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsTransformer.java`
- Modify: `blob/blob-aws/src/test/java/com/salesforce/multicloudj/blob/aws/AwsTransformerTest.java`
- Modify: `blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpTransformer.java`
- Modify: `blob/blob-gcp/src/test/java/com/salesforce/multicloudj/blob/gcp/GcpTransformerTest.java`
- Modify: `blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliTransformer.java`
- Modify: `blob/blob-ali/src/test/java/com/salesforce/multicloudj/blob/ali/AliTransformerTest.java`

- [ ] **Step 1: Remove `deleteMarker` field from BlobMetadata.java**

In `blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/driver/BlobMetadata.java`, remove line 39:

```java
  private final boolean deleteMarker;
```

The file should go from:
```java
  private final String contentType;

  private final boolean deleteMarker;

  /** Object lock information for this blob. null if object lock is not configured. */
  private final ObjectLockInfo objectLockInfo;
```

To:
```java
  private final String contentType;

  /** Object lock information for this blob. null if object lock is not configured. */
  private final ObjectLockInfo objectLockInfo;
```

- [ ] **Step 2: Revert AwsTransformer.java — remove `.deleteMarker()` from all 3 methods**

In `blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsTransformer.java`:

**In `toMetadata` (around line 420):** Remove `.deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))` from the `BlobMetadata.builder()` chain. Change:

```java
                .contentType(response.contentType())
                .deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))
                .objectLockInfo(objectLockInfo)
```

To:

```java
                .contentType(response.contentType())
                .objectLockInfo(objectLockInfo)
```

**In first `toDownloadResponse` (around line 322):** Remove `.deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))`. Change:

```java
                .contentType(response.contentType())
                .deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))
                .build())
```

To:

```java
                .contentType(response.contentType())
                .build())
```

**In second `toDownloadResponse` (around line 343):** Same removal. Change:

```java
                .contentType(response.contentType())
                .deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))
                .build())
```

To:

```java
                .contentType(response.contentType())
                .build())
```

- [ ] **Step 3: Remove v1 tests from AwsTransformerTest.java**

In `blob/blob-aws/src/test/java/com/salesforce/multicloudj/blob/aws/AwsTransformerTest.java`, remove these 4 test methods entirely:

1. `testToDownloadResponse_WithDeleteMarkerTrue` (lines 328-343)
2. `testToDownloadResponse_WithDeleteMarkerFalse` (lines 345-360)
3. `testToMetadata_WithDeleteMarkerTrue` (lines 435-447)
4. `testToMetadata_WithDeleteMarkerFalse` (lines 449-460)

- [ ] **Step 4: Revert GcpTransformer.java — remove `.deleteMarker()` from `toBlobMetadata`**

In `blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpTransformer.java`, around line 214, remove `.deleteMarker(blob.getSoftDeleteTime() != null)`. Change:

```java
        .contentType(blob.getContentType())
        .deleteMarker(blob.getSoftDeleteTime() != null)
        .objectLockInfo(objectLockInfo)
```

To:

```java
        .contentType(blob.getContentType())
        .objectLockInfo(objectLockInfo)
```

- [ ] **Step 5: Remove v1 tests from GcpTransformerTest.java**

In `blob/blob-gcp/src/test/java/com/salesforce/multicloudj/blob/gcp/GcpTransformerTest.java`, remove these 2 test methods entirely:

1. `testToBlobMetadata_WithSoftDeleteTime` (lines 627-642)
2. `testToBlobMetadata_WithoutSoftDeleteTime` (lines 644-658)

- [ ] **Step 6: Revert AliTransformer.java — remove all `.deleteMarker()` additions**

In `blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliTransformer.java`:

**In `toDownloadResponse(OSSObject)` (around line 143-161):** Remove the `deleteMarker` local variable and `.deleteMarker(deleteMarker)` from the builder. Change:

```java
  public DownloadResponse toDownloadResponse(OSSObject ossObject) {
    boolean deleteMarker = "true".equals(
        String.valueOf(ossObject.getObjectMetadata().getRawMetadata().get("x-oss-delete-marker")));
    return DownloadResponse.builder()
        .key(ossObject.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(ossObject.getKey())
                .versionId(ossObject.getObjectMetadata().getVersionId())
                .eTag(ossObject.getObjectMetadata().getETag())
                .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                .createdTime(ossObject.getObjectMetadata().getLastModified().toInstant())
                .metadata(ossObject.getObjectMetadata().getUserMetadata())
                .objectSize(ossObject.getObjectMetadata().getContentLength())
                .contentType(ossObject.getObjectMetadata().getContentType())
                .deleteMarker(deleteMarker)
                .build())
        .build();
  }
```

To:

```java
  public DownloadResponse toDownloadResponse(OSSObject ossObject) {
    return DownloadResponse.builder()
        .key(ossObject.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(ossObject.getKey())
                .versionId(ossObject.getObjectMetadata().getVersionId())
                .eTag(ossObject.getObjectMetadata().getETag())
                .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                .createdTime(ossObject.getObjectMetadata().getLastModified().toInstant())
                .metadata(ossObject.getObjectMetadata().getUserMetadata())
                .objectSize(ossObject.getObjectMetadata().getContentLength())
                .contentType(ossObject.getObjectMetadata().getContentType())
                .build())
        .build();
  }
```

**In `toDownloadResponse(OSSObject, InputStream)` (around line 163-182):** Same removal. Change:

```java
  public DownloadResponse toDownloadResponse(OSSObject ossObject, InputStream inputStream) {
    boolean deleteMarker = "true".equals(
        String.valueOf(ossObject.getObjectMetadata().getRawMetadata().get("x-oss-delete-marker")));
    return DownloadResponse.builder()
        .key(ossObject.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(ossObject.getKey())
                .versionId(ossObject.getObjectMetadata().getVersionId())
                .eTag(ossObject.getObjectMetadata().getETag())
                .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                .createdTime(ossObject.getObjectMetadata().getLastModified().toInstant())
                .metadata(ossObject.getObjectMetadata().getUserMetadata())
                .objectSize(ossObject.getObjectMetadata().getContentLength())
                .contentType(ossObject.getObjectMetadata().getContentType())
                .deleteMarker(deleteMarker)
                .build())
        .inputStream(inputStream)
        .build();
  }
```

To:

```java
  public DownloadResponse toDownloadResponse(OSSObject ossObject, InputStream inputStream) {
    return DownloadResponse.builder()
        .key(ossObject.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(ossObject.getKey())
                .versionId(ossObject.getObjectMetadata().getVersionId())
                .eTag(ossObject.getObjectMetadata().getETag())
                .lastModified(ossObject.getObjectMetadata().getLastModified().toInstant())
                .createdTime(ossObject.getObjectMetadata().getLastModified().toInstant())
                .metadata(ossObject.getObjectMetadata().getUserMetadata())
                .objectSize(ossObject.getObjectMetadata().getContentLength())
                .contentType(ossObject.getObjectMetadata().getContentType())
                .build())
        .inputStream(inputStream)
        .build();
  }
```

**In `toBlobMetadata(String, ObjectMetadata)` (around line 229-246):** Remove the `deleteMarker` local variable and `.deleteMarker(deleteMarker)`. Change:

```java
  public BlobMetadata toBlobMetadata(String key, ObjectMetadata metadata) {
    long objectSize = metadata.getContentLength();
    Map<String, String> rawMetadata = metadata.getUserMetadata();
    boolean deleteMarker =
        "true".equals(String.valueOf(metadata.getRawMetadata().get("x-oss-delete-marker")));
    return BlobMetadata.builder()
        .key(key)
        .versionId(metadata.getVersionId())
        .eTag(metadata.getETag())
        .objectSize(objectSize)
        .metadata(rawMetadata)
        .lastModified(metadata.getLastModified().toInstant())
        .createdTime(metadata.getLastModified().toInstant())
        .md5(HexUtil.convertToBytes(metadata.getContentMD5()))
        .contentType(metadata.getContentType())
        .deleteMarker(deleteMarker)
        .build();
  }
```

To:

```java
  public BlobMetadata toBlobMetadata(String key, ObjectMetadata metadata) {
    long objectSize = metadata.getContentLength();
    Map<String, String> rawMetadata = metadata.getUserMetadata();
    return BlobMetadata.builder()
        .key(key)
        .versionId(metadata.getVersionId())
        .eTag(metadata.getETag())
        .objectSize(objectSize)
        .metadata(rawMetadata)
        .lastModified(metadata.getLastModified().toInstant())
        .createdTime(metadata.getLastModified().toInstant())
        .md5(HexUtil.convertToBytes(metadata.getContentMD5()))
        .contentType(metadata.getContentType())
        .build();
  }
```

- [ ] **Step 7: Remove v1 tests from AliTransformerTest.java**

In `blob/blob-ali/src/test/java/com/salesforce/multicloudj/blob/ali/AliTransformerTest.java`, remove these 4 test methods entirely:

1. `testToDownloadResponse_WithDeleteMarkerTrue` (lines 227-246)
2. `testToDownloadResponse_WithDeleteMarkerFalse` (lines 248-266)
3. `testToBlobMetadata_WithDeleteMarkerTrue` (lines 364-381)
4. `testToBlobMetadata_WithDeleteMarkerFalse` (lines 383-399)

- [ ] **Step 8: Build to verify all reverts compile**

Run: `mvn clean install -DskipTests`
Expected: BUILD SUCCESS — no compilation errors.

- [ ] **Step 9: Run all affected tests**

Run: `mvn test -pl blob/blob-aws -Dtest=AwsTransformerTest && mvn test -pl blob/blob-gcp -Dtest=GcpTransformerTest && mvn test -pl blob/blob-ali -Dtest=AliTransformerTest`
Expected: PASS — all remaining tests pass, v1 delete marker tests are gone.

- [ ] **Step 10: Commit**

```bash
git add blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/driver/BlobMetadata.java blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsTransformer.java blob/blob-aws/src/test/java/com/salesforce/multicloudj/blob/aws/AwsTransformerTest.java blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpTransformer.java blob/blob-gcp/src/test/java/com/salesforce/multicloudj/blob/gcp/GcpTransformerTest.java blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliTransformer.java blob/blob-ali/src/test/java/com/salesforce/multicloudj/blob/ali/AliTransformerTest.java
git commit -m "revert: remove v1 BlobMetadata.deleteMarker field and all transformer mappings"
```

---

### Task 4: AWS sync delete marker enrichment in AwsBlobStore

**Files:**
- Modify: `blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java`
- Modify: `blob/blob-aws/src/test/java/com/salesforce/multicloudj/blob/aws/AwsBlobStoreTest.java`

- [ ] **Step 1: Write failing tests for delete marker enrichment**

Add these tests to `AwsBlobStoreTest.java`. Add the `ResourceNotFoundException` import if not present:

```java
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import software.amazon.awssdk.http.SdkHttpResponse;
```

```java
@Test
void testDoGetMetadata_DeleteMarkerTrue() {
  // Build an S3Exception with 404 status and x-amz-delete-marker: true header
  SdkHttpResponse httpResponse = SdkHttpResponse.builder()
      .statusCode(404)
      .putHeader("x-amz-delete-marker", "true")
      .build();
  AwsErrorDetails errorDetails = AwsErrorDetails.builder()
      .errorCode("NoSuchKey")
      .sdkHttpResponse(httpResponse)
      .build();
  S3Exception s3e = (S3Exception) S3Exception.builder()
      .awsErrorDetails(errorDetails)
      .statusCode(404)
      .message("Not Found")
      .build();

  when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(s3e);

  ResourceNotFoundException thrown = assertThrows(
      ResourceNotFoundException.class,
      () -> aws.doGetMetadata("deleted-key", "v1"));

  assertEquals("true", thrown.getMetadata("deleteMarker"));
}

@Test
void testDoGetMetadata_404WithoutDeleteMarker() {
  SdkHttpResponse httpResponse = SdkHttpResponse.builder()
      .statusCode(404)
      .build();
  AwsErrorDetails errorDetails = AwsErrorDetails.builder()
      .errorCode("NoSuchKey")
      .sdkHttpResponse(httpResponse)
      .build();
  S3Exception s3e = (S3Exception) S3Exception.builder()
      .awsErrorDetails(errorDetails)
      .statusCode(404)
      .message("Not Found")
      .build();

  when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(s3e);

  S3Exception thrown = assertThrows(
      S3Exception.class,
      () -> aws.doGetMetadata("nonexistent-key", null));
  assertEquals(404, thrown.statusCode());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl blob/blob-aws -Dtest=AwsBlobStoreTest#testDoGetMetadata_DeleteMarkerTrue+testDoGetMetadata_404WithoutDeleteMarker`
Expected: FAIL — `doGetMetadata` does not catch `S3Exception` yet.

- [ ] **Step 3: Add `throwIfDeleteMarker` helper and wrap `doGetMetadata`**

In `blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java`:

First, add the import for `ResourceNotFoundException`:

```java
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
```

Add a private helper method (after the `close()` method, before the `Builder` class):

```java
  private void throwIfDeleteMarker(S3Exception e) {
    if (e.statusCode() == 404
        && e.awsErrorDetails() != null
        && e.awsErrorDetails().sdkHttpResponse() != null) {
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

Then wrap `doGetMetadata` (currently at line 328-332):

Change:

```java
  @Override
  protected BlobMetadata doGetMetadata(String key, String versionId) {
    HeadObjectRequest request = transformer.toHeadRequest(key, versionId);
    HeadObjectResponse response = s3Client.headObject(request);
    return transformer.toMetadata(response, key);
  }
```

To:

```java
  @Override
  protected BlobMetadata doGetMetadata(String key, String versionId) {
    try {
      HeadObjectRequest request = transformer.toHeadRequest(key, versionId);
      HeadObjectResponse response = s3Client.headObject(request);
      return transformer.toMetadata(response, key);
    } catch (S3Exception e) {
      throwIfDeleteMarker(e);
      throw e;
    }
  }
```

Also wrap all `doDownload` overloads with the same pattern. Each one should catch `S3Exception`, call `throwIfDeleteMarker(e)`, then rethrow.

**`doDownload(DownloadRequest, OutputStream)` (currently at line 203-209):**

Change:

```java
  @Override
  protected DownloadResponse doDownload(
      DownloadRequest downloadRequest, OutputStream outputStream) {
    GetObjectRequest request = transformer.toRequest(downloadRequest);
    GetObjectResponse response =
        s3Client.getObject(request, ResponseTransformer.toOutputStream(outputStream));
    return transformer.toDownloadResponse(downloadRequest, response);
  }
```

To:

```java
  @Override
  protected DownloadResponse doDownload(
      DownloadRequest downloadRequest, OutputStream outputStream) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      GetObjectResponse response =
          s3Client.getObject(request, ResponseTransformer.toOutputStream(outputStream));
      return transformer.toDownloadResponse(downloadRequest, response);
    } catch (S3Exception e) {
      throwIfDeleteMarker(e);
      throw e;
    }
  }
```

**`doDownload(DownloadRequest, ByteArray)` (currently at line 219-226):**

Change:

```java
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray) {
    GetObjectRequest request = transformer.toRequest(downloadRequest);
    ResponseBytes<GetObjectResponse> responseBytes =
        s3Client.getObject(request, ResponseTransformer.toBytes());
    byteArray.setBytes(responseBytes.asByteArray());
    GetObjectResponse response = responseBytes.response();
    return transformer.toDownloadResponse(downloadRequest, response);
  }
```

To:

```java
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      ResponseBytes<GetObjectResponse> responseBytes =
          s3Client.getObject(request, ResponseTransformer.toBytes());
      byteArray.setBytes(responseBytes.asByteArray());
      GetObjectResponse response = responseBytes.response();
      return transformer.toDownloadResponse(downloadRequest, response);
    } catch (S3Exception e) {
      throwIfDeleteMarker(e);
      throw e;
    }
  }
```

**`doDownload(DownloadRequest, File)` (currently at line 236-240):**

Change:

```java
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, File file) {
    GetObjectRequest request = transformer.toRequest(downloadRequest);
    GetObjectResponse response = s3Client.getObject(request, ResponseTransformer.toFile(file));
    return transformer.toDownloadResponse(downloadRequest, response);
  }
```

To:

```java
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, File file) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      GetObjectResponse response = s3Client.getObject(request, ResponseTransformer.toFile(file));
      return transformer.toDownloadResponse(downloadRequest, response);
    } catch (S3Exception e) {
      throwIfDeleteMarker(e);
      throw e;
    }
  }
```

**`doDownload(DownloadRequest, Path)` (currently at line 250-254):**

Change:

```java
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, Path path) {
    GetObjectRequest request = transformer.toRequest(downloadRequest);
    GetObjectResponse response = s3Client.getObject(request, ResponseTransformer.toFile(path));
    return transformer.toDownloadResponse(downloadRequest, response);
  }
```

To:

```java
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, Path path) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      GetObjectResponse response = s3Client.getObject(request, ResponseTransformer.toFile(path));
      return transformer.toDownloadResponse(downloadRequest, response);
    } catch (S3Exception e) {
      throwIfDeleteMarker(e);
      throw e;
    }
  }
```

**`doDownload(DownloadRequest)` (currently at line 264-269):**

Change:

```java
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest) {
    GetObjectRequest request = transformer.toRequest(downloadRequest);
    ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(request);
    return transformer.toDownloadResponse(
        downloadRequest, responseInputStream.response(), responseInputStream);
  }
```

To:

```java
  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest) {
    try {
      GetObjectRequest request = transformer.toRequest(downloadRequest);
      ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(request);
      return transformer.toDownloadResponse(
          downloadRequest, responseInputStream.response(), responseInputStream);
    } catch (S3Exception e) {
      throwIfDeleteMarker(e);
      throw e;
    }
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl blob/blob-aws -Dtest=AwsBlobStoreTest`
Expected: PASS — all tests pass including new delete marker tests.

- [ ] **Step 5: Commit**

```bash
git add blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsBlobStore.java blob/blob-aws/src/test/java/com/salesforce/multicloudj/blob/aws/AwsBlobStoreTest.java
git commit -m "feat(blob-aws): detect delete markers from S3Exception headers in AwsBlobStore"
```

---

### Task 5: AWS async delete marker enrichment in AwsAsyncBlobStore

**Files:**
- Modify: `blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/async/AwsAsyncBlobStore.java`

The async path returns `CompletableFuture`. When S3 returns a 404 for a deleted object, the `S3AsyncClient` completes the future exceptionally with an `S3Exception`. We need to intercept this in the future chain, check for the `x-amz-delete-marker` header, and if present, replace the exception with an enriched `ResourceNotFoundException`.

- [ ] **Step 1: Add the async enrichment helper**

In `blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/async/AwsAsyncBlobStore.java`:

Add imports:

```java
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import java.util.concurrent.CompletionException;
```

Add a private helper method (after the `close()` method, before the `builder()` method):

```java
  private <T> CompletableFuture<T> enrichDeleteMarker(CompletableFuture<T> future) {
    return future.exceptionally(ex -> {
      Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
      if (cause instanceof S3Exception) {
        S3Exception s3e = (S3Exception) cause;
        if (s3e.statusCode() == 404
            && s3e.awsErrorDetails() != null
            && s3e.awsErrorDetails().sdkHttpResponse() != null) {
          String header = s3e.awsErrorDetails()
              .sdkHttpResponse()
              .firstMatchingHeader("x-amz-delete-marker")
              .orElse(null);
          if ("true".equals(header)) {
            throw new ResourceNotFoundException(s3e)
                .withMetadata("deleteMarker", "true");
          }
        }
      }
      if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }
      throw new CompletionException(ex);
    });
  }
```

- [ ] **Step 2: Wrap `doGetMetadata` and all `doDownload` overloads**

**`doGetMetadata` (line 239-241):**

Change:

```java
  @Override
  protected CompletableFuture<BlobMetadata> doGetMetadata(String key, String versionId) {
    var request = transformer.toHeadRequest(key, versionId);
    return client.headObject(request).thenApply(response -> transformer.toMetadata(response, key));
  }
```

To:

```java
  @Override
  protected CompletableFuture<BlobMetadata> doGetMetadata(String key, String versionId) {
    var request = transformer.toHeadRequest(key, versionId);
    return enrichDeleteMarker(
        client.headObject(request).thenApply(response -> transformer.toMetadata(response, key)));
  }
```

**`doDownload(DownloadRequest, OutputStream)` (line 144-158):**

Change:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, OutputStream outputStream) {
    return client
        .getObject(transformer.toRequest(request), AsyncResponseTransformer.toBlockingInputStream())
        .thenApply(
            response -> {
              try (response) {
                response.transferTo(outputStream);
              } catch (IOException e) {
                throw new SubstrateSdkException(
                    "Request failed while transforming to output stream", e);
              }
              return transformer.toDownloadResponse(request, response.response());
            });
  }
```

To:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, OutputStream outputStream) {
    return enrichDeleteMarker(client
        .getObject(transformer.toRequest(request), AsyncResponseTransformer.toBlockingInputStream())
        .thenApply(
            response -> {
              try (response) {
                response.transferTo(outputStream);
              } catch (IOException e) {
                throw new SubstrateSdkException(
                    "Request failed while transforming to output stream", e);
              }
              return transformer.toDownloadResponse(request, response.response());
            }));
  }
```

**`doDownload(DownloadRequest, ByteArray)` (line 161-170):**

Change:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, ByteArray byteArray) {
    return client
        .getObject(transformer.toRequest(request), AsyncResponseTransformer.toBytes())
        .thenApply(
            responseBytes -> {
              byteArray.setBytes(responseBytes.asByteArray());
              return transformer.toDownloadResponse(request, responseBytes.response());
            });
  }
```

To:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(
      DownloadRequest request, ByteArray byteArray) {
    return enrichDeleteMarker(client
        .getObject(transformer.toRequest(request), AsyncResponseTransformer.toBytes())
        .thenApply(
            responseBytes -> {
              byteArray.setBytes(responseBytes.asByteArray());
              return transformer.toDownloadResponse(request, responseBytes.response());
            }));
  }
```

**`doDownload(DownloadRequest, File)` (line 173-177):**

Change:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, File file) {
    return client
        .getObject(transformer.toRequest(request), AsyncResponseTransformer.toFile(file))
        .thenApply(response -> transformer.toDownloadResponse(request, response));
  }
```

To:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, File file) {
    return enrichDeleteMarker(client
        .getObject(transformer.toRequest(request), AsyncResponseTransformer.toFile(file))
        .thenApply(response -> transformer.toDownloadResponse(request, response)));
  }
```

**`doDownload(DownloadRequest, Path)` (line 187-191):**

Change:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, Path path) {
    return client
        .getObject(transformer.toRequest(request), path)
        .thenApply(response -> transformer.toDownloadResponse(request, response));
  }
```

To:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request, Path path) {
    return enrichDeleteMarker(client
        .getObject(transformer.toRequest(request), path)
        .thenApply(response -> transformer.toDownloadResponse(request, response)));
  }
```

**`doDownload(DownloadRequest)` (line 201-209):**

Change:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request) {
    GetObjectRequest getObjectRequest = transformer.toRequest(request);
    return client
        .getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
        .thenApply(
            responseInputStream ->
                transformer.toDownloadResponse(
                    request, responseInputStream.response(), responseInputStream));
  }
```

To:

```java
  @Override
  protected CompletableFuture<DownloadResponse> doDownload(DownloadRequest request) {
    GetObjectRequest getObjectRequest = transformer.toRequest(request);
    return enrichDeleteMarker(client
        .getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
        .thenApply(
            responseInputStream ->
                transformer.toDownloadResponse(
                    request, responseInputStream.response(), responseInputStream)));
  }
```

- [ ] **Step 3: Build to verify compilation**

Run: `mvn clean install -DskipTests -pl blob/blob-aws`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/async/AwsAsyncBlobStore.java
git commit -m "feat(blob-aws): detect delete markers in async AwsAsyncBlobStore"
```

---

### Task 6: GCP delete marker enrichment in GcpBlobStore

**Files:**
- Modify: `blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java`
- Modify: `blob/blob-gcp/src/test/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStoreTest.java`

- [ ] **Step 1: Write failing tests for GCP delete marker enrichment**

Add these tests to `GcpBlobStoreTest.java`. The `ResourceNotFoundException` import is already present (line 91).

```java
@Test
void testDoGetMetadata_SoftDeleted() {
  when(mockTransformer.toBlobId(TEST_BUCKET, TEST_KEY, null)).thenReturn(mockBlobId);
  when(mockStorage.get(mockBlobId)).thenReturn(null);

  Blob softDeletedBlob = mock(Blob.class);
  when(mockStorage.get(mockBlobId, Storage.BlobGetOption.softDeleted(true)))
      .thenReturn(softDeletedBlob);

  ResourceNotFoundException thrown = assertThrows(
      ResourceNotFoundException.class,
      () -> gcpBlobStore.doGetMetadata(TEST_KEY, null));

  assertEquals("true", thrown.getMetadata("deleteMarker"));
}

@Test
void testDoGetMetadata_NeverExisted() {
  when(mockTransformer.toBlobId(TEST_BUCKET, TEST_KEY, null)).thenReturn(mockBlobId);
  when(mockStorage.get(mockBlobId)).thenReturn(null);
  when(mockStorage.get(mockBlobId, Storage.BlobGetOption.softDeleted(true)))
      .thenReturn(null);

  ResourceNotFoundException thrown = assertThrows(
      ResourceNotFoundException.class,
      () -> gcpBlobStore.doGetMetadata(TEST_KEY, null));

  assertNull(thrown.getMetadata("deleteMarker"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl blob/blob-gcp -Dtest=GcpBlobStoreTest#testDoGetMetadata_SoftDeleted+testDoGetMetadata_NeverExisted`
Expected: `testDoGetMetadata_SoftDeleted` FAILS — `getRequiredBlob` doesn't check for soft-deleted objects yet, so `getMetadata("deleteMarker")` returns null instead of "true".

- [ ] **Step 3: Update `getRequiredBlob` to check for soft-deleted objects**

In `blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java`, update the `getRequiredBlob` method (line 534-541).

Change:

```java
  private Blob getRequiredBlob(BlobId blobId) {
    Blob blob = storage.get(blobId);
    if (blob == null) {
      throw new ResourceNotFoundException(
          "Blob not found: " + blobId.getBucket() + "/" + blobId.getName());
    }
    return blob;
  }
```

To:

```java
  private Blob getRequiredBlob(BlobId blobId) {
    Blob blob = storage.get(blobId);
    if (blob == null) {
      Blob softDeleted = storage.get(blobId, Storage.BlobGetOption.softDeleted(true));
      if (softDeleted != null) {
        throw new ResourceNotFoundException(
            "Blob not found: " + blobId.getBucket() + "/" + blobId.getName())
            .withMetadata("deleteMarker", "true");
      }
      throw new ResourceNotFoundException(
          "Blob not found: " + blobId.getBucket() + "/" + blobId.getName());
    }
    return blob;
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl blob/blob-gcp -Dtest=GcpBlobStoreTest`
Expected: PASS — all tests pass including new delete marker tests. The existing `testDoGetMetadata_WithNullBlob` test should still pass because `storage.get(blobId, BlobGetOption.softDeleted(true))` is not stubbed in that test, so Mockito returns null by default, and the method throws plain `ResourceNotFoundException`.

- [ ] **Step 5: Commit**

```bash
git add blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStore.java blob/blob-gcp/src/test/java/com/salesforce/multicloudj/blob/gcp/GcpBlobStoreTest.java
git commit -m "feat(blob-gcp): detect soft-deleted objects in GcpBlobStore.getRequiredBlob"
```

---

### Task 7: Full build verification

- [ ] **Step 1: Run full build with tests**

Run: `mvn clean install`

Expected: BUILD SUCCESS for all blob modules. The `sts-ali` module may fail due to a pre-existing SSL certificate issue — this is unrelated to our changes. All blob-client, blob-aws, blob-gcp, blob-ali modules must pass.

If any test fails, fix it before proceeding.

- [ ] **Step 2: Verify no remaining references to BlobMetadata.deleteMarker**

Run: `grep -rn "isDeleteMarker\|\.deleteMarker(" --include="*.java" blob/ multicloudj-common/`

Expected: No output — all v1 references should be gone.

- [ ] **Step 3: Verify new metadata usage compiles end-to-end**

Run: `grep -rn "getMetadata\|withMetadata\|getAllMetadata" --include="*.java" multicloudj-common/ blob/`

Expected: Should show hits in SubstrateSdkException (implementation), ExceptionHandler test (guard), AwsBlobStore (throwIfDeleteMarker), AwsAsyncBlobStore (enrichDeleteMarker), GcpBlobStore (getRequiredBlob), and corresponding test files.
