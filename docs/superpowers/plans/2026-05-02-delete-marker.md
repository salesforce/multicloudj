# Delete Marker in BlobMetadata — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `boolean deleteMarker` field to `BlobMetadata` and populate it from AWS (sync+async), GCP, and Alibaba provider transformers.

**Architecture:** Add a single primitive field to the cloud-agnostic `BlobMetadata` model. Each provider's transformer maps its native concept to this field: AWS uses `response.deleteMarker()`, GCP uses `blob.getSoftDeleteTime() != null`, Alibaba reads the `x-oss-delete-marker` raw metadata header.

**Tech Stack:** Java 11, Lombok, AWS SDK v2, GCP Cloud Storage SDK, Alibaba OSS SDK, JUnit 5, Mockito

---

### Task 1: Add `deleteMarker` field to `BlobMetadata`

**Files:**
- Modify: `blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/driver/BlobMetadata.java:12-41`

- [ ] **Step 1: Add the field**

Add after the `contentType` field (line 37) and before the `objectLockInfo` field (line 39):

```java
  private final boolean deleteMarker;
```

- [ ] **Step 2: Build to verify compilation**

Run: `mvn clean install -DskipTests`
Expected: BUILD SUCCESS. Lombok generates `isDeleteMarker()` getter and builder method automatically. All existing callers that don't set `deleteMarker` get `false` by default.

- [ ] **Step 3: Commit**

```bash
git add blob/blob-client/src/main/java/com/salesforce/multicloudj/blob/driver/BlobMetadata.java
git commit -m "feat(blob): add deleteMarker field to BlobMetadata"
```

---

### Task 2: AWS — populate `deleteMarker` in `AwsTransformer`

**Files:**
- Modify: `blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsTransformer.java:308-345,392-420`
- Test: `blob/blob-aws/src/test/java/com/salesforce/multicloudj/blob/aws/AwsTransformerTest.java`

- [ ] **Step 1: Write the failing test — `toMetadata` with delete marker true**

Add to `AwsTransformerTest.java` after the existing `testToMetadata` test (after line 399):

```java
  @Test
  void testToMetadata_WithDeleteMarkerTrue() {
    var response =
        HeadObjectResponse.builder()
            .versionId("v1")
            .eTag("etag")
            .contentLength(0L)
            .lastModified(Instant.now())
            .deleteMarker(true)
            .build();
    var actual = transformer.toMetadata(response, "some-key");
    assertTrue(actual.isDeleteMarker());
  }

  @Test
  void testToMetadata_WithDeleteMarkerFalse() {
    var response =
        HeadObjectResponse.builder()
            .versionId("v1")
            .eTag("etag")
            .contentLength(1024L)
            .lastModified(Instant.now())
            .build();
    var actual = transformer.toMetadata(response, "some-key");
    assertFalse(actual.isDeleteMarker());
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl blob/blob-aws -Dtest=AwsTransformerTest#testToMetadata_WithDeleteMarkerTrue+testToMetadata_WithDeleteMarkerFalse`
Expected: FAIL — `testToMetadata_WithDeleteMarkerTrue` fails because `toMetadata` doesn't set `deleteMarker` yet, so it defaults to `false`.

- [ ] **Step 3: Implement — update `toMetadata` in `AwsTransformer.java`**

In `toMetadata` method (around line 408-419), add `.deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))` to the builder chain. The builder return block should become:

```java
    return BlobMetadata.builder()
        .key(key)
        .versionId(response.versionId())
        .eTag(eTag)
        .objectSize(objectSize)
        .metadata(metadata)
        .lastModified(response.lastModified())
        .createdTime(response.lastModified())
        .md5(eTagToMD5(eTag))
        .contentType(response.contentType())
        .deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))
        .objectLockInfo(objectLockInfo)
        .build();
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl blob/blob-aws -Dtest=AwsTransformerTest#testToMetadata_WithDeleteMarkerTrue+testToMetadata_WithDeleteMarkerFalse`
Expected: PASS

- [ ] **Step 5: Write the failing test — `toDownloadResponse` with delete marker**

Add to `AwsTransformerTest.java` after the existing `testToDownloadObjectResponse` test (after line 326):

```java
  @Test
  void testToDownloadResponse_WithDeleteMarkerTrue() {
    var request =
        DownloadRequest.builder().withKey("some/key/path.file").withVersionId("version-1").build();
    GetObjectResponse getObjectResponse = mock(GetObjectResponse.class);
    doReturn("version-1").when(getObjectResponse).versionId();
    doReturn("etag").when(getObjectResponse).eTag();
    doReturn(Instant.now()).when(getObjectResponse).lastModified();
    doReturn(Map.<String, String>of()).when(getObjectResponse).metadata();
    doReturn(0L).when(getObjectResponse).contentLength();
    doReturn(true).when(getObjectResponse).deleteMarker();

    DownloadResponse response = transformer.toDownloadResponse(request, getObjectResponse);

    assertTrue(response.getMetadata().isDeleteMarker());
  }

  @Test
  void testToDownloadResponse_WithDeleteMarkerFalse() {
    var request =
        DownloadRequest.builder().withKey("some/key/path.file").withVersionId("version-1").build();
    GetObjectResponse getObjectResponse = mock(GetObjectResponse.class);
    doReturn("version-1").when(getObjectResponse).versionId();
    doReturn("etag").when(getObjectResponse).eTag();
    doReturn(Instant.now()).when(getObjectResponse).lastModified();
    doReturn(Map.<String, String>of()).when(getObjectResponse).metadata();
    doReturn(1024L).when(getObjectResponse).contentLength();
    doReturn(false).when(getObjectResponse).deleteMarker();

    DownloadResponse response = transformer.toDownloadResponse(request, getObjectResponse);

    assertFalse(response.getMetadata().isDeleteMarker());
  }
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `mvn test -pl blob/blob-aws -Dtest=AwsTransformerTest#testToDownloadResponse_WithDeleteMarkerTrue+testToDownloadResponse_WithDeleteMarkerFalse`
Expected: FAIL — `testToDownloadResponse_WithDeleteMarkerTrue` fails.

- [ ] **Step 7: Implement — update both `toDownloadResponse` methods in `AwsTransformer.java`**

In the first `toDownloadResponse` (around lines 308-324), add `.deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))` to the `BlobMetadata.builder()` chain:

```java
  public DownloadResponse toDownloadResponse(
      DownloadRequest downloadRequest, GetObjectResponse response) {
    return DownloadResponse.builder()
        .key(downloadRequest.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(downloadRequest.getKey())
                .versionId(response.versionId())
                .eTag(response.eTag())
                .lastModified(response.lastModified())
                .createdTime(response.lastModified())
                .metadata(response.metadata())
                .objectSize(response.contentLength())
                .contentType(response.contentType())
                .deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))
                .build())
        .build();
  }
```

In the second `toDownloadResponse` (around lines 326-345), same addition:

```java
  public DownloadResponse toDownloadResponse(
      DownloadRequest downloadRequest,
      GetObjectResponse response,
      ResponseInputStream<GetObjectResponse> responseInputStream) {
    return DownloadResponse.builder()
        .key(downloadRequest.getKey())
        .metadata(
            BlobMetadata.builder()
                .key(downloadRequest.getKey())
                .versionId(response.versionId())
                .eTag(response.eTag())
                .lastModified(response.lastModified())
                .createdTime(response.lastModified())
                .metadata(response.metadata())
                .objectSize(response.contentLength())
                .contentType(response.contentType())
                .deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))
                .build())
        .inputStream(responseInputStream)
        .build();
  }
```

- [ ] **Step 8: Run all AWS transformer tests**

Run: `mvn test -pl blob/blob-aws -Dtest=AwsTransformerTest`
Expected: ALL PASS

- [ ] **Step 9: Commit**

```bash
git add blob/blob-aws/src/main/java/com/salesforce/multicloudj/blob/aws/AwsTransformer.java blob/blob-aws/src/test/java/com/salesforce/multicloudj/blob/aws/AwsTransformerTest.java
git commit -m "feat(blob-aws): populate deleteMarker from S3 response in AwsTransformer"
```

---

### Task 3: GCP — populate `deleteMarker` from soft delete time in `GcpTransformer`

**Files:**
- Modify: `blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpTransformer.java:160-216`
- Test: `blob/blob-gcp/src/test/java/com/salesforce/multicloudj/blob/gcp/GcpTransformerTest.java`

- [ ] **Step 1: Write the failing test — soft-deleted blob**

Add to `GcpTransformerTest.java` after the existing `testToBlobMetadata_WithNullFields` test (after line 625):

```java
  @Test
  void testToBlobMetadata_WithSoftDeleteTime() {
    OffsetDateTime softDeleteTime = OffsetDateTime.of(2023, 6, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    when(mockBlob.getName()).thenReturn(TEST_KEY);
    when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
    when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
    when(mockBlob.getSize()).thenReturn(TEST_SIZE);
    when(mockBlob.getMetadata()).thenReturn(null);
    when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);
    when(mockBlob.getMd5()).thenReturn(null);
    when(mockBlob.getSoftDeleteTime()).thenReturn(softDeleteTime);

    BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

    assertTrue(blobMetadata.isDeleteMarker());
  }

  @Test
  void testToBlobMetadata_WithoutSoftDeleteTime() {
    when(mockBlob.getName()).thenReturn(TEST_KEY);
    when(mockBlob.getGeneration()).thenReturn(TEST_GENERATION);
    when(mockBlob.getEtag()).thenReturn(TEST_ETAG);
    when(mockBlob.getSize()).thenReturn(TEST_SIZE);
    when(mockBlob.getMetadata()).thenReturn(null);
    when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);
    when(mockBlob.getMd5()).thenReturn(null);
    when(mockBlob.getSoftDeleteTime()).thenReturn(null);

    BlobMetadata blobMetadata = transformer.toBlobMetadata(mockBlob);

    assertFalse(blobMetadata.isDeleteMarker());
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl blob/blob-gcp -Dtest=GcpTransformerTest#testToBlobMetadata_WithSoftDeleteTime+testToBlobMetadata_WithoutSoftDeleteTime`
Expected: FAIL — `testToBlobMetadata_WithSoftDeleteTime` fails because `deleteMarker` defaults to `false`.

- [ ] **Step 3: Implement — update `toBlobMetadata` in `GcpTransformer.java`**

In the `toBlobMetadata` method (around line 198-215), add `.deleteMarker(blob.getSoftDeleteTime() != null)` to the builder chain. The return block should become:

```java
    return BlobMetadata.builder()
        .key(blob.getName())
        .versionId(blob.getGeneration() != null ? blob.getGeneration().toString() : null)
        .eTag(blob.getEtag())
        .objectSize(blob.getSize())
        .metadata(blob.getMetadata() == null ? Collections.emptyMap() : blob.getMetadata())
        .lastModified(
            blob.getUpdateTimeOffsetDateTime() != null
                ? blob.getUpdateTimeOffsetDateTime().toInstant()
                : null)
        .createdTime(
            blob.getCreateTimeOffsetDateTime() != null
                ? blob.getCreateTimeOffsetDateTime().toInstant()
                : null)
        .md5(HexUtil.convertToBytes(blob.getMd5()))
        .contentType(blob.getContentType())
        .deleteMarker(blob.getSoftDeleteTime() != null)
        .objectLockInfo(objectLockInfo)
        .build();
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl blob/blob-gcp -Dtest=GcpTransformerTest#testToBlobMetadata_WithSoftDeleteTime+testToBlobMetadata_WithoutSoftDeleteTime`
Expected: PASS

- [ ] **Step 5: Run all GCP transformer tests**

Run: `mvn test -pl blob/blob-gcp -Dtest=GcpTransformerTest`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add blob/blob-gcp/src/main/java/com/salesforce/multicloudj/blob/gcp/GcpTransformer.java blob/blob-gcp/src/test/java/com/salesforce/multicloudj/blob/gcp/GcpTransformerTest.java
git commit -m "feat(blob-gcp): populate deleteMarker from soft delete time in GcpTransformer"
```

---

### Task 4: Alibaba — populate `deleteMarker` from raw metadata in `AliTransformer`

**Files:**
- Modify: `blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliTransformer.java:143-176,223-237`
- Test: `blob/blob-ali/src/test/java/com/salesforce/multicloudj/blob/ali/AliTransformerTest.java`

- [ ] **Step 1: Write the failing test — `toBlobMetadata` with delete marker header**

Add to `AliTransformerTest.java` after the existing `testToBlobMetadata` test (after line 319):

```java
  @Test
  void testToBlobMetadata_WithDeleteMarkerTrue() {
    ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
    doReturn("version-1").when(objectMetadata).getVersionId();
    doReturn("etag").when(objectMetadata).getETag();
    Date date = Date.from(Instant.now());
    doReturn(date).when(objectMetadata).getLastModified();
    doReturn(Map.<String, String>of()).when(objectMetadata).getUserMetadata();
    doReturn(0L).when(objectMetadata).getContentLength();
    doReturn(null).when(objectMetadata).getContentMD5();
    Map<String, Object> rawMetadata = new HashMap<>();
    rawMetadata.put("x-oss-delete-marker", "true");
    doReturn(rawMetadata).when(objectMetadata).getRawMetadata();

    var actual = transformer.toBlobMetadata("key", objectMetadata);

    assertTrue(actual.isDeleteMarker());
  }

  @Test
  void testToBlobMetadata_WithDeleteMarkerFalse() {
    ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
    doReturn("version-1").when(objectMetadata).getVersionId();
    doReturn("etag").when(objectMetadata).getETag();
    Date date = Date.from(Instant.now());
    doReturn(date).when(objectMetadata).getLastModified();
    doReturn(Map.<String, String>of()).when(objectMetadata).getUserMetadata();
    doReturn(100L).when(objectMetadata).getContentLength();
    doReturn(null).when(objectMetadata).getContentMD5();
    Map<String, Object> rawMetadata = new HashMap<>();
    doReturn(rawMetadata).when(objectMetadata).getRawMetadata();

    var actual = transformer.toBlobMetadata("key", objectMetadata);

    assertFalse(actual.isDeleteMarker());
  }
```

Note: Add `import java.util.HashMap;` to the imports in `AliTransformerTest.java` — it is not currently present.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl blob/blob-ali -Dtest=AliTransformerTest#testToBlobMetadata_WithDeleteMarkerTrue+testToBlobMetadata_WithDeleteMarkerFalse`
Expected: FAIL — `testToBlobMetadata_WithDeleteMarkerTrue` fails.

- [ ] **Step 3: Implement — update `toBlobMetadata` in `AliTransformer.java`**

In `toBlobMetadata` (around lines 223-237), add `.deleteMarker("true".equals(String.valueOf(metadata.getRawMetadata().get("x-oss-delete-marker"))))`:

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
        .deleteMarker("true".equals(String.valueOf(metadata.getRawMetadata().get("x-oss-delete-marker"))))
        .build();
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -pl blob/blob-ali -Dtest=AliTransformerTest#testToBlobMetadata_WithDeleteMarkerTrue+testToBlobMetadata_WithDeleteMarkerFalse`
Expected: PASS

- [ ] **Step 5: Write the failing test — `toDownloadResponse` with delete marker**

Add to `AliTransformerTest.java` after the existing `testToDownloadResponseWithInputStream` test (after line 223):

```java
  @Test
  void testToDownloadResponse_WithDeleteMarkerTrue() {
    OSSObject ossObject = mock(OSSObject.class);
    doReturn("key").when(ossObject).getKey();
    ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
    doReturn(objectMetadata).when(ossObject).getObjectMetadata();
    doReturn("version-1").when(objectMetadata).getVersionId();
    doReturn("etag").when(objectMetadata).getETag();
    Date date = Date.from(Instant.now());
    doReturn(date).when(objectMetadata).getLastModified();
    doReturn(Map.<String, String>of()).when(objectMetadata).getUserMetadata();
    doReturn(0L).when(objectMetadata).getContentLength();
    Map<String, Object> rawMetadata = new HashMap<>();
    rawMetadata.put("x-oss-delete-marker", "true");
    doReturn(rawMetadata).when(objectMetadata).getRawMetadata();

    var actual = transformer.toDownloadResponse(ossObject);

    assertTrue(actual.getMetadata().isDeleteMarker());
  }

  @Test
  void testToDownloadResponse_WithDeleteMarkerFalse() {
    OSSObject ossObject = mock(OSSObject.class);
    doReturn("key").when(ossObject).getKey();
    ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
    doReturn(objectMetadata).when(ossObject).getObjectMetadata();
    doReturn("version-1").when(objectMetadata).getVersionId();
    doReturn("etag").when(objectMetadata).getETag();
    Date date = Date.from(Instant.now());
    doReturn(date).when(objectMetadata).getLastModified();
    doReturn(Map.<String, String>of()).when(objectMetadata).getUserMetadata();
    doReturn(100L).when(objectMetadata).getContentLength();
    Map<String, Object> rawMetadata = new HashMap<>();
    doReturn(rawMetadata).when(objectMetadata).getRawMetadata();

    var actual = transformer.toDownloadResponse(ossObject);

    assertFalse(actual.getMetadata().isDeleteMarker());
  }
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `mvn test -pl blob/blob-ali -Dtest=AliTransformerTest#testToDownloadResponse_WithDeleteMarkerTrue+testToDownloadResponse_WithDeleteMarkerFalse`
Expected: FAIL — `testToDownloadResponse_WithDeleteMarkerTrue` fails.

- [ ] **Step 7: Implement — update both `toDownloadResponse` methods in `AliTransformer.java`**

In the first `toDownloadResponse` (around lines 143-158), add `.deleteMarker(...)`:

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
                .deleteMarker("true".equals(String.valueOf(ossObject.getObjectMetadata().getRawMetadata().get("x-oss-delete-marker"))))
                .build())
        .build();
  }
```

In the second `toDownloadResponse` (around lines 160-176), same addition:

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
                .deleteMarker("true".equals(String.valueOf(ossObject.getObjectMetadata().getRawMetadata().get("x-oss-delete-marker"))))
                .build())
        .inputStream(inputStream)
        .build();
  }
```

- [ ] **Step 8: Run all Alibaba transformer tests**

Run: `mvn test -pl blob/blob-ali -Dtest=AliTransformerTest`
Expected: ALL PASS

- [ ] **Step 9: Commit**

```bash
git add blob/blob-ali/src/main/java/com/salesforce/multicloudj/blob/ali/AliTransformer.java blob/blob-ali/src/test/java/com/salesforce/multicloudj/blob/ali/AliTransformerTest.java
git commit -m "feat(blob-ali): populate deleteMarker from OSS raw metadata in AliTransformer"
```

---

### Task 5: Full build verification

**Files:** None — verification only.

- [ ] **Step 1: Run full build with all unit tests**

Run: `mvn clean install`
Expected: BUILD SUCCESS with all tests passing.

- [ ] **Step 2: Run checkstyle verification**

This is included in the `mvn clean install` lifecycle. If any checkstyle violations are reported, fix line length or formatting issues (the `deleteMarker` lines in Alibaba `toDownloadResponse` may be long — break across lines if needed).

- [ ] **Step 3: Commit any checkstyle fixes if needed**

```bash
git add -u
git commit -m "style: fix checkstyle issues in delete marker implementation"
```
