# Delete Marker Field in BlobMetadata

## Summary

Add a `boolean deleteMarker` field to `BlobMetadata` so callers can determine whether an object version is a delete marker. Implement across AWS (sync + async), GCP, and Alibaba providers.

## Motivation

In versioned S3 buckets, deleting an object creates a "delete marker" — a zero-byte placeholder version. A `HeadObject` call targeting that version returns `x-amz-delete-marker: true`. The current `BlobMetadata` model doesn't expose this information, so callers can't distinguish delete markers from real object versions.

## Design

### Cloud-Agnostic API

Add to `BlobMetadata`:

```java
private final boolean deleteMarker;
```

Lombok `@Builder` gives it a default of `false`. No changes needed to `BlobMetadata.Builder` beyond the field declaration.

### AWS Implementation (Sync + Async)

In `AwsTransformer`, add `.deleteMarker(Boolean.TRUE.equals(response.deleteMarker()))` to all methods that build `BlobMetadata`:

- `toMetadata(HeadObjectResponse, String)` — used by both sync and async getMetadata
- `toDownloadResponse(DownloadRequest, GetObjectResponse)` — sync download without stream
- `toDownloadResponse(DownloadRequest, GetObjectResponse, ResponseInputStream)` — sync download with stream

`GetObjectResponse` and `HeadObjectResponse` both expose `deleteMarker()` returning `Boolean`.

### GCP Implementation

GCP Cloud Storage does not have S3-style delete markers. Instead, it has a "soft delete" feature: when enabled on a bucket, deleted objects are retained for a configurable period. The `Blob` class exposes `getSoftDeleteTime()` (when the object was soft-deleted) and `getHardDeleteTime()` (when it will be permanently purged).

In `GcpTransformer.toBlobMetadata(Blob)`:

```java
.deleteMarker(blob.getSoftDeleteTime() != null)
```

A non-null `softDeleteTime` indicates the object is in soft-deleted state — the closest GCP equivalent to an AWS delete marker.

### Alibaba Implementation

Alibaba OSS versioning supports delete markers. The `ObjectMetadata` raw metadata can carry the `x-oss-delete-marker` header. In `AliTransformer`:

- `toBlobMetadata(String, ObjectMetadata)`: Read `"true".equals(String.valueOf(metadata.getRawMetadata().get("x-oss-delete-marker")))`.
- `toDownloadResponse(OSSObject)` and `toDownloadResponse(OSSObject, InputStream)`: Same extraction from `ossObject.getObjectMetadata().getRawMetadata()`.

Defaults to `false` when the header is absent.

### Files Changed

| File | Change |
|------|--------|
| `blob/blob-client/.../BlobMetadata.java` | Add `boolean deleteMarker` field |
| `blob/blob-aws/.../AwsTransformer.java` | Extract `response.deleteMarker()` in `toMetadata()` and both `toDownloadResponse()` methods |
| `blob/blob-gcp/.../GcpTransformer.java` | Map `blob.getSoftDeleteTime() != null` to `deleteMarker` in `toBlobMetadata()` |
| `blob/blob-ali/.../AliTransformer.java` | Extract from raw metadata in `toBlobMetadata()` and both `toDownloadResponse()` methods |

### Testing

- **AWS unit tests**: Mock `HeadObjectResponse` with `deleteMarker(true)` and verify `BlobMetadata.isDeleteMarker()` returns true. Also verify the false case.
- **GCP unit tests**: Mock `Blob` with non-null `getSoftDeleteTime()` and verify `isDeleteMarker()` returns true. Also verify the null (false) case.
- **Alibaba unit tests**: Mock `ObjectMetadata` with delete marker header and verify mapping. Also verify the false/null case.
- **Conformance tests**: Add an assertion in the abstract conformance test that `isDeleteMarker()` is accessible on `BlobMetadata` (basic field presence check). Provider-specific delete marker behavior is provider-dependent and tested in unit tests.

### Backward Compatibility

Fully backward-compatible. `boolean` primitive defaults to `false` in Lombok builders, so all existing callers that don't set `deleteMarker` get `false`.
