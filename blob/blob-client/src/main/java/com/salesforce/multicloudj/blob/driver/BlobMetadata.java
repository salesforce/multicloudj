package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/** Blob metadata data object */
@Builder(toBuilder = true)
@Getter
public class BlobMetadata {

  private final String key;

  /**
   * The versionId of this blob. This value only serves a purpose for buckets with versioning
   * enabled, although non-versioned buckets may still return a value for it. Non-versioned buckets
   * should simply ignore the versionId value as it serves no purpose for them.
   */
  private final String versionId;

  private final String eTag;
  private final long objectSize;

  @Singular("metadata")
  private final Map<String, String> metadata;

  private final Instant lastModified;

  /** The creation time of the blob. This represents when the blob was originally created. */
  private final Instant createdTime;

  private final byte[] md5;

  /** The content type of the blob (e.g., "application/octet-stream", "application/x-directory") */
  private final String contentType;

  /** Object lock information for this blob. null if object lock is not configured. */
  private final ObjectLockInfo objectLockInfo;

  /**
   * Checksum of the object as reported by the store, or {@code null} if the store does not report
   * one. Populated on responses from {@code getMetadata} and {@code download}.
   */
  private final Checksum checksum;

  /** The correlation ID associated with the operation that produced this metadata. */
  private final String correlationId;

  /**
   * Whether this entry is a delete marker rather than a content version. Delete markers record that
   * an object was deleted at a point in time; they carry no downloadable content. Defaults to
   * {@code false} for content versions and for stores that do not surface delete markers.
   */
  private final boolean deleteMarker;

  /**
   * The instant at which this version stopped being the current version, or {@code null} if it is
   * still current (or the store does not report it). Together with {@link #createdTime} this
   * defines the version's validity interval {@code [createdTime, noncurrentAt)}, with the end
   * instant exclusive.
   *
   * <p>How this instant is obtained depends on the backing store: some stores report the
   * supersession moment natively, while others derive it from the creation time of the immediately
   * superseding version. The interval contract is identical in both cases, but the value may differ
   * by a small margin from a store-native timestamp for the same logical event, so treat it as the
   * boundary of the validity interval rather than an exact, cross-store-comparable clock reading.
   */
  private final Instant noncurrentAt;
}
