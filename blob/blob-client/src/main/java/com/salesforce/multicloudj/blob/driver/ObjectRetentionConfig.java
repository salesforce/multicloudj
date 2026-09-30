package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for updating per-object retention via {@link
 * BlobStore#updateObjectRetention(String, String, ObjectRetentionConfig)}.
 *
 * <p>Distinct from {@link ObjectLockConfiguration}, which is used at upload time and additionally
 * carries legal-hold flags. This type is scoped strictly to <em>retention update</em>: changing the
 * retention mode and/or the retention expiration date on an object that already has retention
 * configured. Legal hold remains on {@link BlobStore#updateLegalHold(String, String, boolean)}.
 *
 * <h3>Field semantics</h3>
 *
 * <ul>
 *   <li>{@code mode} — target retention mode. May be {@code null} to preserve the object's current
 *       retention mode (the provider fetches the current mode and reuses it).
 *   <li>{@code retainUntilDate} — new retention expiration date. Must be non-null. Sub-millisecond
 *       precision is not preserved on GCP (GCS truncates to RFC 3339 millisecond precision); for
 *       retention dates this is generally not a concern.
 *   <li>{@code bypassGovernanceRetention} — when {@code true}, allows shortening or removing
 *       retention on objects currently in {@link RetentionMode#GOVERNANCE} (AWS) or {@code
 *       UNLOCKED} (GCP) mode. Maps to {@code bypassGovernanceRetention} on AWS and to {@code
 *       overrideUnlockedRetention} on GCP. The flag is <strong>ignored on the immutable
 *       mode</strong> ({@link RetentionMode#COMPLIANCE} / {@code LOCKED}); both clouds reject any
 *       shorten or downgrade attempt server-side regardless of this flag.
 * </ul>
 *
 * <p>The complete (mode, bypass, current state) → outcome rules table is documented on {@link
 * BlobStore#updateObjectRetention(String, String, ObjectRetentionConfig)}.
 */
@Builder
@Getter
public class ObjectRetentionConfig {

  /**
   * Target retention mode. May be {@code null} to preserve the current mode of the object.
   *
   * <p>Mode downgrades from {@link RetentionMode#COMPLIANCE} to {@link RetentionMode#GOVERNANCE}
   * are rejected by every provider — the underlying clouds (AWS S3, GCP GCS) do not permit such
   * transitions, and MultiCloudJ surfaces a uniform {@code FailedPreconditionException} for the
   * attempt.
   */
  private final RetentionMode mode;

  /**
   * New retention expiration date. Must be non-null.
   *
   * <p>Sub-millisecond precision is not preserved on GCP — GCS truncates this value to RFC 3339
   * millisecond precision on the wire.
   */
  private final Instant retainUntilDate;

  /**
   * When {@code true}, allows shortening or removing retention on objects currently in {@link
   * RetentionMode#GOVERNANCE} (AWS) / {@code UNLOCKED} (GCP) mode.
   *
   * <p>Maps to {@code bypassGovernanceRetention} on AWS {@code PutObjectRetention} requests and to
   * {@code Storage.BlobTargetOption.overrideUnlockedRetention(true)} on GCP {@code storage.update}
   * calls.
   *
   * <p><strong>No effect on the immutable mode.</strong> Both AWS and GCP reject any shorten,
   * downgrade, or remove attempt against {@link RetentionMode#COMPLIANCE} / {@code LOCKED}
   * regardless of this flag, IAM permissions, or principal (including AWS root). MultiCloudJ
   * mirrors this server-side semantic with a client-side guard for uniform error reporting.
   *
   * <p>{@code null} or {@code false} ⇒ no bypass (default).
   */
  private final Boolean bypassGovernanceRetention;
}
