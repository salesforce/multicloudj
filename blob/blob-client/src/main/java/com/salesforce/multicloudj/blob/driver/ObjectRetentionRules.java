package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import java.time.Instant;

/**
 * State-dependent rules shared by every provider's {@code doUpdateObjectRetention} hook so AWS,
 * GCP, and the in-memory provider surface uniform exception types and messages.
 *
 * <p>The full rules table lives on the javadoc for {@link BlobStore#updateObjectRetention(String,
 * String, ObjectRetentionConfig)}. This class is the single source of truth for the runtime
 * checks.
 *
 * <p><strong>Stateless</strong> validation (null config, null retain-until-date) lives in {@link
 * BlobStoreValidator#validate(ObjectRetentionConfig)} and is run by the {@code
 * AbstractBlobStore} template before the provider hook is invoked. By the time control reaches
 * this helper, both {@code config} and {@code config.getRetainUntilDate()} are guaranteed
 * non-null.
 */
public final class ObjectRetentionRules {

  static final String NO_CURRENT_RETENTION_MSG =
      "Object does not have retention configured. Cannot update retention.";
  static final String MODE_DOWNGRADE_MSG =
      "Cannot downgrade retention mode from COMPLIANCE to GOVERNANCE. "
          + "The immutable mode does not permit downgrade on AWS or GCP.";
  static final String COMPLIANCE_SHORTEN_MSG =
      "Cannot shorten retention for objects in COMPLIANCE mode. "
          + "bypassGovernanceRetention has no effect on the immutable mode.";
  static final String GOVERNANCE_SHORTEN_NO_BYPASS_MSG =
      "Cannot shorten retention for objects in GOVERNANCE mode without "
          + "bypassGovernanceRetention=true.";
  static final String MODE_UPGRADE_NO_BYPASS_MSG =
      "Cannot upgrade retention mode from GOVERNANCE to COMPLIANCE without "
          + "bypassGovernanceRetention=true. Both AWS S3 and GCP GCS treat the lock-mode "
          + "transition as a modification of the existing lock and require the bypass/override "
          + "flag.";

  private ObjectRetentionRules() {
    // utility
  }

  /**
   * Resolves the effective new retention against the locked rules table. Throws {@link
   * FailedPreconditionException} when a rule rejects the request.
   *
   * @param currentMode current retention mode on the object, or {@code null} if there is none
   * @param currentRetainUntil current retain-until date, or {@code null} if there is none
   * @param config the requested change (must have non-null retainUntilDate)
   * @return resolved retention mode to write back ({@code config.mode} when non-null, else {@code
   *     currentMode})
   */
  public static RetentionMode resolveAndValidate(
      RetentionMode currentMode, Instant currentRetainUntil, ObjectRetentionConfig config) {

    // 1. No current retention → reject. Update is for objects that already have retention set;
    //    "first time set" belongs on the upload path.
    if (currentMode == null) {
      throw new FailedPreconditionException(NO_CURRENT_RETENTION_MSG);
    }

    RetentionMode effectiveMode = config.getMode() != null ? config.getMode() : currentMode;
    boolean bypass = Boolean.TRUE.equals(config.getBypassGovernanceRetention());

    // 2. Mode downgrade COMPLIANCE → GOVERNANCE is rejected by both AWS and GCP server-side.
    if (currentMode == RetentionMode.COMPLIANCE && effectiveMode == RetentionMode.GOVERNANCE) {
      throw new FailedPreconditionException(MODE_DOWNGRADE_MSG);
    }

    // 2b. Mode upgrade GOVERNANCE → COMPLIANCE requires bypassGovernanceRetention=true. Both
    //     AWS S3 and GCP GCS treat the mode change as a modification of the existing lock and
    //     reject it server-side without the bypass/override flag (AWS HTTP 403 AccessDenied;
    //     GCP HTTP 400/412). We mirror that client-side for uniform error reporting.
    if (currentMode == RetentionMode.GOVERNANCE
        && effectiveMode == RetentionMode.COMPLIANCE
        && !bypass) {
      throw new FailedPreconditionException(MODE_UPGRADE_NO_BYPASS_MSG);
    }

    // 3. Shorten check — strict less-than, equal dates are not "shortening".
    boolean shortening =
        currentRetainUntil != null && config.getRetainUntilDate().isBefore(currentRetainUntil);
    if (shortening) {
      // 3a. COMPLIANCE / LOCKED is extend-only regardless of bypass — both clouds ignore the flag
      //     on the immutable mode.
      if (currentMode == RetentionMode.COMPLIANCE) {
        throw new FailedPreconditionException(COMPLIANCE_SHORTEN_MSG);
      }
      // 3b. GOVERNANCE / UNLOCKED requires explicit bypass to shorten.
      if (!bypass) {
        throw new FailedPreconditionException(GOVERNANCE_SHORTEN_NO_BYPASS_MSG);
      }
    }

    return effectiveMode;
  }
}
