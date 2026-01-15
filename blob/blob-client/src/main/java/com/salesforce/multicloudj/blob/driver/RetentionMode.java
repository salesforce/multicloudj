package com.salesforce.multicloudj.blob.driver;

/**
 * Retention mode for WORM (Write Once Read Many) protection.
 * 
 * <p>For AWS S3:
 * <ul>
 *   <li>GOVERNANCE: Users with s3:BypassGovernanceMode permission can override or remove the retention</li>
 *   <li>COMPLIANCE: No user can override or remove the retention until it expires</li>
 * </ul>
 * 
 * <p>For GCP GCS:
 * <ul>
 *   <li>UNLOCKED (GOVERNANCE): Authorized users can modify or remove retention with bypass header (x-goog-bypass-governance-retention: true)</li>
 *   <li>LOCKED (COMPLIANCE): Retention cannot be reduced or removed, only increased. Mode cannot be changed once set.</li>
 * </ul>
 */
public enum RetentionMode {
    /**
     * Governance mode: Retention can be bypassed by users with special permissions.
     * For AWS S3: Maps to GOVERNANCE mode.
     * For GCP GCS: Maps to UNLOCKED mode (requires bypass header to modify/remove).
     */
    GOVERNANCE,

    /**
     * Compliance mode: Retention cannot be bypassed by anyone until it expires.
     * For AWS S3: Maps to COMPLIANCE mode.
     * For GCP GCS: Maps to LOCKED mode (cannot be reduced or removed, only increased).
     */
    COMPLIANCE
}
