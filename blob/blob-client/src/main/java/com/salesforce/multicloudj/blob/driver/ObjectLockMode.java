package com.salesforce.multicloudj.blob.driver;

/**
 * Object lock retention mode for WORM (Write Once Read Many) protection.
 * 
 * <p>For AWS S3:
 * <ul>
 *   <li>GOVERNANCE: Users with s3:BypassGovernanceMode permission can override or remove the retention</li>
 *   <li>COMPLIANCE: No user can override or remove the retention until it expires</li>
 * </ul>
 * 
 * <p>For GCP GCS:
 * <ul>
 *   <li>GCP does not support retention modes. Object lock is achieved through bucket retention policies
 *       combined with object holds (temporaryHold or eventBasedHold).</li>
 *   <li>When using this SDK, the mode is ignored for GCP and bucket retention policy provides the WORM protection.</li>
 * </ul>
 */
public enum ObjectLockMode {
    /**
     * Governance mode: Retention can be bypassed by users with special permissions.
     * For AWS S3: Maps to GOVERNANCE mode.
     * For GCP: Not applicable, ignored.
     */
    GOVERNANCE,
    
    /**
     * Compliance mode: Retention cannot be bypassed by anyone until it expires.
     * For AWS S3: Maps to COMPLIANCE mode.
     * For GCP: Not applicable, ignored.
     */
    COMPLIANCE
}
