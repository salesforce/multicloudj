package com.salesforce.multicloudj.blob.driver;

/**
 * Retention mode for WORM (Write Once Read Many) protection.
 */
public enum RetentionMode {
    /**
     * Governance mode: Retention can be bypassed by users with special permissions.
     */
    GOVERNANCE,

    /**
     * Compliance mode: Retention cannot be bypassed by anyone until it expires.
     */
    COMPLIANCE
}
