package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;

/**
 * Object lock information for a blob (retrieved from metadata).
 * 
 * <p>This represents the current object lock state of an object, including:
 * <ul>
 *   <li>Retention mode (AWS S3 only)</li>
 *   <li>Retention expiration date</li>
 *   <li>Legal hold status</li>
 *   <li>Hold type for GCP (temporary vs event-based)</li>
 * </ul>
 */
@Builder
@Getter
public class ObjectLockInfo {
    
    /**
     * Retention mode: GOVERNANCE or COMPLIANCE.
     * 
     * <p>For AWS S3: The actual mode set on the object.
     * <p>For GCP GCS: Always null (GCP doesn't support retention modes).
     */
    private final ObjectLockMode mode;
    
    /**
     * Date until which the object is retained.
     * 
     * <p>For AWS S3: The objectLockRetainUntilDate.
     * <p>For GCP GCS: Calculated from bucket retention policy and object creation time.
     */
    private final Instant retainUntilDate;
    
    /**
     * Whether legal hold is currently applied.
     * 
     * <p>For AWS S3: objectLockLegalHoldStatus == ON.
     * <p>For GCP GCS: temporaryHold or eventBasedHold is true.
     */
    private final boolean legalHold;
    
    /**
     * For GCP GCS only: Whether eventBasedHold is used (true) or temporaryHold (false).
     * 
     * <p>For AWS S3: Always null.
     */
    private final Boolean useEventBasedHold;
}
