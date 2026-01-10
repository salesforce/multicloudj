package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;

/**
 * Configuration for object lock (WORM protection) when uploading objects.
 * 
 * <p>This provides a unified API for object lock across cloud providers:
 * <ul>
 *   <li><b>AWS S3:</b> Full support - object-level retention with mode (GOVERNANCE/COMPLIANCE) and legal hold</li>
 *   <li><b>GCP GCS:</b> Partial support - requires bucket retention policy; uses object holds (temporaryHold/eventBasedHold)</li>
 *   <li><b>OSS:</b> Not supported</li>
 * </ul>
 * 
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * ObjectLockConfiguration lockConfig = ObjectLockConfiguration.builder()
 *     .mode(ObjectLockMode.COMPLIANCE)
 *     .retainUntilDate(Instant.now().plus(30, ChronoUnit.DAYS))
 *     .legalHold(true)
 *     .build();
 * 
 * UploadRequest request = UploadRequest.builder()
 *     .withKey("backup-snapshot")
 *     .withObjectLock(lockConfig)
 *     .build();
 * }</pre>
 */
@Builder
@Getter
public class ObjectLockConfiguration {
    
    /**
     * Retention mode: GOVERNANCE (can be bypassed) or COMPLIANCE (cannot be bypassed).
     * 
     * <p>For AWS S3: Directly maps to objectLockMode.
     * <p>For GCP GCS: Not applicable, ignored. Bucket retention policy provides the protection.
     */
    private final ObjectLockMode mode;
    
    /**
     * Date until which the object should be retained (WORM protection).
     * 
     * <p>For AWS S3: Directly maps to objectLockRetainUntilDate.
     * <p>For GCP GCS: Requires bucket retention policy to be set. This value is validated against
     *    the bucket's retention period to ensure compliance.
     */
    private final Instant retainUntilDate;
    
    /**
     * Legal hold status: prevents deletion/modification until removed.
     * 
     * <p>For AWS S3: Maps to objectLockLegalHoldStatus (ON/OFF).
     * <p>For GCP GCS: Maps to temporaryHold or eventBasedHold (based on useEventBasedHold flag).
     */
    private final boolean legalHold;
    
    /**
     * For GCP GCS only: Whether to use eventBasedHold (true) or temporaryHold (false).
     * 
     * <p>Event-based hold: Resets the object's time-in-bucket for retention policy calculation when released.
     * <p>Temporary hold: Does not affect retention policy calculation.
     * <p>Default: false (temporaryHold)
     * <p>For AWS S3: Ignored.
     */
    private final Boolean useEventBasedHold;
}
