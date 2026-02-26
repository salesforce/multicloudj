package com.salesforce.multicloudj.dbbackuprestore.driver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Request object for restoring a backup to a database table.
 *
 * @since 0.2.25
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RestoreRequest {

    /**
     * ID of the backup to restore from.
     */
    private String backupId;

    /**
     * Name of the target table to restore to.
     * If null or empty, restores to the original table name.
     */
    private String targetResource;

    /**
     * role id for restore operations requiring role-based authorization.
     * Some providers require an IAM role that the backup service assumes to perform
     * the restore operation and create the restored resource.
     */
    private String roleId;

    /**
     * Vault identifier for backup vault-based restore operations.
     * Some providers use vault-based backup systems where the vault ID
     * is required to locate and restore from a backup.
     */
    private String vaultId;

    /**
     * KMS encryption key identifier for encrypting the restored resource.
     */
    private String kmsEncryptionKeyId;
}
