package com.salesforce.multicloudj.dbbackuprestore.driver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the RestoreRequest model class.
 */
public class RestoreRequestTest {

    @Test
    void testRestoreRequestBuilder() {
        RestoreRequest request = RestoreRequest.builder()
                .backupId("backup-123")
                .targetResource("restored-table")
                .roleId("arn:aws:iam::123456789012:role/RestoreRole")
                .vaultId("vault-abc")
                .kmsEncryptionKeyId("arn:aws:kms:us-west-2:123456789012:key/abc-def")
                .build();

        assertEquals("backup-123", request.getBackupId());
        assertEquals("restored-table", request.getTargetResource());
        assertEquals("arn:aws:iam::123456789012:role/RestoreRole", request.getRoleId());
        assertEquals("vault-abc", request.getVaultId());
        assertEquals("arn:aws:kms:us-west-2:123456789012:key/abc-def", request.getKmsEncryptionKeyId());
    }

    @Test
    void testRestoreRequestSetters() {
        RestoreRequest request = new RestoreRequest();

        request.setBackupId("backup-789");
        request.setTargetResource("new-table");
        request.setRoleId("role-123");
        request.setVaultId("vault-xyz");
        request.setKmsEncryptionKeyId("arn:aws:kms:us-west-2:123456789012:key/xyz");

        assertEquals("backup-789", request.getBackupId());
        assertEquals("new-table", request.getTargetResource());
        assertEquals("role-123", request.getRoleId());
        assertEquals("vault-xyz", request.getVaultId());
        assertEquals("arn:aws:kms:us-west-2:123456789012:key/xyz", request.getKmsEncryptionKeyId());
    }

    @Test
    void testRestoreRequestAllArgsConstructor() {
        RestoreRequest request = new RestoreRequest(
                "backup-all",
                "target-table",
                "role-arn",
                "vault-id",
                "arn:aws:kms:us-west-2:123456789012:key/all-args"
        );

        assertEquals("backup-all", request.getBackupId());
        assertEquals("target-table", request.getTargetResource());
        assertEquals("role-arn", request.getRoleId());
        assertEquals("vault-id", request.getVaultId());
        assertEquals("arn:aws:kms:us-west-2:123456789012:key/all-args", request.getKmsEncryptionKeyId());
    }
}
