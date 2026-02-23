package com.salesforce.multicloudj.dbbackuprestore;

import com.salesforce.multicloudj.dbbackuprestore.client.DBBackupRestoreClient;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;

import java.util.List;

public class Main {

    static String provider = "aws";
    static String region = "us-west-2";
    static String resourceName = "arn:aws:dynamodb:us-west-2:654654370895:table/docstore-test-1";
    static String restoreId = "";

    public static void main(String[] args) throws Exception {
        listBackups();
        getBackup();
        restoreBackup();
        getRestoreJob();
    }

    /**
     * Lists all available backups for the configured resource.
     */
    public static void listBackups() throws Exception {
        try (DBBackupRestoreClient client = DBBackupRestoreClient.builder(provider)
                .withRegion(region)
                .withResourceName(resourceName)
                .build()) {

            List<Backup> backups = client.listBackups();
            for (Backup backup : backups) {
                System.out.printf("Backup ID: %s, Resource: %s, Status: %s, Size: %d bytes%n",
                        backup.getBackupId(),
                        backup.getResourceName(),
                        backup.getStatus(),
                        backup.getSizeInBytes());
            }
        }
    }

    /**
     * Gets details of a specific backup by its ID.
     */
    public static void getBackup() throws Exception {
        try (DBBackupRestoreClient client = DBBackupRestoreClient.builder(provider)
                .withRegion(region)
                .withResourceName(resourceName)
                .build()) {

            String backupId = "arn:aws:backup:us-west-2:654654370895:recovery-point:c5baed67-6777-4052-8afe-024d7f2e964a";
            Backup backup = client.getBackup(backupId);

            System.out.printf("Backup ID: %s%n", backup.getBackupId());
            System.out.printf("Resource: %s%n", backup.getResourceName());
            System.out.printf("Status: %s%n", backup.getStatus());
            System.out.printf("Created: %s%n", backup.getCreationTime());
            System.out.printf("Expires: %s%n", backup.getExpiryTime());
            System.out.printf("Size: %d bytes%n", backup.getSizeInBytes());
            System.out.printf("Description: %s%n", backup.getDescription());
            System.out.printf("Vault ID: %s%n", backup.getVaultId());
        }
    }

    /**
     * Restores a backup to a target resource.
     * The restore operation is asynchronous - it returns a restore ID that
     * can be used to track the progress of the operation.
     */
    public static void restoreBackup() throws Exception {
        try (DBBackupRestoreClient client = DBBackupRestoreClient.builder(provider)
                .withRegion(region)
                .withResourceName(resourceName)
                .build()) {

            RestoreRequest request = RestoreRequest.builder()
                    .backupId("arn:aws:backup:us-west-2:654654370895:recovery-point:c5baed67-6777-4052-8afe-024d7f2e964a")
                    .targetResource("my-restored-table")
                    .roleId("arn:aws:iam::654654370895:role/chameleon-multi--f4msu63ppffhs")
                    .build();

            restoreId = client.restoreBackup(request);
            System.out.printf("Restore started with ID: %s%n", restoreId);
        }
    }

    /**
     * Gets the status of a restore operation.
     * Since restores are asynchronous, this can be used to poll for completion.
     */
    public static void getRestoreJob() throws Exception {
        try (DBBackupRestoreClient client = DBBackupRestoreClient.builder(provider)
                .withRegion(region)
                .withResourceName(resourceName)
                .build()) {

            Restore restore = client.getRestoreJob(restoreId);

            System.out.printf("Restore ID: %s%n", restore.getRestoreId());
            System.out.printf("Backup ID: %s%n", restore.getBackupId());
            System.out.printf("Target: %s%n", restore.getTargetResource());
            System.out.printf("Status: %s%n", restore.getStatus());
            System.out.printf("Started: %s%n", restore.getStartTime());
            System.out.printf("Ended: %s%n", restore.getEndTime());
        }
    }
}
