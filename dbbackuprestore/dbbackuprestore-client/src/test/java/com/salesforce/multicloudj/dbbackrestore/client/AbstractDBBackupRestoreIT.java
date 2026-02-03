package com.salesforce.multicloudj.dbbackuprestore.client;

import com.salesforce.multicloudj.common.util.UUID;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Random;

/**
 * Abstract conformance test for database backup and restore operations.
 * Provider-specific implementations should extend this class to ensure
 * compliance with the DBBackupRestore API contract.
 *
 * @since 0.2.25
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDBBackupRestoreIT {

    /**
     * Harness interface for provider-specific test setup.
     */
    public interface Harness extends AutoCloseable {
        /**
         * Creates a DBBackupRestore driver instance for testing.
         *
         * @return an AbstractDBBackupRestore implementation
         */
        com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore createDBBackupRestoreDriver();

        /**
         * Gets the backup service endpoint for the provider.
         *
         * @return the endpoint URL
         */
        String getBackupEndpoint();

        /**
         * Gets the wiremock server port for recording/replaying HTTP interactions.
         *
         * @return the port number
         */
        int getPort();

        /**
         * Gets the IAM role for restore operations.*
         *
         * @return the IAM role ARN, or null if not applicable
         */
        default String getRoleId() {
            return null;
        }

        /**
         * Gets the vault ID for restore operations if applicable.
         *
         * @return the vault ID, or null if not applicable
         */
        default String getVaultId() {
            return null;
        }
    }

    protected abstract Harness createHarness();

    protected Harness harness;

    /**
     * Initializes the WireMock server before all tests.
     */
    @BeforeAll
    public void initializeWireMockServer() {
        Random random = new Random(1234L);
        UUID.setUuidSupplier(() -> new java.util.UUID(random.nextLong(), random.nextLong()).toString());
        harness = createHarness();
        TestsUtil.startWireMockServer("src/test/resources", harness.getPort());
    }

    /**
     * Shuts down the WireMock server after all tests.
     */
    @AfterAll
    public void shutdownWireMockServer() throws Exception {
        TestsUtil.stopWireMockServer();
        if (harness != null) {
            harness.close();
        }
    }

    /**
     * Starts WireMock recording before each test.
     */
    @BeforeEach
    public void setupTestEnvironment() {
        TestsUtil.startWireMockRecording(harness.getBackupEndpoint());
    }

    /**
     * Stops WireMock recording after each test.
     */
    @AfterEach
    public void cleanupTestEnvironment() {
        TestsUtil.stopWireMockRecording();
    }

    /**
     * Tests listing backups for a table.
     * This test verifies that the listBackups operation returns a list
     * (which may be empty if no backups exist).
     */
    @Test
    public void testListBackups() throws Exception {
        try (DBBackupRestoreClient client = new DBBackupRestoreClient(
                harness.createDBBackupRestoreDriver())) {
            List<Backup> backups = client.listBackups();
            Assertions.assertNotNull(backups, "Backup list should not be null");
            Assertions.assertFalse(backups.isEmpty(), "Backup list should not be empty");
            for (Backup backup : backups) {
                Assertions.assertNotNull(backup.getBackupId(), "Backup ID should not be null");
                Assertions.assertNotNull(backup.getStatus(), "Backup status should not be null");
                System.out.println("Found backup: " + backup.getBackupId()
                        + " with status: " + backup.getStatus());
            }
        }
    }

    /**
     * Tests getting backup details by ID.
     * This test verifies that backup metadata can be retrieved for an existing backup.
     */
    @Test
    public void testGetBackup() throws Exception {
        try (DBBackupRestoreClient client = new DBBackupRestoreClient(
                harness.createDBBackupRestoreDriver())) {
            List<Backup> backups = client.listBackups();
            Assertions.assertFalse(backups.isEmpty(), "Backup list should not be empty");

            // Get details for the first backup
            Backup backup = backups.get(0);
            Backup retrieved = client.getBackup(backup.getBackupId());

            Assertions.assertNotNull(retrieved, "Retrieved backup should not be null");
            Assertions.assertEquals(backup.getBackupId(), retrieved.getBackupId(),
                    "Backup IDs should match");
            Assertions.assertNotNull(retrieved.getStatus(), "Backup status should not be null");

            System.out.println("Retrieved backup: " + retrieved.getBackupId()
                    + " with status: " + retrieved.getStatus());
        }
    }

    /**
     * Tests getting backup status by ID.
     * This test verifies that backup status can be retrieved for an existing backup.
     */
    @Test
    public void testGetBackupStatus() throws Exception {
        try (DBBackupRestoreClient client = new DBBackupRestoreClient(
                harness.createDBBackupRestoreDriver())) {
            List<Backup> backups = client.listBackups();
            Assertions.assertFalse(backups.isEmpty(), "Backup list should not be empty");

            // Get status for the first backup
            Backup backup = backups.get(0);
            BackupStatus status = client.getBackupStatus(backup.getBackupId());

            Assertions.assertNotNull(status, "Backup status should not be null");
            System.out.println("Backup " + backup.getBackupId() + " has status: " + status);
        }
    }

    /**
     * Tests restoring from a backup.
     * This test verifies that a backup can be restored to a new table.
     * Note: This test may take a long time to complete depending on backup size.
     */
    @Test
    public void testRestoreBackup() throws Exception {
        try (DBBackupRestoreClient client = new DBBackupRestoreClient(
                harness.createDBBackupRestoreDriver())) {
            List<Backup> backups = client.listBackups();
            if (backups.isEmpty()) {
                System.out.println("No backups available to test restore, skipping");
                return;
            }

            // Find an available backup
            Backup availableBackup = backups.stream()
                    .filter(b -> b.getStatus() == BackupStatus.AVAILABLE)
                    .findFirst()
                    .orElse(null);

            if (availableBackup == null) {
                System.out.println("No AVAILABLE backups to test restore, skipping");
                return;
            }

            // Create restore request (include provider-specific fields if needed)
            RestoreRequest.RestoreRequestBuilder requestBuilder = RestoreRequest.builder()
                    .backupId(availableBackup.getBackupId())
                    .targetResource("restored-collection-" + UUID.uniqueString());

            // Add provider-specific fields
            if (harness.getRoleId() != null) {
                requestBuilder.roleId(harness.getRoleId());
            }
            if (harness.getVaultId() != null) {
                requestBuilder.vaultId(harness.getVaultId());
            }

            RestoreRequest request = requestBuilder.build();

            // Perform restore
            client.restoreBackup(request);

            System.out.println("Restore operation initiated from backup: "
                    + availableBackup.getBackupId());
            System.out.println("Target collection: " + request.getTargetResource());

            // Note: Restore is typically an async operation, so we don't verify completion here
            // In a real test, you might want to poll for completion or verify the restored data
        }
    }
}
