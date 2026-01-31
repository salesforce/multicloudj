package com.salesforce.multicloudj.dbbackrestore.client;

import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract conformance test for database backup and restore operations.
 * Provider-specific implementations should extend this class to ensure
 * compliance with the DBBackRestore API contract.
 *
 * @since 0.2.26
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDBBackRestoreIT {

  /**
   * Harness interface for provider-specific test setup.
   */
  public interface Harness extends AutoCloseable {
    /**
     * Creates a DBBackRestore driver instance for testing.
     *
     * @return an AbstractDBBackRestore implementation
     */
    AbstractDBBackRestore createDBBackRestoreDriver();

    /**
     * Gets the wiremock server port for recording/replaying HTTP interactions.
     *
     * @return the port number
     */
    int getPort();

    /**
     * Gets wiremock extensions to use for proxying.
     *
     * @return list of fully qualified class names for wiremock extensions
     */
    List<String> getWiremockExtensions();

    /**
     * Checks if the provider supports backup restore operations.
     *
     * @return true if backup restore is supported
     */
    default boolean supportsBackupRestore() {
      return false;
    }

    /**
     * Provider-specific options for restore (e.g. AWS requires "iamRoleArn").
     * Default is empty. Override to supply options needed for restore tests.
     *
     * @return map of provider-specific restore options
     */
    default java.util.Map<String, String> getRestoreOptions() {
      return java.util.Collections.emptyMap();
    }
  }

  protected abstract Harness createHarness();

  protected Harness harness;

  @BeforeAll
  public void setUp() {
    harness = createHarness();
  }

  @AfterAll
  public void tearDown() throws Exception {
    if (harness != null) {
      harness.close();
    }
  }

  /**
   * Tests listing backups for a collection/table.
   * This test verifies that the listBackups operation returns a list
   * (which may be empty if no backups exist).
   */
  @Test
  public void testListBackups() {
    if (!harness.supportsBackupRestore()) {
      System.out.println("Provider does not support backup restore, skipping test");
      return;
    }

    DBBackRestoreClient client = new DBBackRestoreClient(
        harness.createDBBackRestoreDriver());

    try {
      List<Backup> backups = client.listBackups();
      Assertions.assertNotNull(backups, "Backup list should not be null");
      
      // If backups exist, verify their structure
      if (!backups.isEmpty()) {
        for (Backup backup : backups) {
          Assertions.assertNotNull(backup.getBackupId(), "Backup ID should not be null");
          Assertions.assertNotNull(backup.getStatus(), "Backup status should not be null");
          System.out.println("Found backup: " + backup.getBackupId() 
              + " with status: " + backup.getStatus());
        }
      } else {
        System.out.println("No backups found for the collection");
      }
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        System.err.println("Error closing client: " + e.getMessage());
      }
    }
  }

  /**
   * Tests getting backup details by ID.
   * This test verifies that backup metadata can be retrieved for an existing backup.
   */
  @Test
  public void testGetBackup() {
    if (!harness.supportsBackupRestore()) {
      System.out.println("Provider does not support backup restore, skipping test");
      return;
    }

    DBBackRestoreClient client = new DBBackRestoreClient(
        harness.createDBBackRestoreDriver());

    try {
      List<Backup> backups = client.listBackups();
      if (backups.isEmpty()) {
        System.out.println("No backups available to test getBackup, skipping");
        return;
      }

      // Get details for the first backup
      Backup backup = backups.get(0);
      Backup retrieved = client.getBackup(backup.getBackupId());

      Assertions.assertNotNull(retrieved, "Retrieved backup should not be null");
      Assertions.assertEquals(backup.getBackupId(), retrieved.getBackupId(),
          "Backup IDs should match");
      Assertions.assertNotNull(retrieved.getStatus(), "Backup status should not be null");
      
      System.out.println("Retrieved backup: " + retrieved.getBackupId() 
          + " with status: " + retrieved.getStatus());
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        System.err.println("Error closing client: " + e.getMessage());
      }
    }
  }

  /**
   * Tests getting backup status by ID.
   * This test verifies that backup status can be retrieved for an existing backup.
   */
  @Test
  public void testGetBackupStatus() {
    if (!harness.supportsBackupRestore()) {
      System.out.println("Provider does not support backup restore, skipping test");
      return;
    }

    DBBackRestoreClient client = new DBBackRestoreClient(
        harness.createDBBackRestoreDriver());

    try {
      List<Backup> backups = client.listBackups();
      if (backups.isEmpty()) {
        System.out.println("No backups available to test getBackupStatus, skipping");
        return;
      }

      // Get status for the first backup
      Backup backup = backups.get(0);
      BackupStatus status = client.getBackupStatus(backup.getBackupId());

      Assertions.assertNotNull(status, "Backup status should not be null");
      System.out.println("Backup " + backup.getBackupId() + " has status: " + status);
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        System.err.println("Error closing client: " + e.getMessage());
      }
    }
  }

  /**
   * Tests restoring from a backup.
   * This test verifies that a backup can be restored to a new collection/table.
   * Note: This test may take a long time to complete depending on backup size.
   */
  @Test
  public void testRestoreBackup() {
    if (!harness.supportsBackupRestore()) {
      System.out.println("Provider does not support backup restore, skipping test");
      return;
    }

    DBBackRestoreClient client = new DBBackRestoreClient(
        harness.createDBBackRestoreDriver());

    try {
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

      // Create restore request (include provider-specific options e.g. AWS iamRoleArn)
      RestoreRequest.RestoreRequestBuilder requestBuilder = RestoreRequest.builder()
          .backupId(availableBackup.getBackupId())
          .targetCollectionName("restored-collection-" + System.currentTimeMillis());
      java.util.Map<String, String> restoreOptions = harness.getRestoreOptions();
      if (restoreOptions != null && !restoreOptions.isEmpty()) {
        requestBuilder.options(restoreOptions);
      }
      RestoreRequest request = requestBuilder.build();

      // Perform restore
      client.restoreBackup(request);

      System.out.println("Restore operation initiated from backup: " 
          + availableBackup.getBackupId());
      System.out.println("Target collection: " + request.getTargetCollectionName());

      // Note: Restore is typically an async operation, so we don't verify completion here
      // In a real test, you might want to poll for completion or verify the restored data
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        System.err.println("Error closing client: " + e.getMessage());
      }
    }
  }

  /**
   * Tests deleting a backup.
   * WARNING: This test will actually delete a backup if one exists.
   * It should only be run in a test environment with disposable backups.
   * 
   * This test is commented out by default to prevent accidental deletion.
   * Uncomment to enable in environments where backup deletion is safe.
   */
  // @Test
  public void testDeleteBackup() {
    if (!harness.supportsBackupRestore()) {
      System.out.println("Provider does not support backup restore, skipping test");
      return;
    }

    DBBackRestoreClient client = new DBBackRestoreClient(
        harness.createDBBackRestoreDriver());

    try {
      List<Backup> backups = client.listBackups();
      if (backups.isEmpty()) {
        System.out.println("No backups available to test delete, skipping");
        return;
      }

      // Find a backup that is safe to delete (you might want to create one specifically for this test)
      // For now, we'll skip this test to avoid deleting production backups
      System.out.println("Delete test skipped to prevent accidental data loss");
      
      // Uncomment to enable:
      // Backup backup = backups.get(0);
      // client.deleteBackup(backup.getBackupId());
      // System.out.println("Deleted backup: " + backup.getBackupId());
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        System.err.println("Error closing client: " + e.getMessage());
      }
    }
  }
}
