package com.salesforce.multicloudj.dbbackuprestore;

import com.salesforce.multicloudj.dbbackuprestore.client.DBBackupRestoreClient;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
import com.salesforce.multicloudj.examples.AppConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class demonstrating DBBackupRestore operations across different cloud providers. This
 * example shows how to use the multicloudj library for database backup and restore operations.
 *
 * <p>Usage: {@code java -cp examples/target/multicloudj-examples-<version>.jar
 * com.salesforce.multicloudj.dbbackuprestore.Main [provider-id] [resource-name]}. The provider id
 * defaults to {@code dbbackuprestore.provider}, else the global {@code provider}; the resource name
 * defaults to {@code dbbackuprestore.resource.name}; the region comes from the global {@code
 * region} (see {@code examples.properties}).
 */
public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  // Demo settings
  private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  // Runtime configuration
  private final String provider;
  private final String resourceName;
  private final String region;

  // State shared across demo steps
  private String restoreId;

  public Main(String provider, String resourceName, String region) {
    this.provider = provider;
    this.resourceName = resourceName;
    this.region = region;
  }

  public static void main(String[] args) {
    String provider = parseProvider(args);
    String resourceName = parseResourceName(args);
    String region = AppConfig.get("region");

    printWelcomeBanner();
    printConfiguration(provider, resourceName, region);

    Main main = new Main(provider, resourceName, region);
    main.runDemo();

    printCompletionBanner();

    try {
      reader.close();
    } catch (IOException e) {
      // Ignore
    }
  }

  private static String parseProvider(String[] args) {
    if (args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
      return args[0].trim();
    }
    // Never hardcode the provider id; resolve it from configuration (see examples.properties)
    return AppConfig.provider("dbbackuprestore");
  }

  private static String parseResourceName(String[] args) {
    if (args.length > 1 && args[1] != null && !args[1].trim().isEmpty()) {
      return args[1].trim();
    }
    return AppConfig.get("dbbackuprestore.resource.name");
  }

  private static void printWelcomeBanner() {
    logger.info("");
    logger.info("========================================================================");
    logger.info("          MultiCloudJ DBBackupRestore Demo");
    logger.info("          Cross-Cloud Database Backup & Restore");
    logger.info("========================================================================");
    logger.info("");
  }

  private static void printConfiguration(String provider, String resourceName, String region) {
    logger.info("Configuration:");
    logger.info("   Provider:  {}", provider);
    logger.info("   Resource:  {}", resourceName);
    logger.info("   Region:    {}", region);
    logger.info("");
    waitForEnter("Press Enter to start the demo...");
  }

  private static void printCompletionBanner() {
    logger.info("");
    logger.info("========================================================================");
    logger.info("          Demo Completed Successfully!");
    logger.info("          Thanks for trying MultiCloudJ!");
    logger.info("========================================================================");
    logger.info("");
  }

  private static void waitForEnter(String message) {
    logger.info(message);
    try {
      reader.readLine();
    } catch (IOException e) {
      logger.info("(Continuing automatically...)");
    }
  }

  /** Prompts for a value and returns it trimmed, or {@code null} if the user skipped it. */
  private static String readOptional(String prompt) {
    logger.info(prompt);
    try {
      String value = reader.readLine();
      return value == null || value.trim().isEmpty() ? null : value.trim();
    } catch (IOException e) {
      return null;
    }
  }

  private static void showInfo(String message) {
    logger.info("{}", message);
  }

  private static void showSuccess(String message) {
    logger.info("[OK]   {}", message);
  }

  private static void showSectionHeader(String title) {
    logger.info("");
    logger.info("------------------------------------------------------------------------");
    logger.info("  {}", title);
    logger.info("------------------------------------------------------------------------");
    logger.info("");
    waitForEnter("Press Enter to start this section...");
  }

  /** Create a DBBackupRestoreClient with the configured provider, region, and resource. */
  private DBBackupRestoreClient createClient() {
    return DBBackupRestoreClient.builder(provider)
        .withRegion(region)
        .withResourceName(resourceName)
        .build();
  }

  /** Main demo method that orchestrates all the DBBackupRestore operations. */
  private void runDemo() {
    showInfo("Starting DBBackupRestore demo with provider: " + provider);
    demonstrateListBackups();
    demonstrateGetBackup();
    demonstrateRestoreBackup();
    demonstrateGetRestoreJob();
    showSuccess("DBBackupRestore demo completed successfully!");
  }

  /** Demonstrate listing all available backups for the configured resource. */
  private void demonstrateListBackups() {
    showSectionHeader("LIST BACKUPS");

    showInfo("Listing all backups for resource: " + resourceName);
    try (DBBackupRestoreClient client = createClient()) {
      List<Backup> backups = client.listBackups();

      if (backups.isEmpty()) {
        showInfo("No backups found for this resource.");
      } else {
        for (int i = 0; i < backups.size(); i++) {
          Backup backup = backups.get(i);
          logger.info("  [{}] ID:       {}", i + 1, backup.getBackupId());
          logger.info("      Resource: {}", backup.getResourceName());
          logger.info("      Status:   {}", backup.getStatus());
          logger.info("      Size:     {} bytes", backup.getSizeInBytes());
          logger.info("      Created:  {}", backup.getCreationTime());
          logger.info("");
        }
        showSuccess("Found " + backups.size() + " backup(s).");
      }
    } catch (Exception e) {
      logger.error("Failed to list backups: {}", e.getMessage());
    }
  }

  /**
   * Demonstrate getting details of a specific backup. Prompts the user for a backup ID to look up.
   */
  private void demonstrateGetBackup() {
    showSectionHeader("GET BACKUP");

    logger.info("Enter a backup ID to look up (or press Enter to skip): ");
    String backupId;
    try {
      backupId = reader.readLine();
    } catch (IOException e) {
      backupId = null;
    }

    if (backupId == null || backupId.trim().isEmpty()) {
      showInfo("Skipping get backup.");
      return;
    }
    backupId = backupId.trim();

    showInfo("Getting backup: " + backupId);
    try (DBBackupRestoreClient client = createClient()) {
      Backup backup = client.getBackup(backupId);

      logger.info("  Backup ID:   {}", backup.getBackupId());
      logger.info("  Resource:    {}", backup.getResourceName());
      logger.info("  Status:      {}", backup.getStatus());
      logger.info("  Created:     {}", backup.getCreationTime());
      logger.info("  Expires:     {}", backup.getExpiryTime());
      logger.info("  Size:        {} bytes", backup.getSizeInBytes());
      logger.info("  Description: {}", backup.getDescription());
      logger.info("  Vault ID:    {}", backup.getVaultId());
      showSuccess("Backup details retrieved.");
    } catch (Exception e) {
      logger.error("Failed to get backup: {}", e.getMessage());
    }
  }

  /**
   * Demonstrate restoring from a backup. Prompts the user for restore parameters. The restore is
   * asynchronous - it returns a restore ID that can be tracked with getRestoreJob.
   */
  private void demonstrateRestoreBackup() {
    showSectionHeader("RESTORE BACKUP");

    logger.info("Enter a backup ID to restore from (or press Enter to skip): ");
    String backupId;
    try {
      backupId = reader.readLine();
    } catch (IOException e) {
      backupId = null;
    }

    if (backupId == null || backupId.trim().isEmpty()) {
      showInfo("Skipping restore backup.");
      return;
    }
    backupId = backupId.trim();

    logger.info("Enter target resource name for restore: ");
    String targetResource;
    try {
      targetResource = reader.readLine();
    } catch (IOException e) {
      targetResource = null;
    }
    if (targetResource != null) {
      targetResource = targetResource.trim();
    }

    // Whether a role ID or a vault ID is required depends on the provider; prompt for both and let
    // the user supply whichever applies.
    String roleId = readOptional("Enter role ID if required, or press Enter to skip: ");
    String vaultId = readOptional("Enter vault ID if required, or press Enter to skip: ");

    RestoreRequest.RestoreRequestBuilder requestBuilder =
        RestoreRequest.builder().backupId(backupId).targetResource(targetResource);
    if (roleId != null) {
      requestBuilder.roleId(roleId);
    }
    if (vaultId != null) {
      requestBuilder.vaultId(vaultId);
    }

    showInfo("Starting restore from backup: " + backupId);
    try (DBBackupRestoreClient client = createClient()) {
      restoreId = client.restoreBackup(requestBuilder.build());
      showSuccess("Restore started with ID: " + restoreId);
    } catch (Exception e) {
      logger.error("Failed to start restore: {}", e.getMessage());
    }
  }

  /**
   * Demonstrate checking the status of a restore operation. Uses the restore ID from the previous
   * step, or prompts the user for one.
   */
  private void demonstrateGetRestoreJob() {
    showSectionHeader("GET RESTORE JOB");

    String jobId = restoreId;
    if (jobId == null || jobId.isEmpty()) {
      logger.info("Enter a restore job ID to look up (or press Enter to skip): ");
      try {
        jobId = reader.readLine();
      } catch (IOException e) {
        jobId = null;
      }
    } else {
      showInfo("Using restore ID from previous step: " + jobId);
    }

    if (jobId == null || jobId.trim().isEmpty()) {
      showInfo("Skipping get restore job.");
      return;
    }
    jobId = jobId.trim();

    showInfo("Getting restore job: " + jobId);
    try (DBBackupRestoreClient client = createClient()) {
      Restore restore = client.getRestoreJob(jobId);

      logger.info("  Restore ID:  {}", restore.getRestoreId());
      logger.info("  Backup ID:   {}", restore.getBackupId());
      logger.info("  Target:      {}", restore.getTargetResource());
      logger.info("  Status:      {}", restore.getStatus());
      logger.info("  Started:     {}", restore.getStartTime());
      logger.info("  Ended:       {}", restore.getEndTime());
      showSuccess("Restore job details retrieved.");
    } catch (Exception e) {
      logger.error("Failed to get restore job: {}", e.getMessage());
    }
  }
}
