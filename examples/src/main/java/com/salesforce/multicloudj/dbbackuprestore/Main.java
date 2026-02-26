package com.salesforce.multicloudj.dbbackuprestore;

import com.salesforce.multicloudj.dbbackuprestore.client.DBBackupRestoreClient;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Main class demonstrating DBBackupRestore operations across different cloud providers.
 * This example shows how to use the multicloudj library for database backup and restore operations.
 * Usage: java -cp ... com.salesforce.multicloudj.dbbackuprestore.Main [provider] [resource-name]
 *   - provider: Cloud provider (aws, gcp-firestore, ali) - defaults to "aws"
 *   - resource-name: Table/database resource name - defaults to AWS DynamoDB table ARN
 * Examples:
 *   java -cp ... com.salesforce.multicloudj.dbbackuprestore.Main
 *   java -cp ... com.salesforce.multicloudj.dbbackuprestore.Main aws "arn:aws:dynamodb:us-west-2:123456789012:table/my-table"
 *   java -cp ... com.salesforce.multicloudj.dbbackuprestore.Main gcp-firestore "projects/my-project/locations/us-west-2"
 *   java -cp ... com.salesforce.multicloudj.dbbackuprestore.Main ali "my-tablestore-instance"
 */
public class Main {

    // Default Configuration
    private static final String DEFAULT_PROVIDER = "gcp-firestore";
    private static final String DEFAULT_RESOURCE_NAME =
            "projects/substrate-sdk-gcp-poc1/databases/(default)/documents/docstore-test-1";
    private static final String REGION = "projects/substrate-sdk-gcp-poc1/locations/nam5";

    // Demo settings
    private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    // Runtime configuration
    private final String provider;
    private final String resourceName;

    // State shared across demo steps
    private String restoreId;

    public Main(String provider, String resourceName) {
        this.provider = provider;
        this.resourceName = resourceName;
    }

    public static void main(String[] args) {
        String provider = parseProvider(args);
        String resourceName = parseResourceName(args);

        printWelcomeBanner();
        printConfiguration(provider, resourceName);

        Main main = new Main(provider, resourceName);
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
        return DEFAULT_PROVIDER;
    }

    private static String parseResourceName(String[] args) {
        if (args.length > 1 && args[1] != null && !args[1].trim().isEmpty()) {
            return args[1].trim();
        }
        return DEFAULT_RESOURCE_NAME;
    }

    private static void printWelcomeBanner() {
        System.out.println();
        System.out.println("========================================================================");
        System.out.println("          MultiCloudJ DBBackupRestore Demo");
        System.out.println("          Cross-Cloud Database Backup & Restore");
        System.out.println("========================================================================");
        System.out.println();
    }

    private static void printConfiguration(String provider, String resourceName) {
        System.out.println("Configuration:");
        System.out.println("   Provider:  " + provider);
        System.out.println("   Resource:  " + resourceName);
        System.out.println("   Region:    " + REGION);
        System.out.println();
        waitForEnter("Press Enter to start the demo...");
    }

    private static void printCompletionBanner() {
        System.out.println();
        System.out.println("========================================================================");
        System.out.println("          Demo Completed Successfully!");
        System.out.println("          Thanks for trying MultiCloudJ!");
        System.out.println("========================================================================");
        System.out.println();
    }

    private static void waitForEnter(String message) {
        System.out.print(message);
        try {
            reader.readLine();
        } catch (IOException e) {
            System.out.println("(Continuing automatically...)");
        }
    }

    private static void showInfo(String message) {
        System.out.println("[INFO] " + message);
    }

    private static void showSuccess(String message) {
        System.out.println("[OK]   " + message);
    }

    private static void showSectionHeader(String title) {
        System.out.println();
        System.out.println("------------------------------------------------------------------------");
        System.out.println("  " + title);
        System.out.println("------------------------------------------------------------------------");
        System.out.println();
        waitForEnter("Press Enter to start this section...");
    }

    /**
     * Create a DBBackupRestoreClient with the configured provider, region, and resource.
     */
    private DBBackupRestoreClient createClient() {
        return DBBackupRestoreClient.builder(provider)
                .withRegion(REGION)
                .withResourceName(resourceName)
                .build();
    }

    /**
     * Main demo method that orchestrates all the DBBackupRestore operations.
     */
    private void runDemo() {
        showInfo("Starting DBBackupRestore demo with provider: " + provider);
        demonstrateListBackups();
        demonstrateGetBackup();
        demonstrateRestoreBackup();
        demonstrateGetRestoreJob();
        showSuccess("DBBackupRestore demo completed successfully!");
    }

    /**
     * Demonstrate listing all available backups for the configured resource.
     */
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
                    System.out.printf("  [%d] ID:       %s%n", i + 1, backup.getBackupId());
                    System.out.printf("      Resource: %s%n", backup.getResourceName());
                    System.out.printf("      Status:   %s%n", backup.getStatus());
                    System.out.printf("      Size:     %d bytes%n", backup.getSizeInBytes());
                    System.out.printf("      Created:  %s%n", backup.getCreationTime());
                    System.out.println();
                }
                showSuccess("Found " + backups.size() + " backup(s).");
            }
        } catch (Exception e) {
            System.out.println("[ERROR] Failed to list backups: " + e.getMessage());
        }
    }

    /**
     * Demonstrate getting details of a specific backup.
     * Prompts the user for a backup ID to look up.
     */
    private void demonstrateGetBackup() {
        showSectionHeader("GET BACKUP");

        System.out.print("Enter a backup ID to look up (or press Enter to skip): ");
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

            System.out.printf("  Backup ID:   %s%n", backup.getBackupId());
            System.out.printf("  Resource:    %s%n", backup.getResourceName());
            System.out.printf("  Status:      %s%n", backup.getStatus());
            System.out.printf("  Created:     %s%n", backup.getCreationTime());
            System.out.printf("  Expires:     %s%n", backup.getExpiryTime());
            System.out.printf("  Size:        %d bytes%n", backup.getSizeInBytes());
            System.out.printf("  Description: %s%n", backup.getDescription());
            System.out.printf("  Vault ID:    %s%n", backup.getVaultId());
            showSuccess("Backup details retrieved.");
        } catch (Exception e) {
            System.out.println("[ERROR] Failed to get backup: " + e.getMessage());
        }
    }

    /**
     * Demonstrate restoring from a backup.
     * Prompts the user for restore parameters. The restore is asynchronous -
     * it returns a restore ID that can be tracked with getRestoreJob.
     */
    private void demonstrateRestoreBackup() {
        showSectionHeader("RESTORE BACKUP");

        System.out.print("Enter a backup ID to restore from (or press Enter to skip): ");
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

        System.out.print("Enter target resource name for restore: ");
        String targetResource;
        try {
            targetResource = reader.readLine();
        } catch (IOException e) {
            targetResource = null;
        }
        if (targetResource != null) {
            targetResource = targetResource.trim();
        }

        System.out.print("Enter role ID (AWS) or vault ID (Ali) if required, or press Enter to skip: ");
        String roleOrVaultId;
        try {
            roleOrVaultId = reader.readLine();
        } catch (IOException e) {
            roleOrVaultId = null;
        }

        RestoreRequest.RestoreRequestBuilder requestBuilder = RestoreRequest.builder()
                .backupId(backupId)
                .targetResource(targetResource);

        if (roleOrVaultId != null && !roleOrVaultId.trim().isEmpty()) {
            roleOrVaultId = roleOrVaultId.trim();
            if ("ali".equals(provider)) {
                requestBuilder.vaultId(roleOrVaultId);
            } else {
                requestBuilder.roleId(roleOrVaultId);
            }
        }

        showInfo("Starting restore from backup: " + backupId);
        try (DBBackupRestoreClient client = createClient()) {
            restoreId = client.restoreBackup(requestBuilder.build());
            showSuccess("Restore started with ID: " + restoreId);
        } catch (Exception e) {
            System.out.println("[ERROR] Failed to start restore: " + e.getMessage());
        }
    }

    /**
     * Demonstrate checking the status of a restore operation.
     * Uses the restore ID from the previous step, or prompts the user for one.
     */
    private void demonstrateGetRestoreJob() {
        showSectionHeader("GET RESTORE JOB");

        String jobId = restoreId;
        if (jobId == null || jobId.isEmpty()) {
            System.out.print("Enter a restore job ID to look up (or press Enter to skip): ");
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

            System.out.printf("  Restore ID:  %s%n", restore.getRestoreId());
            System.out.printf("  Backup ID:   %s%n", restore.getBackupId());
            System.out.printf("  Target:      %s%n", restore.getTargetResource());
            System.out.printf("  Status:      %s%n", restore.getStatus());
            System.out.printf("  Started:     %s%n", restore.getStartTime());
            System.out.printf("  Ended:       %s%n", restore.getEndTime());
            showSuccess("Restore job details retrieved.");
        } catch (Exception e) {
            System.out.println("[ERROR] Failed to get restore job: " + e.getMessage());
        }
    }
}
