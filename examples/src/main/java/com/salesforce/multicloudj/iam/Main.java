package com.salesforce.multicloudj.iam;

import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.examples.AppConfig;
import com.salesforce.multicloudj.iam.client.IamClient;
import com.salesforce.multicloudj.iam.model.AttachInlinePolicyRequest;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.Effect;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.StorageActions;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class demonstrating IAM operations across different cloud providers. This example shows how
 * to use the multicloudj library for identity and access management operations.
 *
 * <p>Usage: {@code java -cp examples/target/multicloudj-examples-<version>.jar
 * com.salesforce.multicloudj.iam.Main [provider-id]}. The provider id defaults to the global {@code
 * provider} and the region comes from the global {@code region}. Tenant, service account and
 * policy values come from the {@code iam.*} keys in {@code examples.properties}.
 */
public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  // Demo settings
  private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  // Runtime configuration, resolved from examples.properties
  private final String provider;
  private final String region;
  private final String tenantId;
  private final String serviceAccount;
  private final String storageResource;
  private final String policyRoleName;

  /** Constructor that accepts provider configuration; other values come from configuration. */
  public Main(String provider) {
    this.provider = provider;
    this.region = AppConfig.get("region");
    this.tenantId = AppConfig.get("iam.tenant.id");
    this.serviceAccount = AppConfig.get("iam.service.account");
    this.storageResource = AppConfig.get("iam.storage.resource");
    this.policyRoleName = AppConfig.get("iam.policy.role.name");
  }

  public static void main(String[] args) {
    // Parse command line arguments
    String provider = parseProvider(args);
    Main main = new Main(provider);

    // Display welcome banner
    printWelcomeBanner();

    // Display configuration
    main.printConfiguration();

    main.runDemo();

    // Display completion banner
    printCompletionBanner();

    // Close reader
    try {
      reader.close();
    } catch (IOException e) {
      // Ignore
    }
  }

  /** Print a welcome banner for the demo. */
  private static void printWelcomeBanner() {
    logger.info("");
    logger.info("╔══════════════════════════════════════════════════════════════════════════════╗");
    logger.info("║                    🚀 MultiCloudJ IAM Demo 🚀                                 ║");
    logger.info("║                 Cross-Cloud Identity & Access Management                     ║");
    logger.info("╚══════════════════════════════════════════════════════════════════════════════╝");
    logger.info("");
  }

  /** Print the configuration being used. */
  private void printConfiguration() {
    logger.info("📋 Configuration:");
    logger.info("   Provider: {}", provider);
    logger.info("   Region: {}", region);
    logger.info("   Tenant ID: {}", tenantId);
    logger.info("   Service Account: {}", serviceAccount);
    logger.info("");
    waitForEnter("Press Enter to start the demo...");
  }

  /** Print a completion banner. */
  private static void printCompletionBanner() {
    logger.info("");
    logger.info("╔══════════════════════════════════════════════════════════════════════════════╗");
    logger.info("║                    ✅ Demo Completed Successfully! ✅                       ║");
    logger.info("║                    Thanks for trying MultiCloudJ!                          ║");
    logger.info("╚══════════════════════════════════════════════════════════════════════════════╝");
    logger.info("");
    waitForEnter("Press Enter to exit...");
  }

  /** Parse provider from command line arguments, falling back to configuration. */
  private static String parseProvider(String[] args) {
    if (args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
      return args[0].trim();
    }
    // Never hardcode the provider id; resolve it from configuration (see examples.properties)
    return AppConfig.provider();
  }

  /** Wait for user to press Enter key. */
  private static void waitForEnter(String message) {
    logger.info(message);
    try {
      reader.readLine();
    } catch (IOException e) {
      // If there's an error reading input, just continue
      logger.info("(Continuing automatically...)");
    }
  }

  /** Display a success message with emoji. */
  private static void showSuccess(String message) {
    logger.info("✅ {}", message);
  }

  /** Display an info message with emoji. */
  private static void showInfo(String message) {
    logger.info("ℹ️  {}", message);
  }

  /** Display an error message with emoji. */
  private static void showError(String message) {
    logger.error("❌ {}", message);
  }

  /** Display a section header. */
  private static void showSectionHeader(String title) {
    logger.info("");
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("📚 {}", title);
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("");
  }

  /** Display a section header with pause for major transitions. */
  private static void showSectionHeaderWithPause(String title) {
    logger.info("");
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("📚 {}", title);
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("");
    waitForEnter("Press Enter to start this section...");
  }

  /** Main demo method that orchestrates all the IAM operations. */
  private void runDemo() {
    showInfo("Starting IAM demo with provider: " + provider);

    try {
      // Run different operation demos
      demonstrateIdentityLifecycle();
      demonstratePolicyManagement();
      demonstrateErrorHandling();

      showSuccess("IAM demo completed successfully!");
    } catch (Exception e) {
      showError("Demo failed: " + e.getMessage());
      logger.error("Demo failed", e);
    }
  }

  /** Demonstrate basic identity lifecycle operations. */
  private void demonstrateIdentityLifecycle() {
    showSectionHeaderWithPause("Identity Lifecycle Operations");

    String testIdentity = "demo-identity-" + System.currentTimeMillis();

    // Create identity
    showInfo("Creating identity: " + testIdentity);
    try {
      String identityId = createIdentity(testIdentity);
      showSuccess("Created identity with ID: " + identityId);
    } catch (Exception e) {
      showError("Failed to create identity: " + e.getMessage());
      return;
    }

    // Get identity
    showInfo("Retrieving identity details...");
    try {
      String identityDetails = getIdentity(testIdentity);
      showSuccess("Retrieved identity: " + identityDetails);
    } catch (Exception e) {
      showError("Failed to get identity: " + e.getMessage());
    }

    // Delete identity
    waitForEnter("Press Enter to delete the identity (check cloud console before proceeding)...");
    showInfo("Deleting identity: " + testIdentity);
    try {
      deleteIdentity(testIdentity);
      showSuccess("Successfully deleted identity: " + testIdentity);
    } catch (Exception e) {
      showError("Failed to delete identity: " + e.getMessage());
    }

    waitForEnter(
        "Press Enter to verify identity deletion (check cloud console to confirm deletion)...");
    // Verify deletion by trying to get the identity again
    showInfo("Verifying identity deletion...");
    try {
      getIdentity(testIdentity);
      showError("Identity still exists after deletion");
    } catch (ResourceNotFoundException e) {
      showSuccess("Identity successfully deleted and verified");
    } catch (Exception e) {
      showError("Unexpected exception during verification: " + e.getMessage());
    }

    waitForEnter("Press Enter to continue to policy management...");
  }

  /** Demonstrate policy management operations. */
  private void demonstratePolicyManagement() {
    showSectionHeaderWithPause("Policy Management Operations");

    // Create and attach inline policy
    showInfo("Creating and attaching inline policy...");
    try {
      attachStoragePolicy();
      showSuccess("Attached storage policy successfully");
    } catch (Exception e) {
      showError("Failed to attach policy: " + e.getMessage());
      return;
    }

    // Get policy details
    showInfo("Retrieving policy details...");
    try {
      String policyDetails = getPolicyDetails();
      showSuccess("Policy details retrieved");
    } catch (Exception e) {
      showError("Failed to get policy details: " + e.getMessage());
    }

    // List attached policies
    showInfo("Listing attached policies...");
    try {
      List<String> policies = listAttachedPolicies();
      showSuccess("Found " + policies.size() + " attached policies");
      policies.forEach(policy -> logger.info("   - {}", policy));
    } catch (Exception e) {
      showError("Failed to list policies: " + e.getMessage());
    }

    // Remove a policy
    waitForEnter(
        "Press Enter to remove the storage policy (check cloud console before proceeding)...");
    showInfo("Removing storage policy...");
    try {
      removePolicy("storage-policy");
      showSuccess("Successfully removed storage policy");
    } catch (Exception e) {
      showError("Failed to remove policy: " + e.getMessage());
    }

    waitForEnter(
        "Press Enter to verify policy removal (check cloud console to confirm removal)...");
    // Verify policy removal by listing policies again
    showInfo("Verifying policy removal...");
    try {
      List<String> remainingPolicies = listAttachedPolicies();
      showSuccess("Policy removal verified - " + remainingPolicies.size() + " policies remaining");
      remainingPolicies.forEach(policy -> logger.info("   - {}", policy));
    } catch (Exception e) {
      showError("Failed to verify policy removal: " + e.getMessage());
    }

    waitForEnter("Press Enter to continue to error handling examples...");
  }

  /** Demonstrate error handling scenarios. */
  private void demonstrateErrorHandling() {
    showSectionHeaderWithPause("Error Handling Examples");

    // Try to get non-existent identity
    showInfo("Testing ResourceNotFoundException with non-existent identity...");
    try {
      getIdentity("non-existent-identity-" + System.currentTimeMillis());
      showError("Expected ResourceNotFoundException was not thrown");
    } catch (ResourceNotFoundException e) {
      showSuccess("Correctly caught ResourceNotFoundException: " + e.getMessage());
    } catch (Exception e) {
      showError("Unexpected exception: " + e.getMessage());
    }

    // Test idempotent createIdentity operation
    showInfo("Testing idempotent createIdentity (should succeed even if identity exists)...");
    String idempotentIdentity = "idempotent-test-" + System.currentTimeMillis();
    try {
      // First creation
      createIdentity(idempotentIdentity);
      showSuccess("First identity creation successful");

      // Second creation (should succeed due to idempotency)
      createIdentity(idempotentIdentity);
      showSuccess("Second identity creation succeeded (idempotent operation)");
    } catch (Exception e) {
      showError("Unexpected exception during idempotent test: " + e.getMessage());
    }

    // Clean up the idempotent identity
    try {
      deleteIdentity(idempotentIdentity);
    } catch (Exception e) {
      // Ignore cleanup errors
    }

    waitForEnter("Press Enter to continue to cleanup...");
  }

  /** Initialize the IAM client with appropriate configuration. */
  private IamClient initializeClient() {
    return IamClient.builder(provider)
        .withRegion(region)
        .build();
  }

  /** Create a new identity with trust configuration. */
  private String createIdentity(String identityName) throws Exception {
    try (IamClient iamClient = initializeClient()) {
      TrustConfiguration trustConfig =
          TrustConfiguration.builder().addTrustedPrincipal(serviceAccount).build();

      CreateOptions options = CreateOptions.builder().build();

      return iamClient.createIdentity(
          identityName,
          "Demo IAM Identity for testing",
          tenantId,
          region,
          Optional.of(trustConfig),
          Optional.of(options));
    }
  }

  /** Retrieve identity details. */
  private String getIdentity(String identityName) throws Exception {
    try (IamClient iamClient = initializeClient()) {
      return iamClient.getIdentity(identityName, tenantId, region);
    }
  }

  /** Delete an identity. */
  private void deleteIdentity(String identityName) throws Exception {
    try (IamClient iamClient = initializeClient()) {
      iamClient.deleteIdentity(identityName, tenantId, region);
    }
  }

  /**
   * Attach a storage policy using substrate-neutral actions. Each provider translates these
   * actions into its native permissions or roles.
   */
  private void attachStoragePolicy() throws Exception {
    try (IamClient iamClient = initializeClient()) {
      // Create a comprehensive policy document using substrate-neutral actions
      PolicyDocument policyDocument =
          PolicyDocument.builder()
              .version("2024-01-01")
              .statement(
                  Statement.builder()
                      .sid("StorageReadAccess")
                      .effect(Effect.ALLOW)
                      .action(StorageActions.GET_OBJECT)
                      .action(StorageActions.LIST_BUCKET)
                      .resource(storageResource)
                      .build())
              .statement(
                  Statement.builder()
                      .sid("StorageWriteAccess")
                      .effect(Effect.ALLOW)
                      .action(StorageActions.PUT_OBJECT)
                      .action(StorageActions.DELETE_OBJECT)
                      .resource(storageResource)
                      .build())
              .statement(
                  Statement.builder()
                      .sid("StorageFullAccess")
                      .effect(Effect.ALLOW)
                      .action(StorageActions.ALL)
                      .resource(storageResource)
                      .build())
              .build();

      iamClient.attachInlinePolicy(
          AttachInlinePolicyRequest.builder()
              .policyDocument(policyDocument)
              .tenantId(tenantId)
              .region(region)
              .identityName(serviceAccount)
              .build());
    }
  }

  /** Get details of a specific inline policy. */
  private String getPolicyDetails() throws Exception {
    try (IamClient iamClient = initializeClient()) {
      GetInlinePolicyDetailsRequest request =
          GetInlinePolicyDetailsRequest.builder()
              .identityName(serviceAccount)
              .policyName("storage-policy")
              .roleName(policyRoleName)
              .tenantId(tenantId)
              .region(region)
              .build();

      return iamClient.getInlinePolicyDetails(request);
    }
  }

  /** List all policies attached to an identity. */
  private List<String> listAttachedPolicies() throws Exception {
    try (IamClient iamClient = initializeClient()) {
      GetAttachedPoliciesRequest request =
          GetAttachedPoliciesRequest.builder()
              .identityName(serviceAccount)
              .tenantId(tenantId)
              .region(region)
              .build();

      return iamClient.getAttachedPolicies(request);
    }
  }

  /** Remove a policy from an identity. */
  private void removePolicy(String policyName) throws Exception {
    try (IamClient iamClient = initializeClient()) {
      iamClient.removePolicy(serviceAccount, policyName, tenantId, region);
    }
  }
}
