package com.salesforce.multicloudj.iam;

import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.client.IamClient;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Optional;

/**
 * Main class demonstrating IAM operations across different cloud providers.
 * This example shows how to use the multicloudj library for identity and access management operations.
 *
 * Usage: java -jar iam-example.jar [provider]
 *   - provider: Cloud provider (gcp, aws, ali) - defaults to "gcp"
 *
 * Examples:
 *   java -jar iam-example.jar
 *   java -jar iam-example.jar aws
 *   java -jar iam-example.jar gcp
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // Constants
    private static final String DEFAULT_PROVIDER = "gcp";
    private static final String REGION = "us-central-1";
    private static final String TENANT_ID = "projects/substrate-sdk-gcp-poc1";
    private static final String SERVICE_ACCOUNT = "serviceAccount:multicloudjexample@substrate-sdk-gcp-poc1.iam.gserviceaccount.com";

    // Demo settings
    private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    // Runtime configuration
    private final String provider;

    /**
     * Constructor that accepts provider configuration.
     */
    public Main(String provider) {
        this.provider = provider;
    }

    public static void main(String[] args) {
        // Parse command line arguments
        String provider = parseProvider(args);

        // Display welcome banner
        printWelcomeBanner();

        // Display configuration
        printConfiguration(provider);

        Main main = new Main(provider);
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

    /**
     * Print a welcome banner for the demo.
     */
    private static void printWelcomeBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    🚀 MultiCloudJ IAM Demo 🚀                                 ║");
        System.out.println("║                 Cross-Cloud Identity & Access Management                     ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    /**
     * Print the configuration being used.
     */
    private static void printConfiguration(String provider) {
        System.out.println("📋 Configuration:");
        System.out.println("   Provider: " + provider);
        System.out.println("   Region: " + REGION);
        System.out.println("   Tenant ID: " + TENANT_ID);
        System.out.println("   Service Account: " + SERVICE_ACCOUNT);
        System.out.println();
        waitForEnter("Press Enter to start the demo...");
    }

    /**
     * Print a completion banner.
     */
    private static void printCompletionBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    ✅ Demo Completed Successfully! ✅                       ║");
        System.out.println("║                    Thanks for trying MultiCloudJ!                          ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        waitForEnter("Press Enter to exit...");
    }

    /**
     * Parse provider from command line arguments.
     * Defaults to DEFAULT_PROVIDER if not specified.
     */
    private static String parseProvider(String[] args) {
        if (args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
            return args[0].trim();
        }
        return DEFAULT_PROVIDER;
    }

    /**
     * Wait for user to press Enter key.
     */
    private static void waitForEnter(String message) {
        System.out.print(message);
        try {
            reader.readLine();
        } catch (IOException e) {
            // If there's an error reading input, just continue
            System.out.println("(Continuing automatically...)");
        }
    }

    /**
     * Display a success message with emoji.
     */
    private static void showSuccess(String message) {
        System.out.println("✅ " + message);
    }

    /**
     * Display an info message with emoji.
     */
    private static void showInfo(String message) {
        System.out.println("ℹ️  " + message);
    }

    /**
     * Display an error message with emoji.
     */
    private static void showError(String message) {
        System.out.println("❌ " + message);
    }

    /**
     * Display a section header.
     */
    private static void showSectionHeader(String title) {
        System.out.println();
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("📚 " + title);
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println();
    }

    /**
     * Display a section header with pause for major transitions.
     */
    private static void showSectionHeaderWithPause(String title) {
        System.out.println();
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("📚 " + title);
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println();
        waitForEnter("Press Enter to start this section...");
    }

    /**
     * Main demo method that orchestrates all the IAM operations.
     */
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

    /**
     * Demonstrate basic identity lifecycle operations.
     */
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

        waitForEnter("Press Enter to verify identity deletion (check cloud console to confirm deletion)...");
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

    /**
     * Demonstrate policy management operations.
     */
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
            policies.forEach(policy -> System.out.println("   - " + policy));
        } catch (Exception e) {
            showError("Failed to list policies: " + e.getMessage());
        }

        // Remove a policy
        waitForEnter("Press Enter to remove the storage policy (check cloud console before proceeding)...");
        showInfo("Removing storage policy...");
        try {
            removePolicy("roles/storage.admin");
            showSuccess("Successfully removed storage policy");
        } catch (Exception e) {
            showError("Failed to remove policy: " + e.getMessage());
        }

        waitForEnter("Press Enter to verify policy removal (check cloud console to confirm removal)...");
        // Verify policy removal by listing policies again
        showInfo("Verifying policy removal...");
        try {
            List<String> remainingPolicies = listAttachedPolicies();
            showSuccess("Policy removal verified - " + remainingPolicies.size() + " policies remaining");
            remainingPolicies.forEach(policy -> System.out.println("   - " + policy));
        } catch (Exception e) {
            showError("Failed to verify policy removal: " + e.getMessage());
        }

        waitForEnter("Press Enter to continue to error handling examples...");
    }

    /**
     * Demonstrate error handling scenarios.
     */
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

    /**
     * Initialize the IAM client with appropriate configuration.
     */
    private IamClient initializeClient() {
        return IamClient.builder(provider)
                .withRegion(REGION)
                .build();
    }

    /**
     * Create a new identity with trust configuration.
     */
    private String createIdentity(String identityName) throws Exception {
        try (IamClient iamClient = initializeClient()) {
            TrustConfiguration trustConfig = TrustConfiguration.builder()
                    .addTrustedPrincipal(SERVICE_ACCOUNT)
                    .build();

            CreateOptions options = CreateOptions.builder().build();

            return iamClient.createIdentity(
                    identityName,
                    "Demo IAM Identity for testing",
                    TENANT_ID,
                    REGION,
                    Optional.of(trustConfig),
                    Optional.of(options)
            );
        }
    }

    /**
     * Retrieve identity details.
     */
    private String getIdentity(String identityName) throws Exception {
        try (IamClient iamClient = initializeClient()) {
            return iamClient.getIdentity(identityName, TENANT_ID, REGION);
        }
    }

    /**
     * Delete an identity.
     */
    private void deleteIdentity(String identityName) throws Exception {
        try (IamClient iamClient = initializeClient()) {
            iamClient.deleteIdentity(identityName, TENANT_ID, REGION);
        }
    }

    /**
     * Attach a storage policy using a single comprehensive GCP IAM role.
     * Using roles/storage.admin which provides full storage permissions.
     */
    private void attachStoragePolicy() throws Exception {
        try (IamClient iamClient = initializeClient()) {
            // Create a policy document using a single comprehensive GCP IAM role
            PolicyDocument policyDocument = PolicyDocument.builder()
                    .version("2024-01-01")
                    .statement(Statement.builder()
                            .sid("StorageFullAccess")
                            .effect("Allow")
                            .action("roles/storage.admin")
                            .resource("storage://demo-bucket/*")
                            .build())
                    .build();

            iamClient.attachInlinePolicy(
                    policyDocument,
                    TENANT_ID,
                    REGION,
                    SERVICE_ACCOUNT
            );
        }
    }

    /**
     * Get details of a specific inline policy.
     */
    private String getPolicyDetails() throws Exception {
        try (IamClient iamClient = initializeClient()) {
            GetInlinePolicyDetailsRequest request = GetInlinePolicyDetailsRequest.builder()
                    .identityName(SERVICE_ACCOUNT)
                    .policyName("storage-policy")
                    .roleName("roles/storage.objectViewer")
                    .tenantId(TENANT_ID)
                    .region(REGION)
                    .build();

            return iamClient.getInlinePolicyDetails(request);
        }
    }

    /**
     * List all policies attached to an identity.
     */
    private List<String> listAttachedPolicies() throws Exception {
        try (IamClient iamClient = initializeClient()) {
            GetAttachedPoliciesRequest request = GetAttachedPoliciesRequest.builder()
                    .identityName(SERVICE_ACCOUNT)
                    .tenantId(TENANT_ID)
                    .region(REGION)
                    .build();

            return iamClient.getAttachedPolicies(request);
        }
    }

    /**
     * Remove a policy from an identity.
     */
    private void removePolicy(String policyName) throws Exception {
        try (IamClient iamClient = initializeClient()) {
            iamClient.removePolicy(
                    SERVICE_ACCOUNT,
                    policyName,
                    TENANT_ID,
                    REGION
            );
        }
    }
}