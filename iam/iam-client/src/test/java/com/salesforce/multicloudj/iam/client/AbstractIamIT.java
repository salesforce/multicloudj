package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

/**
 * Abstract base class for IAM integration tests across different cloud providers.
 * 
 * <p>This class defines the conformance test suite that all IAM provider implementations
 * must pass. It uses WireMock for recording and replaying HTTP interactions with cloud
 * provider IAM APIs.
 * 
 * <p>Provider-specific test classes should extend this class and implement the
 * {@link #createHarness()} method to provide provider-specific configuration.
 * 
 * <p>To record new test interactions, run with -Drecord system property.
 * Otherwise, tests will replay from recorded mappings in src/test/resources.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIamIT {
    
    /**
     * Harness interface that provider-specific test implementations must provide.
     * This interface encapsulates all provider-specific configuration and behavior.
     */
    public interface Harness extends AutoCloseable {
        /**
         * Creates an IAM driver instance for testing.
         * 
         * @return configured AbstractIam driver
         */
        AbstractIam createIamDriver();
        
        /**
         * Creates an IamClient instance for testing.
         * 
         * @return configured IamClient
         */
        IamClient createIamClient();
        
        /**
         * Gets the test project/account/tenant ID.
         * 
         * @return the tenant ID (AWS Account ID, GCP Project ID, or AliCloud Account ID)
         */
        String getTenantId();
        
        /**
         * Gets the test region.
         * 
         * @return the region for IAM operations
         */
        String getRegion();
        
        /**
         * Gets a test identity name for creating service accounts/roles.
         * 
         * @return the identity name
         */
        String getTestIdentityName();
        
        /**
         * Gets the IAM endpoint URL for the provider.
         * 
         * @return the IAM endpoint URL
         */
        String getIamEndpoint();
        
        /**
         * Gets the provider ID.
         * 
         * @return the provider ID (e.g., "aws", "gcp", "ali")
         */
        String getProviderId();
        
        /**
         * Gets the WireMock server port.
         * WireMock server needs the https port. If we make it constant at abstract class,
         * we won't be able to run tests in parallel. Each provider can provide a
         * randomly selected port number.
         * 
         * @return the port number
         */
        int getPort();
        
        /**
         * Gets WireMock extensions if needed.
         * Provide the fully qualified class names here.
         * 
         * @return list of WireMock extension class names
         */
        List<String> getWiremockExtensions();
        
        /**
         * Indicates whether the provider supports creating identities.
         * 
         * @return true if createIdentity is supported
         */
        boolean supportsCreateIdentity();
        
        /**
         * Indicates whether the provider supports creating identities with trust configuration.
         * 
         * @return true if createIdentity with trust config is supported
         */
        boolean supportsCreateIdentityWithTrustConfig();
        
        /**
         * Indicates whether the provider supports getting identity details.
         * 
         * @return true if getIdentity is supported
         */
        boolean supportsGetIdentity();
        
        /**
         * Indicates whether the provider supports deleting identities.
         * 
         * @return true if deleteIdentity is supported
         */
        boolean supportsDeleteIdentity();
        
        /**
         * Indicates whether the provider supports attaching inline policies.
         * 
         * @return true if attachInlinePolicy is supported
         */
        boolean supportsAttachInlinePolicy();
        
        /**
         * Indicates whether the provider supports getting inline policy details.
         * 
         * @return true if getInlinePolicyDetails is supported
         */
        boolean supportsGetInlinePolicyDetails();
        
        /**
         * Indicates whether the provider supports listing attached policies.
         * 
         * @return true if getAttachedPolicies is supported
         */
        boolean supportsGetAttachedPolicies();
        
        /**
         * Indicates whether the provider supports removing policies.
         * 
         * @return true if removePolicy is supported
         */
        boolean supportsRemovePolicy();
        
        /**
         * Gets a trusted principal for trust configuration tests.
         * 
         * @return a trusted principal identifier
         */
        String getTrustedPrincipal();
    }
    
    protected abstract Harness createHarness();
    
    private Harness harness;
    
    /**
     * Initializes the WireMock server before all tests.
     */
    @BeforeAll
    public void initializeWireMockServer() {
        harness = createHarness();
        TestsUtil.startWireMockServer(
                "src/test/resources", 
                harness.getPort(), 
                harness.getWiremockExtensions().toArray(new String[0]));
    }
    
    /**
     * Shuts down the WireMock server after all tests.
     */
    @AfterAll
    public void shutdownWireMockServer() throws Exception {
        TestsUtil.stopWireMockServer();
        harness.close();
    }
    
    /**
     * Initialize the harness and start WireMock recording.
     */
    @BeforeEach
    public void setupTestEnvironment() {
        TestsUtil.startWireMockRecording(harness.getIamEndpoint());
    }
    
    /**
     * Cleans up the test environment after each test.
     */
    @AfterEach
    public void cleanupTestEnvironment() {
        TestsUtil.stopWireMockRecording();
    }
    
    /**
     * Tests creating an identity without trust configuration.
     */
    @Test
    public void testCreateIdentityWithoutTrustConfig() {
        Assumptions.assumeTrue(harness.supportsCreateIdentity(), 
                "Skipping test as harness does not support createIdentity");
        
        IamClient iamClient = harness.createIamClient();
        
        String identityId = iamClient.createIdentity(
                harness.getTestIdentityName(),
                "Test identity for MultiCloudJ integration tests",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );
        
        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty");
    }
    
    /**
     * Tests creating an identity with trust configuration.
     */
    @Test
    public void testCreateIdentityWithTrustConfig() {
        Assumptions.assumeTrue(harness.supportsCreateIdentityWithTrustConfig(), 
                "Skipping test as harness does not support createIdentity with trust config");
        
        IamClient iamClient = harness.createIamClient();
        
        TrustConfiguration trustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal(harness.getTrustedPrincipal())
                .build();
        
        String identityId = iamClient.createIdentity(
                harness.getTestIdentityName() + "-trusted",
                "Test identity with trust configuration",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.of(trustConfig),
                Optional.empty()
        );
        
        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty");
    }
    
    /**
     * Tests creating an identity with CreateOptions.
     */
    @Test
    public void testCreateIdentityWithOptions() {
        Assumptions.assumeTrue(harness.supportsCreateIdentity(), 
                "Skipping test as harness does not support createIdentity");
        
        IamClient iamClient = harness.createIamClient();
        
        CreateOptions options = CreateOptions.builder().build();
        
        String identityId = iamClient.createIdentity(
                harness.getTestIdentityName() + "-options",
                "Test identity with options",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.of(options)
        );
        
        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty");
    }
    
    /**
     * Tests creating an identity with null description.
     */
    @Test
    public void testCreateIdentityWithNullDescription() {
        Assumptions.assumeTrue(harness.supportsCreateIdentity(), 
                "Skipping test as harness does not support createIdentity");
        
        IamClient iamClient = harness.createIamClient();
        
        String identityId = iamClient.createIdentity(
                harness.getTestIdentityName() + "-nodesc",
                null,
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );
        
        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty");
    }
    
    /**
     * Tests getting an identity by name.
     */
    @Test
    public void testGetIdentity() {
        Assumptions.assumeTrue(harness.supportsGetIdentity(), 
                "Skipping test as harness does not support getIdentity");
        
        IamClient iamClient = harness.createIamClient();
        
        // First create an identity
        String identityId = iamClient.createIdentity(
                harness.getTestIdentityName() + "-get",
                "Test identity for get operation",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );
        
        // Then retrieve it
        String retrievedIdentity = iamClient.getIdentity(
                harness.getTestIdentityName() + "-get",
                harness.getTenantId(),
                harness.getRegion()
        );
        
        Assertions.assertNotNull(retrievedIdentity, "Retrieved identity should not be null");
        Assertions.assertFalse(retrievedIdentity.isEmpty(), "Retrieved identity should not be empty");
    }
    
    /**
     * Tests attaching an inline policy to an identity.
     */
    @Test
    public void testAttachInlinePolicy() {
        Assumptions.assumeTrue(harness.supportsAttachInlinePolicy(), 
                "Skipping test as harness does not support attachInlinePolicy");
        
        IamClient iamClient = harness.createIamClient();
        
        // Create a test policy
        PolicyDocument policy = PolicyDocument.builder()
                .version("2012-10-17")
                .statement(Statement.builder()
                        .sid("AllowStorageAccess")
                        .effect("Allow")
                        .action("storage:GetObject")
                        .resource("storage://test-bucket/*")
                        .build())
                .build();
        
        // Attach the policy
        iamClient.attachInlinePolicy(
                policy,
                harness.getTenantId(),
                harness.getRegion(),
                harness.getTestIdentityName()
        );
        
        // If we get here without exception, the test passed
        Assertions.assertTrue(true, "Policy attachment should succeed");
    }
    
    /**
     * Tests getting inline policy details.
     */
    @Test
    public void testGetInlinePolicyDetails() {
        Assumptions.assumeTrue(harness.supportsGetInlinePolicyDetails(), 
                "Skipping test as harness does not support getInlinePolicyDetails");
        
        IamClient iamClient = harness.createIamClient();
        
        String policyDetails = iamClient.getInlinePolicyDetails(
                harness.getTestIdentityName(),
                "test-policy",
                harness.getTenantId(),
                harness.getRegion()
        );
        
        Assertions.assertNotNull(policyDetails, "Policy details should not be null");
    }
    
    /**
     * Tests getting attached policies for an identity.
     */
    @Test
    public void testGetAttachedPolicies() {
        Assumptions.assumeTrue(harness.supportsGetAttachedPolicies(), 
                "Skipping test as harness does not support getAttachedPolicies");
        
        IamClient iamClient = harness.createIamClient();
        
        List<String> policies = iamClient.getAttachedPolicies(
                harness.getTestIdentityName(),
                harness.getTenantId(),
                harness.getRegion()
        );
        
        Assertions.assertNotNull(policies, "Policies list should not be null");
    }
    
    /**
     * Tests removing a policy from an identity.
     */
    @Test
    public void testRemovePolicy() {
        Assumptions.assumeTrue(harness.supportsRemovePolicy(), 
                "Skipping test as harness does not support removePolicy");
        
        IamClient iamClient = harness.createIamClient();
        
        iamClient.removePolicy(
                harness.getTestIdentityName(),
                "test-policy",
                harness.getTenantId(),
                harness.getRegion()
        );
        
        // If we get here without exception, the test passed
        Assertions.assertTrue(true, "Policy removal should succeed");
    }
    
    /**
     * Tests deleting an identity.
     */
    @Test
    public void testDeleteIdentity() {
        Assumptions.assumeTrue(harness.supportsDeleteIdentity(), 
                "Skipping test as harness does not support deleteIdentity");
        
        IamClient iamClient = harness.createIamClient();
        
        // First create an identity to delete
        iamClient.createIdentity(
                harness.getTestIdentityName() + "-delete",
                "Test identity for delete operation",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );
        
        // Then delete it
        iamClient.deleteIdentity(
                harness.getTestIdentityName() + "-delete",
                harness.getTenantId(),
                harness.getRegion()
        );
        
        // If we get here without exception, the test passed
        Assertions.assertTrue(true, "Identity deletion should succeed");
    }
    
    /**
     * Tests that the provider ID is correctly set.
     */
    @Test
    public void testProviderId() {
        AbstractIam iam = harness.createIamDriver();
        
        Assertions.assertNotNull(iam.getProviderId(), "Provider ID should not be null");
        Assertions.assertEquals(harness.getProviderId(), iam.getProviderId(), 
                "Provider ID should match the expected value");
    }
    
    /**
     * Tests exception mapping for provider-specific exceptions.
     */
    @Test
    public void testExceptionMapping() {
        AbstractIam iam = harness.createIamDriver();
        
        // Test with a generic exception
        Throwable genericException = new RuntimeException("Generic error");
        Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> exceptionClass = 
                iam.getException(genericException);
        
        Assertions.assertNotNull(exceptionClass, "Exception class should not be null");
        Assertions.assertEquals(
                com.salesforce.multicloudj.common.exceptions.UnknownException.class, 
                exceptionClass,
                "Generic exceptions should map to UnknownException"
        );
    }
}
