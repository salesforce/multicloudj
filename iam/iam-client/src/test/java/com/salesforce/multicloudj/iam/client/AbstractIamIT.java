package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
    
    /**
     * Tests deleting an identity.
     */
    @Test
    public void testDeleteIdentity() {
        IamClient iamClient = harness.createIamClient();
        
        // First create an identity
        String identityId = iamClient.createIdentity(
                harness.getTestIdentityName() + "-delete",
                "Test identity for delete operation",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );
        
        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        
        // Then delete it - should not throw any exception
        Assertions.assertDoesNotThrow(() ->
                iamClient.deleteIdentity(
                        harness.getTestIdentityName() + "-delete",
                        harness.getTenantId(),
                        harness.getRegion()
                )
        );
    }
    
    /**
     * Tests the complete lifecycle: create, get, and delete an identity.
     */
    @Test
    public void testIdentityLifecycle() {
        IamClient iamClient = harness.createIamClient();
        
        String testIdentityName = harness.getTestIdentityName() + "-lifecycle";
        
        // Step 1: Create an identity
        String identityId = iamClient.createIdentity(
                testIdentityName,
                "Test identity for lifecycle test",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );
        
        Assertions.assertNotNull(identityId, "Identity ID should not be null after creation");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty after creation");
        
        // Step 2: Get the identity to verify it exists
        String retrievedIdentity = iamClient.getIdentity(
                testIdentityName,
                harness.getTenantId(),
                harness.getRegion()
        );
        
        Assertions.assertNotNull(retrievedIdentity, "Retrieved identity should not be null");
        Assertions.assertFalse(retrievedIdentity.isEmpty(), "Retrieved identity should not be empty");
        
        // Step 3: Delete the identity
        Assertions.assertDoesNotThrow(() ->
                iamClient.deleteIdentity(
                        testIdentityName,
                        harness.getTenantId(),
                        harness.getRegion()
                ),
                "Deleting identity should not throw an exception"
        );
    }
}
