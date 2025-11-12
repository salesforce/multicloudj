package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIamIT {
	public interface Harness extends AutoCloseable {
		AbstractIam createIamDriver(boolean useValidCredentials);

		String getIdentityName();

		String getTenantId();

		String getRegion();

		String getProviderId();

		int getPort();

		List<String> getWiremockExtensions();

		String getIamEndpoint();

        String getTrustedPrincipal();

        String getTestIdentityName();

		default String getPolicyVersion() {
			return "";
		}

		String getTestPolicyEffect();

		List<String> getTestPolicyActions();

        String getTestPolicyName();
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
				"src/test/resources", harness.getPort(), harness.getWiremockExtensions().toArray(new String[0]));
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
	 * Initialize the harness and
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

	@Test
	public void testAttachInlinePolicy() {
        AbstractIam iam = harness.createIamDriver(true);
		IamClient iamClient = new IamClient(iam);

		Statement.StatementBuilder statementBuilder = Statement.builder()
				.effect(harness.getTestPolicyEffect());
		for (String action : harness.getTestPolicyActions()) {
			statementBuilder.action(action);
		}

		PolicyDocument policyDocument = PolicyDocument.builder()
				.version(harness.getPolicyVersion())
				.statement(statementBuilder.build())
				.build();

		iamClient.attachInlinePolicy(
				policyDocument,
				harness.getTenantId(),
				harness.getRegion(),
				harness.getIdentityName()
		);
	}

	@Test
	public void testGetInlinePolicyDetails() {
        AbstractIam iam = harness.createIamDriver(true);
		IamClient iamClient = new IamClient(iam);

		PolicyDocument policyDocument = PolicyDocument.builder()
				.version(harness.getPolicyVersion())
				.statement(Statement.builder()
						.effect(harness.getTestPolicyEffect())
						.action(harness.getTestPolicyName())
						.build())
				.build();

		iamClient.attachInlinePolicy(
				policyDocument,
				harness.getTenantId(),
				harness.getRegion(),
				harness.getIdentityName()
		);

		String policyDetails = iamClient.getInlinePolicyDetails(
				harness.getIdentityName(),
				harness.getTestPolicyName(),
				harness.getTenantId(),
				harness.getRegion()
		);
		Assertions.assertNotNull(policyDetails, "Policy details shouldn't be null");
		Assertions.assertFalse(policyDetails.trim().isEmpty(), "Policy details shouldn't be empty");
	}

	@Test
	public void testGetAttachedPolicies() {
        AbstractIam iam = harness.createIamDriver(true);
		IamClient iamClient = new IamClient(iam);

		Statement.StatementBuilder statementBuilder = Statement.builder()
				.effect(harness.getTestPolicyEffect());
		for (String action : harness.getTestPolicyActions()) {
			statementBuilder.action(action);
		}

		PolicyDocument policyDocument = PolicyDocument.builder()
				.version(harness.getPolicyVersion())
				.statement(statementBuilder.build())
				.build();

		iamClient.attachInlinePolicy(
				policyDocument,
				harness.getTenantId(),
				harness.getRegion(),
				harness.getIdentityName()
		);

		List<String> attachedPolicies = iamClient.getAttachedPolicies(
				harness.getIdentityName(),
				harness.getTenantId(),
				harness.getRegion()
		);
		Assertions.assertNotNull(attachedPolicies, "Attached policies list shouldn't be null");
		Assertions.assertFalse(attachedPolicies.isEmpty(), "Attached policies list shouldn't be empty");
	}

	@Test
	public void testRemovePolicy() {
        AbstractIam iam = harness.createIamDriver(true);
		IamClient iamClient = new IamClient(iam);

		PolicyDocument policyDocument = PolicyDocument.builder()
				.version(harness.getPolicyVersion())
				.statement(Statement.builder()
						.effect(harness.getTestPolicyEffect())
						.action(harness.getTestPolicyName())
						.build())
				.build();

		iamClient.attachInlinePolicy(
				policyDocument,
				harness.getTenantId(),
				harness.getRegion(),
				harness.getIdentityName()
		);

        iamClient.removePolicy(
                harness.getIdentityName(),
                harness.getTestPolicyName(),
                harness.getTenantId(),
                harness.getRegion()
        );
    }

    /**
     * Tests creating an identity without trust configuration.
     */
    @Test
    public void testCreateIdentityWithoutTrustConfig() {
        AbstractIam iam = harness.createIamDriver(true);
        IamClient iamClient = new IamClient(iam);

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
        AbstractIam iam = harness.createIamDriver(true);
        IamClient iamClient = new IamClient(iam);

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
        AbstractIam iam = harness.createIamDriver(true);
        IamClient iamClient = new IamClient(iam);

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
        AbstractIam iam = harness.createIamDriver(true);
        IamClient iamClient = new IamClient(iam);

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
        AbstractIam iam = harness.createIamDriver(true);
        IamClient iamClient = new IamClient(iam);

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
        AbstractIam iam = harness.createIamDriver(true);

        Assertions.assertNotNull(iam.getProviderId(), "Provider ID should not be null");
        Assertions.assertEquals(harness.getProviderId(), iam.getProviderId(),
                "Provider ID should match the expected value");
    }

    /**
     * Tests exception mapping for provider-specific exceptions.
     */
    @Test
    public void testExceptionMapping() {
        AbstractIam iam = harness.createIamDriver(true);

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
        AbstractIam iam = harness.createIamDriver(true);
        IamClient iamClient = new IamClient(iam);

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
        AbstractIam iam = harness.createIamDriver(true);
        IamClient iamClient = new IamClient(iam);

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
