package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.AttachInlinePolicyRequest;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIamIT {
	public interface Harness extends AutoCloseable {
		AbstractIam createIamDriver();

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

        /**
         * Role name for getInlinePolicyDetails (and similar) when the API requires it.
         * Unused for AWS (identity is used); required for GCP (e.g. "roles/storage.objectViewer").
         */
        String getTestRoleName();

        /**
         * Resource ARN for the test inline policy statement (e.g. S3 bucket ARN for AWS).
         * Return null or blank if the provider does not require a resource in the statement.
         */
        default String getTestPolicyResource() {
            return null;
        }
    }

	protected abstract Harness createHarness();

	private Harness harness;
    private AbstractIam iam;
    private IamClient iamClient;

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
        iam = harness.createIamDriver();
        iamClient = new IamClient(iam);
	}

	/**
	 * Cleans up the test environment after each test.
	 */
	@AfterEach
	public void cleanupTestEnvironment() throws Exception {
		TestsUtil.stopWireMockRecording();
        if (iamClient != null) {
            iamClient.close(); // closes underlying AbstractIam
        }
	}

	@Test
	public void testAttachInlinePolicy() {
        Statement.StatementBuilder statementBuilder = Statement.builder()
				.effect(harness.getTestPolicyEffect());
		if (StringUtils.isNotBlank(harness.getTestPolicyResource())) {
			statementBuilder.resource(harness.getTestPolicyResource());
		}
		for (String action : harness.getTestPolicyActions()) {
			statementBuilder.action(action);
		}

		PolicyDocument policyDocument = PolicyDocument.builder()
                .name(harness.getTestPolicyName())
				.version(harness.getPolicyVersion())
				.statement(statementBuilder.build())
				.build();

		iamClient.attachInlinePolicy(
                AttachInlinePolicyRequest.builder()
                        .policyDocument(policyDocument)
                        .tenantId(harness.getTenantId())
                        .region(harness.getRegion())
                        .identityName(harness.getIdentityName())
                        .build());
	}

	@Test
	public void testGetInlinePolicyDetails() {
        Statement.StatementBuilder statementBuilder = Statement.builder()
                .effect(harness.getTestPolicyEffect());
        if (StringUtils.isNotBlank(harness.getTestPolicyResource())) {
            statementBuilder.resource(harness.getTestPolicyResource());
        }
        for (String action : harness.getTestPolicyActions()) {
            statementBuilder.action(action);
        }
        PolicyDocument policyDocument = PolicyDocument.builder()
                .name(harness.getTestPolicyName())
				.version(harness.getPolicyVersion())
                .statement(statementBuilder.build())
				.build();

		iamClient.attachInlinePolicy(
                AttachInlinePolicyRequest.builder()
                        .policyDocument(policyDocument)
                        .tenantId(harness.getTenantId())
                        .region(harness.getRegion())
                        .identityName(harness.getIdentityName())
                        .build());

		String policyDetails = iamClient.getInlinePolicyDetails(
				GetInlinePolicyDetailsRequest.builder()
						.identityName(harness.getIdentityName())
                .policyName(harness.getTestPolicyName())
                .roleName(harness.getTestRoleName())
						.tenantId(harness.getTenantId())
						.region(harness.getRegion())
						.build()
		);
		Assertions.assertNotNull(policyDetails, "Policy details shouldn't be null");
		Assertions.assertFalse(policyDetails.trim().isEmpty(), "Policy details shouldn't be empty");
	}

	@Test
	public void testGetAttachedPolicies() {
        Statement.StatementBuilder statementBuilder = Statement.builder()
				.effect(harness.getTestPolicyEffect());
		if (StringUtils.isNotBlank(harness.getTestPolicyResource())) {
			statementBuilder.resource(harness.getTestPolicyResource());
		}
		for (String action : harness.getTestPolicyActions()) {
			statementBuilder.action(action);
		}

		PolicyDocument policyDocument = PolicyDocument.builder()
                .name(harness.getTestPolicyName())
				.version(harness.getPolicyVersion())
				.statement(statementBuilder.build())
				.build();

		iamClient.attachInlinePolicy(
                AttachInlinePolicyRequest.builder()
                        .policyDocument(policyDocument)
                        .tenantId(harness.getTenantId())
                        .region(harness.getRegion())
                        .identityName(harness.getIdentityName())
                        .build());

		List<String> attachedPolicies = iamClient.getAttachedPolicies(
				GetAttachedPoliciesRequest.builder()
                .roleName(harness.getTestRoleName())
						.identityName(harness.getIdentityName())
						.tenantId(harness.getTenantId())
						.region(harness.getRegion())
						.build()
		);
		Assertions.assertNotNull(attachedPolicies, "Attached policies list shouldn't be null");
		Assertions.assertFalse(attachedPolicies.isEmpty(), "Attached policies list shouldn't be empty");
	}

	@Test
	public void testRemovePolicy() {
        Statement.StatementBuilder statementBuilder = Statement.builder()
                .effect(harness.getTestPolicyEffect());
        if (StringUtils.isNotBlank(harness.getTestPolicyResource())) {
            statementBuilder.resource(harness.getTestPolicyResource());
        }
        for (String action : harness.getTestPolicyActions()) {
            statementBuilder.action(action);
        }
        PolicyDocument policyDocument = PolicyDocument.builder()
                .name(harness.getTestPolicyName())
				.version(harness.getPolicyVersion())
                .statement(statementBuilder.build())
				.build();

		iamClient.attachInlinePolicy(
                AttachInlinePolicyRequest.builder()
                        .policyDocument(policyDocument)
                        .tenantId(harness.getTenantId())
                        .region(harness.getRegion())
                        .identityName(harness.getIdentityName())
                        .build());

        iamClient.removePolicy(
                harness.getIdentityName(),
                harness.getTestPolicyName(),
                harness.getTenantId(),
                harness.getRegion()
        );
    }

    private void cleanUpIdentity(String identity) {
        try {
            iamClient.deleteIdentity(
                    identity,
                    harness.getTenantId(),
                    harness.getRegion()
            );
        } catch (Exception e) {
            // Ignore
        }
    }

    /**
     * Tests creating an identity without trust configuration.
     */
    @Test
    public void testCreateIdentityWithoutTrustConfig() {
        String identityName = harness.getTestIdentityName();
        String identityId = iamClient.createIdentity(
                identityName,
                "Test identity for MultiCloudJ integration tests",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );

        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty");

        cleanUpIdentity(identityName);
    }

    /**
     * Tests creating an identity with trust configuration.
     */
    @Test
    public void testCreateIdentityWithTrustConfig() {
        String identityName = harness.getTestIdentityName() + "Trusted";
        TrustConfiguration trustConfig = TrustConfiguration.builder()
                .addTrustedPrincipal(harness.getTrustedPrincipal())
                .build();

        String identityId = iamClient.createIdentity(
                identityName,
                "Test identity with trust configuration",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.of(trustConfig),
                Optional.empty()
        );

        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty");

        cleanUpIdentity(identityName);
    }

    /**
     * Tests creating an identity with CreateOptions.
     */
    @Test
    public void testCreateIdentityWithOptions() {
        String identityName = harness.getTestIdentityName() + "Options";
        CreateOptions options = CreateOptions.builder().build();

        String identityId = iamClient.createIdentity(
                identityName,
                "Test identity with options",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.of(options)
        );

        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty");

        cleanUpIdentity(identityName);
    }

    /**
     * Tests creating an identity with null description.
     */
    @Test
    public void testCreateIdentityWithNullDescription() {
        String identityName = harness.getTestIdentityName() + "NoDesc";

        String identityId = iamClient.createIdentity(
                identityName,
                null,
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );

        Assertions.assertNotNull(identityId, "Identity ID should not be null");
        Assertions.assertFalse(identityId.isEmpty(), "Identity ID should not be empty");

        cleanUpIdentity(identityName);
    }

    /**
     * Tests getting an identity by name.
     */
    @Test
    public void testGetIdentity() throws InterruptedException {
        String identityName = harness.getTestIdentityName() + "Get";
        // First create an identity
        String identityId = iamClient.createIdentity(
                identityName,
                "Test identity for get operation",
                harness.getTenantId(),
                harness.getRegion(),
                Optional.empty(),
                Optional.empty()
        );

        // sleep for 500ms
        Thread.sleep(1000);

        // Then retrieve it
        String retrievedIdentity = iamClient.getIdentity(
                harness.getTestIdentityName() + "Get",
                harness.getTenantId(),
                harness.getRegion()
        );

        Assertions.assertNotNull(retrievedIdentity, "Retrieved identity should not be null");
        Assertions.assertFalse(retrievedIdentity.isEmpty(), "Retrieved identity should not be empty");

        cleanUpIdentity(identityName);
    }

    /**
     * Tests that the provider ID is correctly set.
     */
    @Test
    public void testProviderId() {
        Assertions.assertNotNull(iam.getProviderId(), "Provider ID should not be null");
        Assertions.assertEquals(harness.getProviderId(), iam.getProviderId(),
                "Provider ID should match the expected value");
    }

    /**
     * Tests exception mapping for provider-specific exceptions.
     */
    @Test
    public void testExceptionMapping() {
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
        // First create an identity
        String identityId = iamClient.createIdentity(
                harness.getTestIdentityName() + "Delete",
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
                        harness.getTestIdentityName() + "Delete",
                        harness.getTenantId(),
                        harness.getRegion()
                )
        );
    }

    /**
     * Tests the complete lifecycle: create, get, and delete an identity.
     */
    @Test
    public void testIdentityLifecycle() throws InterruptedException {
        String testIdentityName = harness.getTestIdentityName() + "LifeCycle";

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
        Thread.sleep(1000);

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
