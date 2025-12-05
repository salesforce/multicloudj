package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

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
}
