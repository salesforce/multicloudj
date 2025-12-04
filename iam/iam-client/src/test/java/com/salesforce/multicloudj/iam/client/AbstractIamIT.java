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
		
		PolicyDocument policyDocument = PolicyDocument.builder()
				.version("2012-10-17")
				.statement(Statement.builder()
						.effect("Allow")
						.action("roles/storage.objectViewer")
						.action("roles/storage.objectCreator")
						.build())
				.build();

		// This should not throw an exception
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
		
		// First attach a policy
		PolicyDocument policyDocument = PolicyDocument.builder()
				.version("2012-10-17")
				.statement(Statement.builder()
						.effect("Allow")
						.action("roles/storage.objectViewer")
						.build())
				.build();

		iamClient.attachInlinePolicy(
				policyDocument,
				harness.getTenantId(),
				harness.getRegion(),
				harness.getIdentityName()
		);

		// Then retrieve it
		String policyDetails = iamClient.getInlinePolicyDetails(
				harness.getIdentityName(),
				"roles/storage.objectViewer", // policy name (role name in GCP)
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
		
		// First attach a policy
		PolicyDocument policyDocument = PolicyDocument.builder()
				.version("2012-10-17")
				.statement(Statement.builder()
						.effect("Allow")
						.action("roles/storage.objectViewer")
						.action("roles/storage.objectCreator")
						.build())
				.build();

		iamClient.attachInlinePolicy(
				policyDocument,
				harness.getTenantId(),
				harness.getRegion(),
				harness.getIdentityName()
		);

		// Then retrieve the list of attached policies
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
		
		// First attach a policy
		PolicyDocument policyDocument = PolicyDocument.builder()
				.version("2012-10-17")
				.statement(Statement.builder()
						.effect("Allow")
						.action("roles/storage.objectViewer")
						.build())
				.build();

		iamClient.attachInlinePolicy(
				policyDocument,
				harness.getTenantId(),
				harness.getRegion(),
				harness.getIdentityName()
		);

		// Then remove it - should not throw an exception
		iamClient.removePolicy(
				harness.getIdentityName(),
				"roles/storage.objectViewer", // policy name is the GCP role
				harness.getTenantId(),
				harness.getRegion()
		);
	}
}
