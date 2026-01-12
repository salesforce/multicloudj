package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractStsIT {
    // Define the Harness interface
    public interface Harness extends AutoCloseable {
        // Method to create a sts driver
        AbstractSts createStsDriver(boolean longTermCredentials);

        // Methods to get various identifiers
        String getRoleName();

        // provide the STS endpoint in provider
        String getStsEndpoint();

        // provide the provider ID
        String getProviderId();

        // Wiremock server need the https port, if
        // we make it constant at abstract class, we won't be able
        // to run tests in parallel. Each provider can provide the
        // randomly selected port number.
        int getPort();

        // If you need to provide extensions to wiremock proxy
        // provide the fully qualified class names here.
        List<String> getWiremockExtensions();

        // TODO: supports getAccessToken
        // remove it when alibaba provides the get session token in cn-shanghai
        boolean supportsGetAccessToken();
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
        TestsUtil.startWireMockRecording(harness.getStsEndpoint());
    }

    /**
     * Cleans up the test environment after each test.
     */
    @AfterEach
    public void cleanupTestEnvironment() {
        TestsUtil.stopWireMockRecording();
    }

    @Test
    public void testGetCallerIdentity() {
        AbstractSts sts = harness.createStsDriver(false);
        StsClient stsClient = new StsClient(sts);
        CallerIdentity identity = stsClient.getCallerIdentity();
        Assertions.assertNotNull(identity, "Identity shouldn't be empty");
        Assertions.assertNotNull(identity.getAccountId());
        Assertions.assertNotNull(identity.getUserId());
        Assertions.assertNotNull(identity.getCloudResourceName());
    }

    @Test
    public void testGetAccessToken() {
        Assumptions.assumeTrue(harness.supportsGetAccessToken(), "Skipping test as harness does not support GetAccessToken");
        AbstractSts sts = harness.createStsDriver(true);
        StsClient stsClient = new StsClient(sts);
        StsCredentials credentials = stsClient.getAccessToken(GetAccessTokenRequest.newBuilder().build());
        Assertions.assertNotNull(credentials, "Credentials shouldn't be empty");
    }

    @Test
    public void testAssumeRole() {
        AbstractSts sts = harness.createStsDriver(false);
        StsClient stsClient = new StsClient(sts);
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withRole(harness.getRoleName()).withExpiration(3600).withSessionName("any-session").build();
        StsCredentials credentials = stsClient.getAssumeRoleCredentials(request);
        Assertions.assertNotNull(credentials, "Credentials shouldn't be empty");
        Assertions.assertNotNull(credentials.getAccessKeySecret());
        Assertions.assertNotNull(credentials.getAccessKeyId());
        Assertions.assertNotNull(credentials.getSecurityToken());
    }
}
