package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.driver.AbstractStsVerifier;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

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

    // Method to create a utilities driver that signs cloud native auth requests
    AbstractStsUtilities<?> createStsUtilitiesDriver();

    // Method to create a verifier driver whose outbound verification calls are routed to the
    // WireMock server
    AbstractStsVerifier createStsVerifierDriver();

    // Whether the sign -> verify round trip can be recorded and replayed through WireMock for this
    // provider.
    boolean supportsCloudNativeAuthReplay();
  }

  protected abstract Harness createHarness();

  private static final String CONFORMANCE_HEADER = "X-Conformance-Header";
  private static final String CONFORMANCE_HEADER_DIFFERENT_CASE = "x-CONFORMANCE-header";

  private Harness harness;

  /** Initializes the WireMock server before all tests. */
  @BeforeAll
  public void initializeWireMockServer() {
    harness = createHarness();
    TestsUtil.startWireMockServer(
        "src/test/resources",
        harness.getPort(),
        harness.getWiremockExtensions().toArray(new String[0]));
  }

  /** Shuts down the WireMock server after all tests. */
  @AfterAll
  public void shutdownWireMockServer() throws Exception {
    TestsUtil.stopWireMockServer();
    harness.close();
  }

  /** Initialize the harness and */
  @BeforeEach
  public void setupTestEnvironment(TestInfo testInfo) {
    String testClassName = testInfo.getTestClass().map(Class::getSimpleName).orElse("Unknown");
    String testMethodName =
        testInfo.getTestMethod().map(java.lang.reflect.Method::getName).orElse("unknown");
    TestsUtil.startWireMockRecording(harness.getStsEndpoint(), testClassName, testMethodName);
  }

  /** Cleans up the test environment after each test. */
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
    Assumptions.assumeTrue(
        harness.supportsGetAccessToken(),
        "Skipping test as harness does not support GetAccessToken");
    AbstractSts sts = harness.createStsDriver(true);
    StsClient stsClient = new StsClient(sts);
    StsCredentials credentials =
        stsClient.getAccessToken(GetAccessTokenRequest.newBuilder().build());
    Assertions.assertNotNull(credentials, "Credentials shouldn't be empty");
  }

  @Test
  public void testAssumeRole() {
    AbstractSts sts = harness.createStsDriver(false);
    StsClient stsClient = new StsClient(sts);
    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole(harness.getRoleName())
            .withExpiration(3600)
            .withSessionName("any-session")
            .build();
    StsCredentials credentials = stsClient.getAssumeRoleCredentials(request);
    Assertions.assertNotNull(credentials, "Credentials shouldn't be empty");
    Assertions.assertNotNull(credentials.getAccessKeySecret());
    Assertions.assertNotNull(credentials.getAccessKeyId());
    Assertions.assertNotNull(credentials.getSecurityToken());
  }

  private String signIdentityWithConformanceHeader() {
    return signIdentity(SignOptions.builder().withCustomHeader(CONFORMANCE_HEADER, "v1").build());
  }

  private String signIdentity(SignOptions options) {
    StsUtilities utilities = new StsUtilities(harness.createStsUtilitiesDriver());
    SignedAuthRequest signed = utilities.newCloudNativeAuthSignedRequest(null, options);
    Assertions.assertNotNull(signed.getSignedIdentity(), "Signed identity shouldn't be empty");
    return signed.getSignedIdentity();
  }

  // Exercises sign -> expected-header check -> replay; signature validity is covered by provider
  // unit tests. The expected name uses different casing than the signed one so header-name
  // normalization is part of the check.
  @Test
  public void testSignedIdentityWithMixedCaseCustomHeaderValidates() {
    Assumptions.assumeTrue(
        harness.supportsCloudNativeAuthReplay(),
        "Skipping test as harness does not support cloud native auth replay");
    String signedIdentity = signIdentityWithConformanceHeader();
    StsVerifier verifier = new StsVerifier(harness.createStsVerifierDriver());
    CallerIdentity identity =
        verifier.validateSignedAuthRequest(
            signedIdentity,
            ValidateOptions.builder()
                .withExpectedCustomHeader(CONFORMANCE_HEADER_DIFFERENT_CASE, "v1")
                .build());
    Assertions.assertNotNull(identity, "Identity shouldn't be empty");
    Assertions.assertNotNull(identity.getCloudResourceName());
    Assertions.assertNotNull(identity.getAccountId());
  }

  @Test
  public void testSignedIdentityWithWrongCustomHeaderValueIsRejected() {
    Assumptions.assumeTrue(
        harness.supportsCloudNativeAuthReplay(),
        "Skipping test as harness does not support cloud native auth replay");
    String signedIdentity = signIdentityWithConformanceHeader();
    StsVerifier verifier = new StsVerifier(harness.createStsVerifierDriver());
    ValidateOptions wrongValue =
        ValidateOptions.builder().withExpectedCustomHeader(CONFORMANCE_HEADER, "v2").build();
    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> verifier.validateSignedAuthRequest(signedIdentity, wrongValue));
  }

  @Test
  public void testSignedIdentityWithMissingCustomHeaderIsRejected() {
    Assumptions.assumeTrue(
        harness.supportsCloudNativeAuthReplay(),
        "Skipping test as harness does not support cloud native auth replay");
    String signedIdentity = signIdentity(SignOptions.builder().build());
    StsVerifier verifier = new StsVerifier(harness.createStsVerifierDriver());
    ValidateOptions expectsHeader =
        ValidateOptions.builder().withExpectedCustomHeader(CONFORMANCE_HEADER, "v1").build();
    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> verifier.validateSignedAuthRequest(signedIdentity, expectsHeader));
  }
}
