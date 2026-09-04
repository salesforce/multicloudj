package com.salesforce.multicloudj.sts.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

import com.google.api.client.http.HttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.salesforce.multicloudj.common.circuitbreaker.CircuitBreakerConfig;
import com.salesforce.multicloudj.common.circuitbreaker.CircuitBreakerExecutor;
import com.salesforce.multicloudj.common.exceptions.CircuitBreakerOpenException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.gcp.GcpSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Fault-injection integration test for the STS circuit breaker on GCP. It drives a real
 * {@link StsClient} — the layer that owns the breaker — against a hand-written WireMock stub that
 * always returns a retryable HTTP 503 from the GCP STS token-exchange endpoint, so the breaker's
 * open transition is observed end-to-end without touching a live GCP endpoint.
 *
 * <p>GCP's token endpoint URL is fixed in the driver, so the request is redirected by injecting a
 * transport that proxies through WireMock (the same trust-all forward proxy the conformance suite
 * uses); a stub then intercepts the proxied {@code POST /v1/token}. The breaker-carrying client is
 * assembled through the package-visible {@link StsClient} constructor (this test deliberately lives
 * in the {@code sts.client} package) because the public builder cannot inject a test transport.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpStsCircuitBreakerIT {

  private static final int PORT = ThreadLocalRandom.current().nextInt(20000, 40000);
  private static final int HTTP_PORT = PORT + 1;

  private static final String WEB_IDENTITY_ROLE =
      "//iam.googleapis.com/projects/test-project/locations/global"
          + "/workloadIdentityPools/test-pool/providers/test-provider";

  @BeforeAll
  public void setUp() {
    TestsUtil.startWireMockServer("src/test/resources", PORT);
    configureFor(TestsUtil.WIREMOCK_HOST, HTTP_PORT);
    // The token-exchange endpoint always responds 503 — a transient failure the breaker must count.
    stubFor(
        post(urlPathEqualTo("/v1/token"))
            .willReturn(
                aResponse()
                    .withStatus(503)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"error\":\"unavailable\"}")));
  }

  @AfterAll
  public void tearDown() {
    TestsUtil.stopWireMockServer();
  }

  private static StsClient breakerClient() {
    HttpTransport transport = TestsUtilGcp.getHttpTransport(PORT);
    HttpTransportFactory factory = () -> transport;
    GcpSts gcpSts =
        new GcpSts().builder().build(MockGoogleCredentialsFactory.createMockCredentials(), factory);

    CircuitBreakerConfig config =
        CircuitBreakerConfig.builder()
            .withFailureRateThreshold(50f)
            .withMinimumNumberOfCalls(3)
            .withSlidingWindowSize(60)
            .withSlowCallRateThreshold(100f) // never trip on slowness, only on failures
            .withWaitDurationInOpenState(Duration.ofSeconds(30))
            .withPermittedNumberOfCallsInHalfOpenState(1)
            .build();

    return new StsClient(gcpSts, new CircuitBreakerExecutor("sts-it", config));
  }

  @Test
  public void breakerOpensAfterRepeatedRetryableFailures() {
    StsClient client = breakerClient();

    int retryableFailures = 0;
    boolean breakerOpened = false;

    for (int i = 0; i < 10 && !breakerOpened; i++) {
      // A distinct subject token per call keeps every request a cache miss that hits the endpoint.
      AssumeRoleWebIdentityRequest request =
          AssumeRoleWebIdentityRequest.builder()
              .role(WEB_IDENTITY_ROLE)
              .webIdentityToken("oidc-token-" + i)
              .sessionName("fault-injection")
              .build();
      try {
        client.getAssumeRoleWithWebIdentityCredentials(request);
        Assertions.fail("Expected the stubbed 503 to surface as an exception");
      } catch (CircuitBreakerOpenException open) {
        breakerOpened = true;
        Assertions.assertFalse(
            open.isRetryable(), "CircuitBreakerOpenException must be non-retryable");
      } catch (SubstrateSdkException e) {
        // The provider failure that feeds the breaker must itself be retryable, or it would never
        // count toward opening.
        Assertions.assertTrue(
            e.isRetryable(), "A 503 from the token endpoint must map to a retryable exception");
        retryableFailures++;
      }
    }

    Assertions.assertTrue(
        retryableFailures >= 3, "expected at least the minimum number of recorded failures");
    Assertions.assertTrue(breakerOpened, "circuit breaker never opened after repeated 503s");
  }
}
