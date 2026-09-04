package com.salesforce.multicloudj.sts.aws;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

import com.salesforce.multicloudj.common.circuitbreaker.CircuitBreakerConfig;
import com.salesforce.multicloudj.common.exceptions.CircuitBreakerOpenException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.client.StsClient;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Fault-injection integration test for the STS circuit breaker on AWS. Unlike the record/replay
 * conformance suite ({@link AwsStsIT}), this drives a real {@link StsClient} — the layer that owns
 * the breaker — against a hand-written WireMock stub that always returns a retryable HTTP 503, so
 * the breaker's open transition is observed end-to-end without touching a live AWS endpoint.
 *
 * <p>The AWS SDK STS client is pointed straight at WireMock's plain-HTTP port via the client's
 * public {@code withEndpoint(...)} builder — no forward proxy or TLS interception is needed because
 * AWS honours an endpoint override directly. Dummy credentials are supplied through JVM system
 * properties so the default credential chain resolves and the request actually reaches the stub.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsStsCircuitBreakerIT {

  private static final int PORT = ThreadLocalRandom.current().nextInt(20000, 40000);
  private static final int HTTP_PORT = PORT + 1;

  // A well-formed STS error body so the AWS SDK surfaces a 503 AwsServiceException (retryable).
  private static final String SERVICE_UNAVAILABLE_BODY =
      "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
          + "<Error><Type>Server</Type><Code>ServiceUnavailable</Code>"
          + "<Message>slow down</Message></Error>"
          + "<RequestId>fault-injection</RequestId></ErrorResponse>";

  private String priorAccessKey;
  private String priorSecretKey;

  @BeforeAll
  public void setUp() {
    // Dummy credentials for the default provider chain — never leave the JVM as anything real.
    priorAccessKey = System.setProperty("aws.accessKeyId", "FAKE_ACCESS_KEY");
    priorSecretKey = System.setProperty("aws.secretAccessKey", "FAKE_SECRET_ACCESS_KEY");

    TestsUtil.startWireMockServer("src/test/resources", PORT);
    configureFor(TestsUtil.WIREMOCK_HOST, HTTP_PORT);
    // Every STS action (a POST to "/") fails with a transient 503.
    stubFor(
        any(anyUrl())
            .willReturn(
                aResponse()
                    .withStatus(503)
                    .withHeader("Content-Type", "text/xml")
                    .withBody(SERVICE_UNAVAILABLE_BODY)));
  }

  @AfterAll
  public void tearDown() {
    TestsUtil.stopWireMockServer();
    restoreProperty("aws.accessKeyId", priorAccessKey);
    restoreProperty("aws.secretAccessKey", priorSecretKey);
  }

  private static void restoreProperty(String key, String prior) {
    if (prior == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, prior);
    }
  }

  @Test
  public void breakerOpensAfterRepeatedRetryableFailures() {
    CircuitBreakerConfig config =
        CircuitBreakerConfig.builder()
            .withFailureRateThreshold(50f)
            .withMinimumNumberOfCalls(3)
            .withSlidingWindowSize(60)
            .withSlowCallRateThreshold(100f) // never trip on slowness, only on failures
            .withWaitDurationInOpenState(Duration.ofSeconds(30))
            .withPermittedNumberOfCallsInHalfOpenState(1)
            .build();

    StsClient client =
        StsClient.builder("aws")
            .withRegion("us-west-2")
            .withEndpoint(URI.create("http://" + TestsUtil.WIREMOCK_HOST + ":" + HTTP_PORT))
            .withCircuitBreakerConfig(config)
            .build();

    int retryableFailures = 0;
    boolean breakerOpened = false;

    // Once the minimum number of failing calls has been recorded, the next call must fast-fail.
    for (int i = 0; i < 10 && !breakerOpened; i++) {
      try {
        client.getCallerIdentity();
        Assertions.fail("Expected the stubbed 503 to surface as an exception");
      } catch (CircuitBreakerOpenException open) {
        breakerOpened = true;
        Assertions.assertFalse(
            open.isRetryable(), "CircuitBreakerOpenException must be non-retryable");
      } catch (SubstrateSdkException e) {
        // The provider failure that feeds the breaker must itself be retryable, or it would never
        // count toward opening.
        Assertions.assertTrue(
            e.isRetryable(), "A 503 ServiceUnavailable must map to a retryable exception");
        retryableFailures++;
      }
    }

    Assertions.assertTrue(
        retryableFailures >= 3, "expected at least the minimum number of recorded failures");
    Assertions.assertTrue(breakerOpened, "circuit breaker never opened after repeated 503s");
  }
}
