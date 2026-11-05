package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.QueueMeta;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Alibaba SMQ (MNS) queue conformance harness: runs the shared {@link AbstractPubsubIT} suite
 * against live MNS in RECORD mode via the shared forward-proxy recipe. The MNS client targets the
 * real account-scoped endpoint over plain HTTP and routes every request through WireMock's HTTP
 * forward proxy (base port + 1); WireMock records by proxying to the real MNS endpoint. MNS request
 * signing covers only the method, headers, and resource path (not the scheme or host), so plain
 * HTTP is signature-safe.
 *
 * <p>Shared setup (endpoint, scrubber, drain net, subscription plumbing) lives in
 * {@link AbstractAliPubsubIT}; this class adds only the per-test queue provisioning and the
 * queue-backed topic driver.
 *
 * <p>Runs in replay by default (no credentials) against the committed WireMock mappings; pass
 * {@code -Drecord} with session credentials on the dedicated Ali machine to regenerate them:
 * {@code mvn test -pl pubsub/pubsub-ali -Dtest=AliPubsubQueueIT -Drecord}.
 */
public class AliPubsubQueueIT extends AbstractAliPubsubIT {

  private static final String BASE_QUEUE_NAME = "test-smq-conf-q";

  private HarnessImpl harnessImpl;
  private String queueName;

  @Override
  protected Harness createHarness() {
    harnessImpl = new HarnessImpl();
    return harnessImpl;
  }

  @BeforeEach
  public void setupTestResources(TestInfo testInfo) {
    String testMethodName =
        testInfo
            .getTestMethod()
            .map(m -> m.getName())
            .orElseThrow(() -> new IllegalStateException("Test method not found in TestInfo"));
    // Each test gets its own queue so it never picks up another test's messages.
    queueName = BASE_QUEUE_NAME + "-" + testMethodName;
    if (harnessImpl != null) {
      harnessImpl.setQueueName(queueName);
    }
  }

  public static class HarnessImpl extends AbstractHarness {
    private String queueName = BASE_QUEUE_NAME;

    public void setQueueName(String queueName) {
      this.queueName = queueName;
    }

    // Idempotently ensure the per-test queue exists (via the provisioning client). In record mode,
    // clear any settled leftovers from a prior -Drecord run BEFORE the test publishes, so a stale
    // message can never be captured into this run's recording. Gated on the same record signal as
    // the static-init fail-fast; in replay it is a complete no-op (no drain HTTP), so the committed
    // mappings replay green with no re-record.
    private void ensureQueueExists() {
      QueueMeta meta = new QueueMeta();
      meta.setQueueName(queueName);
      try {
        provisioningClient().createQueue(meta);
      } catch (ServiceException e) {
        if (!QUEUE_ALREADY_EXISTS.equals(e.getErrorCode())) {
          throw e;
        }
      }
      if (System.getProperty("record") != null) {
        drainQueue(queueName);
      }
    }

    @Override
    protected void ensureResources() {
      ensureQueueExists();
    }

    @Override
    protected String subscriptionQueueName() {
      return queueName;
    }

    @Override
    public AbstractTopic createTopicDriver() {
      ensureQueueExists();
      // Own client per driver (self-built from endpoint+creds, not injected) so topic.close() only
      // shuts down this test's client, never the shared provisioning client.
      AliSmqQueue.Builder builder = new AliSmqQueue.Builder();
      builder.withEndpoint(accountEndpoint());
      builder.withProxyEndpoint(proxyEndpoint());
      builder.withCredentialsOverrider(sessionOverriderFromEnv());
      builder.withTopicName(queueName);
      return builder.build();
    }

    @Override
    public String getProviderId() {
      return AliSmqQueue.PROVIDER_ID;
    }
  }
}
