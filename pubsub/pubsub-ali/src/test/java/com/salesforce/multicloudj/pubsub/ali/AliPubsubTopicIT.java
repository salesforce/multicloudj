package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.QueueMeta;
import com.aliyun.mns.model.SubscriptionMeta;
import com.aliyun.mns.model.TopicMeta;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Alibaba SMQ (MNS) topic conformance harness: runs the shared {@link AbstractPubsubIT} suite over
 * an SMQ topic and its fan-out subscription queue via the shared forward-proxy recipe. A publisher
 * sends to an SMQ topic, the topic fans out to a JSON-format queue subscription, and the consumer
 * reads from that queue — so the messages arrive as the topic-delivery JSON envelope and
 * {@link AliSubscription}'s dual-mode sniff/unwrap runs against the received bytes. Because an SMQ
 * topic and its fan-out subscription queue share one MNS endpoint, the IT publishes to the topic
 * and receives the fan-out-delivered (envelope-unwrapped) message through a single WireMock
 * forward proxy. It does not verify the received payload end to end — the shared conformance suite
 * currently asserts only that the received body and ack id are non-null.
 *
 * <p>Shared setup (endpoint, scrubber, drain net, subscription plumbing) lives in
 * {@link AbstractAliPubsubIT}; this class adds only the per-test provisioning of a topic +
 * fan-out queue + JSON subscription and the {@link AliSmqTopic} publish driver.
 *
 * <p>Runs in replay by default (no credentials) against the committed WireMock mappings; pass
 * {@code -Drecord} with session credentials on the dedicated Ali machine to regenerate them:
 * {@code mvn test -pl pubsub/pubsub-ali -Dtest=AliPubsubTopicIT -Drecord}.
 */
public class AliPubsubTopicIT extends AbstractAliPubsubIT {

  // Per-test resource prefixes: topic, fan-out queue, and the topic→queue subscription binding.
  private static final String TOPIC_PREFIX = "test-smq-conf-t";
  private static final String QUEUE_PREFIX = "test-smq-conf-tq";
  private static final String SUBSCRIPTION_PREFIX = "test-smq-conf-ts";

  private static final String TOPIC_ALREADY_EXISTS = "TopicAlreadyExist";
  private static final String SUBSCRIPTION_ALREADY_EXISTS = "SubscriptionAlreadyExist";

  private HarnessImpl harnessImpl;

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
    // Each test gets its own topic, fan-out queue, and subscription so it never sees another
    // test's messages.
    if (harnessImpl != null) {
      harnessImpl.setNames(
          TOPIC_PREFIX + "-" + testMethodName,
          QUEUE_PREFIX + "-" + testMethodName,
          SUBSCRIPTION_PREFIX + "-" + testMethodName);
    }
  }

  public static class HarnessImpl extends AbstractHarness {
    private String topicName = TOPIC_PREFIX;
    private String queueName = QUEUE_PREFIX;
    private String subscriptionName = SUBSCRIPTION_PREFIX;

    public void setNames(String topicName, String queueName, String subscriptionName) {
      this.topicName = topicName;
      this.queueName = queueName;
      this.subscriptionName = subscriptionName;
    }

    // Idempotently provision the topic, its fan-out queue, and the JSON topic→queue subscription
    // (via the provisioning client). Tolerates the *AlreadyExist* codes so re-runs are idempotent.
    private void ensureInfra() {
      MNSClient client = provisioningClient();

      QueueMeta queueMeta = new QueueMeta();
      queueMeta.setQueueName(queueName);
      createIfAbsent(() -> client.createQueue(queueMeta), QUEUE_ALREADY_EXISTS);

      TopicMeta topicMeta = new TopicMeta();
      topicMeta.setTopicName(topicName);
      createIfAbsent(() -> client.createTopic(topicMeta), TOPIC_ALREADY_EXISTS);

      SubscriptionMeta subscriptionMeta = new SubscriptionMeta();
      subscriptionMeta.setSubscriptionName(subscriptionName);
      subscriptionMeta.setEndpoint(queueResourceEndpoint());
      subscriptionMeta.setNotifyContentFormat(SubscriptionMeta.NotifyContentFormat.JSON);
      createIfAbsent(
          () -> client.getTopicRef(topicName).subscribe(subscriptionMeta),
          SUBSCRIPTION_ALREADY_EXISTS);

      // Record mode only: clear the fan-out subscription queue's settled prior-run leftovers BEFORE
      // the test publishes, so a stale topic delivery can never be captured into this run's
      // recording. Gated on the same record signal as the static-init fail-fast; in replay it is a
      // complete no-op (no drain HTTP), so the committed mappings replay green with no re-record.
      if (System.getProperty("record") != null) {
        drainQueue(queueName);
      }
    }

    // Runs a provisioning action, tolerating only the given already-exists code so re-runs are
    // idempotent; any other ServiceException propagates.
    private void createIfAbsent(Runnable action, String alreadyExistsCode) {
      try {
        action.run();
      } catch (ServiceException e) {
        if (!alreadyExistsCode.equals(e.getErrorCode())) {
          throw e;
        }
      }
    }

    // The fan-out queue's MNS resource endpoint for the subscription binding:
    // acs:mns:<region>:<accountId>:queues/<queueName>. Account id and region are derived from the
    // configured endpoint host (<accountId>.mns.<region>.aliyuncs.com), so they come from the
    // runtime env endpoint, not hardcoded source.
    private String queueResourceEndpoint() {
      String host = URI.create(ENDPOINT).getHost();
      String[] labels = host == null ? new String[0] : host.split("\\.");
      if (labels.length < 4) {
        throw new IllegalStateException(
            "Cannot derive MNS account/region from endpoint host: " + host);
      }
      String accountId = labels[0];
      String region = labels[2];
      return "acs:mns:" + region + ":" + accountId + ":queues/" + queueName;
    }

    @Override
    protected void ensureResources() {
      ensureInfra();
    }

    @Override
    protected String subscriptionQueueName() {
      return queueName;
    }

    @Override
    public AbstractTopic createTopicDriver() {
      ensureInfra();
      // Own client per driver (self-built from endpoint+creds, not injected) so topic.close() only
      // shuts down this test's client, never the shared provisioning client.
      AliSmqTopic.Builder builder = new AliSmqTopic.Builder();
      builder.withEndpoint(accountEndpoint());
      builder.withProxyEndpoint(proxyEndpoint());
      builder.withCredentialsOverrider(sessionOverriderFromEnv());
      builder.withTopicName(topicName);
      return builder.build();
    }

    @Override
    public String getProviderId() {
      return AliSmqTopic.PROVIDER_ID;
    }
  }
}
