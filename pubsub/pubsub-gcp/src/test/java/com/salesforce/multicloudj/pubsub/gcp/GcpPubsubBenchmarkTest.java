package com.salesforce.multicloudj.pubsub.gcp;

import com.salesforce.multicloudj.pubsub.client.AbstractPubsubBenchmarkTest;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpPubsubBenchmarkTest extends AbstractPubsubBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(GcpPubsubBenchmarkTest.class);

  // GCP Project and resource names
  private static final String PROJECT_ID = "substrate-sdk-gcp-poc1";
  private static final String TOPIC_NAME =
      "projects/substrate-sdk-gcp-poc1/topics/multicloudj-pubsub-benchmark-topic";
  private static final String SUBSCRIPTION_NAME =
      "projects/substrate-sdk-gcp-poc1/subscriptions/multicloudj-pubsub-benchmark-subscription";

  // Instance flag for NACK benchmark pre-population
  private volatile boolean nackMessagesPrePopulated = false;

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "gcp";
  }

  @Override
  protected int getMaxBatchAckSize() {
    // GCP supports up to 1000 messages per batch ack (vs AWS limit of 10)
    return 1000;
  }

  /**
   * GCP-specific NACK and redelivery benchmark.
   *
   * Measures NACK performance and redelivery latency - GCP-specific feature.
   * Receives message, sends NACK, then receives again (redelivered message).
   */
  @org.openjdk.jmh.annotations.Benchmark
  @org.openjdk.jmh.annotations.Threads(2)
  public void benchmarkNackAndRedelivery(org.openjdk.jmh.infra.Blackhole bh) {
    // Pre-populate messages once per benchmark run
    ensurePrePopulated(
        () -> nackMessagesPrePopulated,
        () -> nackMessagesPrePopulated = true,
        "NACK",
        PREPOPULATE_SMALL,
        SMALL_MESSAGE);

    try {
      for (int i = 0; i < ITERATIONS_NACK_BENCHMARK; i++) {
        // Publish one to replenish
        com.salesforce.multicloudj.pubsub.driver.Message msgToPublish =
            createMessage(SMALL_MESSAGE);
        topicClient.send(msgToPublish);

        // Receive a message
        com.salesforce.multicloudj.pubsub.driver.Message msg = subscriptionClient.receive();
        bh.consume(msg);

        // NACK the message (GCP-specific)
        subscriptionClient.sendNack(msg.getAckID());

        // Receive the redelivered message
        com.salesforce.multicloudj.pubsub.driver.Message redeliveredMsg =
            subscriptionClient.receive();
        bh.consume(redeliveredMsg);

        // Ack the redelivered message
        subscriptionClient.sendAck(redeliveredMsg.getAckID());
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark NACK FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark NACK failed", e);
    }
  }

  public static class HarnessImpl implements Harness {

    @Override
    public AbstractTopic<?> createTopic() {
      logger.info("Creating GCP Pub/Sub topic: {}", TOPIC_NAME);

      try {
        GcpTopic.Builder builder = new GcpTopic.Builder();
        builder.withTopicName(TOPIC_NAME);

        return builder.build();

      } catch (Exception e) {
        logger.error("Failed to create GCP Pub/Sub topic", e);
        throw new RuntimeException("Failed to create GCP Pub/Sub topic", e);
      }
    }

    @Override
    public AbstractSubscription<?> createSubscription() {
      logger.info("Creating GCP Pub/Sub subscription: {}", SUBSCRIPTION_NAME);

      try {
        GcpSubscription.Builder builder = new GcpSubscription.Builder();
        builder.withSubscriptionName(SUBSCRIPTION_NAME);

        return builder.build();

      } catch (Exception e) {
        logger.error("Failed to create GCP Pub/Sub subscription", e);
        throw new RuntimeException("Failed to create GCP Pub/Sub subscription", e);
      }
    }

    @Override
    public String getTopicName() {
      return TOPIC_NAME;
    }

    @Override
    public String getSubscriptionName() {
      return SUBSCRIPTION_NAME;
    }

    @Override
    public void close() throws Exception {
      // GCP clients are closed within the Topic/Subscription implementations
    }
  }
}
