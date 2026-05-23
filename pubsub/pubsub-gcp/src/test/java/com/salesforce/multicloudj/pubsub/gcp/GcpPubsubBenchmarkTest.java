package com.salesforce.multicloudj.pubsub.gcp;

import com.salesforce.multicloudj.pubsub.client.AbstractPubsubBenchmarkTest;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import org.junit.jupiter.api.TestInstance;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCP Pub/Sub JMH benchmark.
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code PUBSUB_BENCHMARK_GCP_TOPIC_NAME} — full topic name,
 *       e.g. {@code projects/my-project/topics/my-topic}</li>
 *   <li>{@code PUBSUB_BENCHMARK_GCP_SUBSCRIPTION_NAME} — full subscription name,
 *       e.g. {@code projects/my-project/subscriptions/my-sub}</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpPubsubBenchmarkTest extends AbstractPubsubBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(GcpPubsubBenchmarkTest.class);

  @Override
  protected Harness createHarness() {
    return new HarnessImpl(
        requireEnv("PUBSUB_BENCHMARK_GCP_TOPIC_NAME"),
        requireEnv("PUBSUB_BENCHMARK_GCP_SUBSCRIPTION_NAME"));
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
   * GCP NACK-and-redelivery benchmark.
   *
   * <p>Receives a message, NACKs it, then receives the redelivered copy before acking. Measures
   * NACK performance and redelivery latency — a GCP-specific flow.
   */
  @Benchmark
  @Threads(2)
  public void benchmarkNackAndRedelivery(Blackhole bh) {
    try {
      topicClient.send(createMessage(SMALL_MESSAGE));
      Message msg = subscriptionClient.receive();
      bh.consume(msg);
      subscriptionClient.sendNack(msg.getAckID());
      Message redelivered = subscriptionClient.receive();
      bh.consume(redelivered);
      subscriptionClient.sendAck(redelivered.getAckID());
    } catch (Exception e) {
      logger.error(">>> Benchmark NACK FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark NACK failed", e);
    }
  }

  static class HarnessImpl implements Harness {
    private final String topicName;
    private final String subscriptionName;

    HarnessImpl(String topicName, String subscriptionName) {
      this.topicName = topicName;
      this.subscriptionName = subscriptionName;
    }

    @Override
    public AbstractTopic<?> createTopic() {
      logger.info("Creating GCP Pub/Sub topic: {}", topicName);
      try {
        return new GcpTopic.Builder().withTopicName(topicName).build();
      } catch (Exception e) {
        logger.error("Failed to create GCP Pub/Sub topic", e);
        throw new RuntimeException("Failed to create GCP Pub/Sub topic", e);
      }
    }

    @Override
    public AbstractSubscription<?> createSubscription() {
      logger.info("Creating GCP Pub/Sub subscription: {}", subscriptionName);
      try {
        return new GcpSubscription.Builder().withSubscriptionName(subscriptionName).build();
      } catch (Exception e) {
        logger.error("Failed to create GCP Pub/Sub subscription", e);
        throw new RuntimeException("Failed to create GCP Pub/Sub subscription", e);
      }
    }

    @Override
    public String getTopicName() {
      return topicName;
    }

    @Override
    public String getSubscriptionName() {
      return subscriptionName;
    }

    @Override
    public void close() {
      // GCP clients are closed within the Topic/Subscription implementations
    }
  }
}
