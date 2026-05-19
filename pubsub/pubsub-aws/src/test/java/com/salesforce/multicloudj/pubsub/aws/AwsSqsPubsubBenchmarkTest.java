package com.salesforce.multicloudj.pubsub.aws;

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
 * AWS SQS-direct JMH benchmark (no SNS fan-out).
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code PUBSUB_BENCHMARK_AWS_SQS_QUEUE_NAME} — SQS queue name (not URL)</li>
 *   <li>{@code PUBSUB_BENCHMARK_AWS_SQS_REGION} — AWS region, e.g. {@code ap-south-1}</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsSqsPubsubBenchmarkTest extends AbstractPubsubBenchmarkTest {

  private static final Logger logger =
      LoggerFactory.getLogger(AwsSqsPubsubBenchmarkTest.class);

  @Override
  protected Harness createHarness() {
    return new HarnessImpl(
        requireEnv("PUBSUB_BENCHMARK_AWS_SQS_QUEUE_NAME"),
        requireEnv("PUBSUB_BENCHMARK_AWS_SQS_REGION"));
  }

  @Override
  protected String getProviderId() {
    return "awssqs";
  }

  /**
   * AWS NACK-and-redelivery benchmark for the SQS-direct path.
   *
   * <p>Receives a message, NACKs it (visibility timeout = 0), then receives the redelivery before
   * acking. Validates the NACK path and measures redelivery latency.
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
    private final String sqsQueueName;
    private final String sqsRegion;

    HarnessImpl(String sqsQueueName, String sqsRegion) {
      this.sqsQueueName = sqsQueueName;
      this.sqsRegion = sqsRegion;
    }

    @Override
    public AbstractTopic<?> createTopic() {
      logger.info("Creating AWS SQS topic with queue name: {}", sqsQueueName);
      try {
        return new AwsSqsTopic.Builder().withTopicName(sqsQueueName).withRegion(sqsRegion).build();
      } catch (Exception e) {
        logger.error("Failed to create AWS SQS topic", e);
        throw new RuntimeException("Failed to create AWS SQS topic", e);
      }
    }

    @Override
    public AbstractSubscription<?> createSubscription() {
      logger.info("Creating AWS SQS subscription with queue name: {}", sqsQueueName);
      try {
        return new AwsSubscription.Builder()
            .withSubscriptionName(sqsQueueName)
            .withRegion(sqsRegion)
            .build();
      } catch (Exception e) {
        logger.error("Failed to create AWS SQS subscription", e);
        throw new RuntimeException("Failed to create AWS SQS subscription", e);
      }
    }

    @Override
    public String getTopicName() {
      return sqsQueueName;
    }

    @Override
    public String getSubscriptionName() {
      return sqsQueueName;
    }

    @Override
    public void close() {
      // AWS clients are closed within the Topic/Subscription implementations
    }
  }
}
