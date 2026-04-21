package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.pubsub.client.AbstractPubsubBenchmarkTest;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsSnsPubsubBenchmarkTest extends AbstractPubsubBenchmarkTest {

  private static final Logger logger =
      LoggerFactory.getLogger(AwsSnsPubsubBenchmarkTest.class);

  // AWS Resources in ap-south-1 (Mumbai, India)
  private static final String SNS_TOPIC_ARN =
      "arn:aws:sns:ap-south-1:654654370895:multicloudj-pubsub-benchmark-topic";
  private static final String SQS_QUEUE_NAME = "multicloudj-pubsub-benchmark-queue";
  private static final String REGION = "ap-south-1";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "aws-sns";
  }

  public static class HarnessImpl implements Harness {
    SnsClient snsClient;
    SqsClient sqsClient;

    @Override
    public AbstractTopic<?> createTopic() {
      logger.info("Creating AWS SNS topic with ARN: {}", SNS_TOPIC_ARN);

      try {
        snsClient = SnsClient.builder().region(Region.of(REGION)).build();

        AwsSnsTopic.Builder builder = new AwsSnsTopic.Builder();
        builder.withTopicName(SNS_TOPIC_ARN).withRegion(REGION);

        return builder.build();

      } catch (Exception e) {
        logger.error("Failed to create AWS SNS topic", e);
        throw new RuntimeException("Failed to create AWS SNS topic", e);
      }
    }

    @Override
    public AbstractSubscription<?> createSubscription() {
      logger.info("Creating AWS SQS subscription with queue name: {}", SQS_QUEUE_NAME);

      try {
        sqsClient = SqsClient.builder().region(Region.of(REGION)).build();

        AwsSubscription.Builder builder = new AwsSubscription.Builder();
        builder.withSubscriptionName(SQS_QUEUE_NAME).withRegion(REGION);

        return builder.build();

      } catch (Exception e) {
        logger.error("Failed to create AWS SQS subscription", e);
        throw new RuntimeException("Failed to create AWS SQS subscription", e);
      }
    }

    @Override
    public String getTopicName() {
      return SNS_TOPIC_ARN;
    }

    @Override
    public String getSubscriptionName() {
      return SQS_QUEUE_NAME;
    }

    @Override
    public void close() throws Exception {
      if (snsClient != null) {
        snsClient.close();
      }
      if (sqsClient != null) {
        sqsClient.close();
      }
    }
  }
}
