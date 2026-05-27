package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.client.AbstractDocstoreBenchmarkTest;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

@Disabled
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsDocstoreBenchmarkTest extends AbstractDocstoreBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(AwsDocstoreBenchmarkTest.class);

  @Override
  protected String getProviderId() {
    return "aws";
  }

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    DynamoDbClient client;

    @Override
    public AbstractDocStore createDocStore() {
      return createAwsDocStore(
          requireEnv("DOCSTORE_BENCHMARK_AWS_SINGLE_KEY_TABLE"),
          "pName",
          null,
          "single key");
    }

    @Override
    public AbstractDocStore createQueryDocStore() {
      return createAwsDocStore(
          requireEnv("DOCSTORE_BENCHMARK_AWS_COMPOSITE_KEY_TABLE"),
          "Game",
          "Player",
          "composite key query");
    }

    private AbstractDocStore createAwsDocStore(
        String tableName, String partitionKeyName, String sortKeyName, String storeType) {
      String region = requireEnv("DOCSTORE_BENCHMARK_AWS_REGION");
      logger.info(
          "Creating AWS {} docstore with table: {}, region: {}", storeType, tableName, region);

      try {
        DynamoDbClientBuilder builder =
            DynamoDbClient.builder()
                .region(Region.of(region))
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsSessionCredentials.create(
                            requireEnv("AWS_ACCESS_KEY_ID"),
                            requireEnv("AWS_SECRET_ACCESS_KEY"),
                            requireEnv("AWS_SESSION_TOKEN"))));

        DynamoDbClient dynamoClient = builder.build();

        if (client == null) {
          client = dynamoClient;
        }

        CollectionOptions.CollectionOptionsBuilder optionsBuilder =
            new CollectionOptions.CollectionOptionsBuilder()
                .withTableName(tableName)
                .withPartitionKey(partitionKeyName)
                .withAllowScans(true);

        if (sortKeyName != null) {
          optionsBuilder.withSortKey(sortKeyName);
        }

        CollectionOptions collectionOptions = optionsBuilder.build();

        AbstractDocStore docStore =
            new AwsDocStore()
                .builder()
                .withDDBClient(dynamoClient)
                .withCollectionOptions(collectionOptions)
                .build();

        logger.info("Successfully created AWS {} docstore", storeType);
        return docStore;

      } catch (Exception e) {
        logger.error("Failed to create AWS {} docstore", storeType, e);

        if (client != null) {
          try {
            client.close();
          } catch (Exception cleanupException) {
            logger.warn("Failed to cleanup DynamoDB client after failure", cleanupException);
          }
          client = null;
        }

        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new RuntimeException("Failed to create AWS " + storeType + " docstore", e);
        }
      }
    }

    @Override
    public void close() throws Exception {
      if (client != null) {
        client.close();
      }
    }
  }
}
