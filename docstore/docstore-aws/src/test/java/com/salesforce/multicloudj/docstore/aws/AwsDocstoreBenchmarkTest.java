package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.client.AbstractDocstoreBenchmarkTest;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Disabled;

import java.net.URI;

@Disabled
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsDocstoreBenchmarkTest extends AbstractDocstoreBenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(AwsDocstoreBenchmarkTest.class);
    private static final String singleKeyTableName = "docstore-benchmark-test1";
    private static final String compositeKeyTableName = "docstore-benchmark-test2";
    private static final String region = "us-east-2";
    private static final String partitionKey = "pName";
    private static final String queryPartitionKey = "Game";
    private static final String querySortKey = "Player";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        DynamoDbClient client;

        @Override
        public AbstractDocStore createDocStore() {
            return createAwsDocStore(
                singleKeyTableName, 
                partitionKey, 
                null, // no sort key for single key table
                "single key"
            );
        }

        @Override
        public AbstractDocStore createQueryDocStore() {
            return createAwsDocStore(
                compositeKeyTableName,
                queryPartitionKey,
                querySortKey,
                "composite key query"
            );
        }

        /**
         * Helper method to create AWS docstore with specified configuration
         * 
         * @param tableName the DynamoDB table name
         * @param partitionKeyName the partition key field name
         * @param sortKeyName the sort key field name (null for single key tables)
         * @param storeType description for logging purposes
         * @return configured AbstractDocStore instance
         */
        private AbstractDocStore createAwsDocStore(String tableName, String partitionKeyName, 
                                                 String sortKeyName, String storeType) {
            logger.info("Creating AWS {} docstore with table: {}, region: {}", 
                    storeType, tableName, region);
            
            try {
                logger.debug("Building DynamoDB client with region: {}", Region.US_EAST_2);
                DynamoDbClientBuilder builder = DynamoDbClient.builder()
                        .region(Region.US_EAST_2)
                        .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
                                System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                                System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                                System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))));

                DynamoDbClient dynamoClient = builder.build();
                logger.info("Successfully created DynamoDB client for {}", storeType);

                // Store the client reference for cleanup (use the first one created)
                if (client == null) {
                    client = dynamoClient;
                }

                // Configure collection options
                CollectionOptions.CollectionOptionsBuilder optionsBuilder = new CollectionOptions.CollectionOptionsBuilder()
                        .withTableName(tableName)
                        .withPartitionKey(partitionKeyName)
                        .withAllowScans(true);

                // Add sort key if provided
                if (sortKeyName != null) {
                    optionsBuilder.withSortKey(sortKeyName);
                    logger.debug("Building AwsDocStore with table: {}, partition key: {}, sort key: {}", 
                            tableName, partitionKeyName, sortKeyName);
                } else {
                    logger.debug("Building AwsDocStore with table: {}, partition key: {}", 
                            tableName, partitionKeyName);
                }

                CollectionOptions collectionOptions = optionsBuilder.build();

                AbstractDocStore docStore = new AwsDocStore().builder()
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
                        logger.debug("Cleaned up DynamoDB client after failure");
                    } catch (Exception cleanupException) {
                        logger.warn("Failed to cleanup DynamoDB client after docstore creation failure", cleanupException);
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