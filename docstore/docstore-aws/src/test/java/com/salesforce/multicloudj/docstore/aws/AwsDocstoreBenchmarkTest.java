package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.client.AbstractDocstoreBenchmarkTest;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
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
    private static final String endpoint = "https://dynamodb.us-east-2.amazonaws.com";
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
        SdkHttpClient httpClient;
        DynamoDbClient client;

        @Override
        public AbstractDocStore createDocStore() {
            logger.info("Creating AWS docstore with endpoint: {}, table: {}, region: {}", 
                    endpoint, singleKeyTableName, region);
            
            try {
                URI endpointUri;
                try {
                    endpointUri = URI.create(endpoint);
                    logger.debug("Successfully parsed endpoint URI: {}", endpointUri);
                } catch (IllegalArgumentException e) {
                    logger.error("Invalid endpoint URI: {}", endpoint, e);
                    throw new RuntimeException("Failed to parse endpoint URI: " + endpoint, e);
                }

                logger.debug("Building DynamoDB client with region: {}", Region.US_EAST_2);
                DynamoDbClientBuilder builder = DynamoDbClient.builder()
                        .region(Region.US_EAST_2)
                        .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
                                System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                                System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                                System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))))
                        .endpointOverride(endpointUri);

                client = builder.build();
                logger.info("Successfully created DynamoDB client");

                
                CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                        .withTableName(singleKeyTableName)
                        .withPartitionKey(partitionKey)
                        .withAllowScans(true)
                        .build();

                logger.debug("Building AwsDocStore with table: {}, partition key: {}", singleKeyTableName, partitionKey);
                AbstractDocStore docStore = new AwsDocStore().builder()
                        .withDDBClient(client)
                        .withCollectionOptions(collectionOptions)
                        .build();

                logger.info("Successfully created AWS docstore");
                return docStore;
                
            } catch (Exception e) {
                logger.error("Failed to create AWS docstore", e);
                
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
                    throw new RuntimeException("Failed to create AWS docstore", e);
                }
            }
        }

        @Override
        public AbstractDocStore createQueryDocStore() {
            logger.info("Creating AWS query docstore with endpoint: {}, table: {}, region: {}", 
                    endpoint, compositeKeyTableName, region);
            
            try {
                URI endpointUri = URI.create(endpoint);
                
                DynamoDbClientBuilder builder = DynamoDbClient.builder()
                        .region(Region.US_EAST_2)
                        .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
                                System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                                System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                                System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))))
                        .endpointOverride(endpointUri);

                DynamoDbClient queryClient = builder.build();
                logger.info("Successfully created DynamoDB client for queries");

                // Configure collection options for composite key table
                CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                        .withTableName(compositeKeyTableName)
                        .withPartitionKey(queryPartitionKey)
                        .withSortKey(querySortKey)
                        .withAllowScans(true)
                        .build();

                logger.debug("Building AwsDocStore with table: {}, partition key: {}, sort key: {}", 
                        compositeKeyTableName, queryPartitionKey, querySortKey);
                AbstractDocStore docStore = new AwsDocStore().builder()
                        .withDDBClient(queryClient)
                        .withCollectionOptions(collectionOptions)
                        .build();

                logger.info("Successfully created AWS query docstore");
                return docStore;
                
            } catch (Exception e) {
                logger.error("Failed to create AWS query docstore", e);
                throw new RuntimeException("Failed to create AWS query docstore", e);
            }
        }

        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }
} 