package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.docstore.client.AbstractDocstoreIT;
import com.salesforce.multicloudj.docstore.client.CollectionKind;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class AwsDocstoreIT extends AbstractDocstoreIT {

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        SdkHttpClient httpClient;
        DynamoDbClient client;
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        @Override
        public AbstractDocStore createDocstoreDriver(CollectionKind kind) {
            httpClient = TestsUtilAws.getProxyClient("https", port);
            DynamoDbClientBuilder builder = DynamoDbClient.builder()
                    .httpClient(httpClient)
                    .region(Region.US_WEST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
                            System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                            System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                            System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))))
                    .endpointOverride(
                            URI.create("https://dynamodb.us-west-2.amazonaws.com"));

            client = builder.build();
            CollectionOptions collectionOptions = null;
            if (kind == CollectionKind.SINGLE_KEY) {
                collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                        .withTableName("docstore-test-1")
                        .withPartitionKey("pName")
                        .withAllowScans(true)
                        .build();
            } else if (kind == CollectionKind.TWO_KEYS) {
                collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                        .withTableName("docstore-test-2")
                        .withPartitionKey("Game")
                        .withSortKey("Player")
                        .withAllowScans(true)
                        .build();
            }

            return new AwsDocStore().builder()
                    .withDDBClient(client)
                    .withCollectionOptions(collectionOptions)
                    .build();
        }

        @Override
        public String getDocstoreEndpoint() {
            return "https://dynamodb.us-west-2.amazonaws.com";
        }

        @Override
        public String getProviderId() {
            return "aws";
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public List<String> getWiremockExtensions() {
            return List.of();
        }


        @Override
        public void close() {
            client.close();
            httpClient.close();
        }
    }
}
