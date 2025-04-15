package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.core.auth.EnvironmentVariableCredentialsProvider;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.docstore.client.AbstractDocstoreIT;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;


public class AliDocstoreIT extends AbstractDocstoreIT {
    // Switch it to https after table store can support https proxy and
    // method to override the SSL context to trust all certs for wiremock test up.
    // for some reason wiremock over http to server over https doesn't work as expected.
    // either both connection should be http or https in order for wiremock setup to work.
    private static final String END_POINT = "http://chameleon-java.cn-shanghai.ots.aliyuncs.com";
    private static final String INSTANCE_NAME = "chameleon-java";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        SyncClient client;

        @Override
        public AbstractDocStore createDocstoreDriver() {
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.setProxyHost(TestsUtil.WIREMOCK_HOST);
            configuration.setProxyPort(port+1);

            client = new SyncClient(END_POINT, new EnvironmentVariableCredentialsProvider(), INSTANCE_NAME, configuration, null);

            return new AliDocStore().builder().withTableStoreClient(client).withCollectionOptions(
                    new CollectionOptions
                            .CollectionOptionsBuilder()
                            .withTableName("docstore_test_1")
                            .withPartitionKey("pName")
                            .build()
            ).build();
        }

        @Override
        public AbstractDocStore createDocstoreDriver2() {
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.setProxyHost(TestsUtil.WIREMOCK_HOST);
            configuration.setProxyPort(port+1);

            client = new SyncClient(END_POINT, new EnvironmentVariableCredentialsProvider(), INSTANCE_NAME, configuration, null);

            return new AliDocStore().builder().withTableStoreClient(client).withCollectionOptions(
                    new CollectionOptions
                            .CollectionOptionsBuilder()
                            .withTableName("docstore_test_2")
                            .withPartitionKey("Game")
                            .withSortKey("Player")
                            .withAllowScans(true)
                            .build()
            ).build();
        }

        @Override
        public String getDocstoreEndpoint() {
            return END_POINT;
        }

        @Override
        public String getProviderId() {
            return "ali";
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
            client.shutdown();
        }
    }
}
