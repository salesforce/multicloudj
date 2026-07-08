package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.docstore.client.AbstractDocstoreIT;
import com.salesforce.multicloudj.docstore.client.CollectionKind;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Disabled;

@Disabled
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
    int port = ThreadLocalRandom.current().nextInt(2000, 20000);
    SyncClient client;

    @Override
    public AbstractDocStore createDocstoreDriver(CollectionKind kind) {
      ClientConfiguration configuration = new ClientConfiguration();
      configuration.setProxyHost(TestsUtil.WIREMOCK_HOST);
      configuration.setProxyPort(port + 1);
      // The Tablestore SDK validates the HMAC signature of every response against the
      // access-key secret (enabled by default). In replay mode the recorded responses were
      // signed with the real secret while the client uses dummy credentials, so validation
      // fails with "cannot find signature". Disable response validation for the test harness;
      // it only checks client-side response integrity and is meaningless against WireMock stubs.
      configuration.setEnableResponseValidation(false);

      // In record mode the TABLESTORE_* env vars carry real credentials. In replay mode
      // (no creds) fall back to dummy values so the SyncClient still constructs -- otherwise
      // the Tablestore SDK's client-side credential validation throws InvalidCredentialsException
      // before any request reaches WireMock. Mirrors the blob-ali IT harness.
      String accessKeyId =
          System.getenv().getOrDefault("TABLESTORE_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
      String accessKeySecret =
          System.getenv().getOrDefault("TABLESTORE_ACCESS_KEY_SECRET", "FAKE_SECRET_ACCESS_KEY");
      String sessionToken =
          System.getenv().getOrDefault("TABLESTORE_SESSION_TOKEN", "FAKE_SESSION_TOKEN");

      client =
          new SyncClient(
              END_POINT,
              accessKeyId,
              accessKeySecret,
              INSTANCE_NAME,
              configuration,
              sessionToken);
      CollectionOptions collectionOptions = null;
      if (kind == CollectionKind.SINGLE_KEY) {
        collectionOptions =
            new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("docstore_test_1")
                .withPartitionKey("pName")
                .build();
      } else if (kind == CollectionKind.TWO_KEYS) {
        collectionOptions =
            new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("docstore_test_2")
                .withPartitionKey("Game")
                .withSortKey("Player")
                .withAllowScans(true)
                .build();
      }
      return new AliDocStore()
          .builder()
          .withTableStoreClient(client)
          .withCollectionOptions(collectionOptions)
          .build();
    }

    @Override
    public Object getRevisionId() {
      return "123";
    }

    @Override
    public String getDocstoreEndpoint() {
      return END_POINT;
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
