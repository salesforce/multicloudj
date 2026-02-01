package com.salesforce.multicloudj.dbbackrestore.ali;

import com.aliyun.hbr20170908.Client;
import com.aliyun.teaopenapi.models.Config;
import com.salesforce.multicloudj.dbbackrestore.client.AbstractDBBackRestoreIT;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Disabled;

/**
 * Integration tests for Alibaba Cloud DB Backup Restore implementation.
 * These tests use WireMock to record/replay HTTP interactions with Alibaba Cloud HBR service.
 *
 * <p>To run in record mode with actual Alibaba Cloud:
 * <pre>
 * mvn test -pl dbbackrestore/dbbackrestore-ali -Dtest=AliDBBackRestoreIT -Drecord \
 *   -DTABLESTORE_ACCESS_KEY_ID=&lt;your-key&gt; \
 *   -DTABLESTORE_ACCESS_KEY_SECRET=&lt;your-secret&gt;
 * </pre>
 *
 * @since 0.2.25
 */
@Disabled("Enable this test when you have Alibaba Cloud HBR credentials and want to record interactions")
public class AliDBBackRestoreIT extends AbstractDBBackRestoreIT {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    private Client hbrClient;
    private final int port = ThreadLocalRandom.current().nextInt(1000, 10000);

    @Override
    public AbstractDBBackRestore createDBBackRestoreDriver() {
      String accessKeyId = System.getenv().getOrDefault(
          "TABLESTORE_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
      String accessKeySecret = System.getenv().getOrDefault(
          "TABLESTORE_ACCESS_KEY_SECRET", "FAKE_ACCESS_KEY_SECRET");
      String region = "cn-shanghai";

      try {
        Config config = new Config();
        config.setAccessKeyId(accessKeyId);
        config.setAccessKeySecret(accessKeySecret);
        config.setEndpoint("hbr." + region + ".aliyuncs.com");
        config.setRegionId(region);
        
        hbrClient = new Client(config);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create HBR client", e);
      }

      return new AliDBBackRestore.Builder()
          .withHbrClient(hbrClient)
          .withRegion(region)
          .withResourceName("docstore-test-1")
          .withAccessKeyId(accessKeyId)
          .withAccessKeySecret(accessKeySecret)
          .build();
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public String getBackupEndpoint() {
      return "https://hbr.cn-shanghai.aliyuncs.com";
    }

    @Override
    public void close() throws Exception {
      // HBR client doesn't need explicit closing in this SDK version
    }
  }
}
