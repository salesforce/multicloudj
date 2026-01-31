package com.salesforce.multicloudj.dbbackrestore.ali;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.salesforce.multicloudj.dbbackrestore.client.AbstractDBBackRestoreIT;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Disabled;

/**
 * Integration tests for Alibaba Cloud DB Backup Restore implementation.
 * Currently disabled as Alibaba Cloud backup operations have limited support.
 *
 * @since 0.2.26
 */
@Disabled
public class AliDBBackRestoreIT extends AbstractDBBackRestoreIT {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    IAcsClient hbrClient;
    int port = ThreadLocalRandom.current().nextInt(1000, 10000);

    @Override
    public AbstractDBBackRestore createDBBackRestoreDriver() {
      // Create HBR client using environment variables
      String accessKeyId = System.getenv().getOrDefault(
          "TABLESTORE_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
      String accessKeySecret = System.getenv().getOrDefault(
          "TABLESTORE_ACCESS_KEY_SECRET", "FAKE_ACCESS_KEY_SECRET");
      String region = "cn-shanghai";

      DefaultProfile profile = DefaultProfile.getProfile(region, accessKeyId, accessKeySecret);
      hbrClient = new DefaultAcsClient(profile);

      return new AliDBBackRestore.Builder()
          .withHbrClient(hbrClient)
          .withRegion(region)
          .withCollectionName("docstore-test-1")
          .withAccessKeyId(accessKeyId)
          .withAccessKeySecret(accessKeySecret)
          .build();
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
    public boolean supportsBackupRestore() {
      // Alibaba Cloud TableStore backup via HBR has limited support
      // Most operations throw UnSupportedOperationException
      return false;
    }

    @Override
    public Map<String, String> getRestoreOptions() {
      return Collections.emptyMap();
    }

    @Override
    public void close() {
      // HBR client doesn't need explicit closing
    }
  }
}
