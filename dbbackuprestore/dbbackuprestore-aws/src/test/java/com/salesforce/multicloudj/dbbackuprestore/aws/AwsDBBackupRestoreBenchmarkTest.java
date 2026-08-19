package com.salesforce.multicloudj.dbbackuprestore.aws;

import com.salesforce.multicloudj.common.util.UUID;
import com.salesforce.multicloudj.dbbackuprestore.client.AbstractDBBackupRestoreBenchmarkTest;
import com.salesforce.multicloudj.dbbackuprestore.client.DBBackupRestoreClient;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.backup.BackupClient;

/**
 * Hits the real AWS Backup endpoint directly, so it requires live AWS credentials via the default
 * provider chain (OS env / profile) and network access.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsDBBackupRestoreBenchmarkTest extends AbstractDBBackupRestoreBenchmarkTest {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "aws";
  }

  public static class HarnessImpl implements Harness {

    private final String region = requireEnv("DBBACKUPRESTORE_BENCHMARK_AWS_REGION");
    private final String tableArn = requireEnv("DBBACKUPRESTORE_BENCHMARK_AWS_TABLE_ARN");
    private final String roleId = requireEnv("DBBACKUPRESTORE_BENCHMARK_AWS_ROLE_ID");
    private BackupClient backupClient;

    @Override
    public DBBackupRestoreClient createClient() {
      // Credentials flow via the AWS default provider chain (OS env / profile), never -D.
      backupClient = BackupClient.builder().region(Region.of(region)).build();

      AwsDBBackupRestore dbBackupRestore =
          new AwsDBBackupRestore.Builder()
              .withBackupClient(backupClient)
              .withRegion(region)
              .withResourceName(tableArn)
              .build();

      return new DBBackupRestoreClient(dbBackupRestore);
    }

    @Override
    public String getTargetResource() {
      return "restored-table-benchmark-" + UUID.uniqueString();
    }

    @Override
    public String getRoleId() {
      return roleId;
    }

    @Override
    public void close() {
      if (backupClient != null) {
        backupClient.close();
      }
    }
  }
}
