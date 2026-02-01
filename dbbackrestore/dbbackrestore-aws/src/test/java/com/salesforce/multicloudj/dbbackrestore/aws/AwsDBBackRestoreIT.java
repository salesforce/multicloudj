package com.salesforce.multicloudj.dbbackrestore.aws;

import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.dbbackrestore.client.AbstractDBBackRestoreIT;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.backup.BackupClient;

/**
 * Integration tests for AWS DB Backup Restore implementation.
 *
 * @since 0.2.26
 */
public class AwsDBBackRestoreIT extends AbstractDBBackRestoreIT {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    SdkHttpClient httpClient;
    BackupClient backupClient;
    int port = ThreadLocalRandom.current().nextInt(1000, 10000);

    @Override
    public AbstractDBBackRestore createDBBackRestoreDriver() {
      httpClient = TestsUtilAws.getProxyClient("https", port);
      backupClient = BackupClient.builder()
          .httpClient(httpClient)
          .region(Region.US_WEST_2)
          .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
              System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
              System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
              System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))))
          .endpointOverride(
              URI.create("https://backup.us-west-2.amazonaws.com"))
          .build();

      return new AwsDBBackRestore.Builder()
          .withBackupClient(backupClient)
          .withRegion("us-west-2")
          .withCollectionName("docstore-test-1")
          .withTableArn("arn:aws:dynamodb:us-west-2:654654370895:table/docstore-test-1")
          .build();
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public String getBackupEndpoint() {
      return "https://backup.us-west-2.amazonaws.com";
    }

    @Override
    public List<String> getWiremockExtensions() {
      return List.of();
    }

    @Override
    public boolean supportsBackupRestore() {
      return true;
    }

    @Override
    public Map<String, String> getRestoreOptions() {
      // AWS requires IAM role ARN for restore operations
      return Map.of(
          "iamRoleArn", "arn:aws:iam::654654370895:role/chameleon-multi--f4msu63ppffhs"
      );
    }

    @Override
    public void close() {
      if (backupClient != null) {
        backupClient.close();
      }
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }
}
