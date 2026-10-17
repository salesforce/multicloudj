package com.salesforce.multicloudj.kms.aws;

import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.kms.client.AbstractKmsIT;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.kms.model.AliasListEntry;
import software.amazon.awssdk.services.kms.model.DeleteAliasRequest;
import software.amazon.awssdk.services.kms.model.ListAliasesRequest;
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionRequest;

// Runs only in record mode until its WireMock recordings are committed; CI runs mvn verify and
// replay without recordings would fail.
@EnabledIfSystemProperty(named = "record", matches = ".*")
public class AwsKmsIT extends AbstractKmsIT {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements AbstractKmsIT.Harness {
    private static final String ENDPOINT = "https://kms.us-west-2.amazonaws.com";
    private static final String KEY_NAME_PREFIX = "multicloudj-kms-it-";
    private static final String NONEXISTENT_KEY_ID = "5f2b8a1c-9d4e-4c7a-b3e6-0a1d2c3b4e5f";
    private static final int MINIMUM_PENDING_WINDOW_DAYS = 7;

    SdkHttpClient httpClient;
    KmsClient client;
    int port = ThreadLocalRandom.current().nextInt(2000, 20000);

    @Override
    public AbstractKms createKms() {
      httpClient = TestsUtilAws.getProxyClient("https", port);

      KmsClientBuilder builder =
          KmsClient.builder()
              .httpClient(httpClient)
              .region(Region.US_WEST_2)
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsSessionCredentials.create(
                          System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                          System.getenv()
                              .getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                          System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))))
              .endpointOverride(URI.create(ENDPOINT));

      if (System.getProperty("record") != null) {
        builder.credentialsProvider(ProfileCredentialsProvider.create());
      }

      client = builder.build();
      return new AwsKms.Builder().withKmsClient(client).withRegion("us-west-2").build();
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
      return List.of("com.salesforce.multicloudj.kms.aws.ReplaceAuthHeaderTransformer");
    }

    @Override
    public String getKmsEndpoint() {
      return ENDPOINT;
    }

    @Override
    public String getKeyName(String testName) {
      return KEY_NAME_PREFIX + testName;
    }

    @Override
    public String getNonexistentKeyId() {
      return NONEXISTENT_KEY_ID;
    }

    @Override
    public KeyPrincipal getAdminPrincipal() {
      return KeyPrincipal.of(PrincipalType.ACCOUNT, "654654370895");
    }

    @Override
    public KeyPrincipal getGranteePrincipal() {
      return KeyPrincipal.of(
          PrincipalType.IAM_ROLE, "arn:aws:iam::654654370895:role/chameleon-jcloud-test");
    }

    @Override
    public void deleteKey(KeyRef keyRef) {
      String keyId = keyRef.getKeyId();
      List<AliasListEntry> aliases =
          client.listAliases(ListAliasesRequest.builder().keyId(keyId).build()).aliases();
      for (AliasListEntry alias : aliases) {
        if (alias.aliasName().startsWith("alias/" + KEY_NAME_PREFIX)) {
          client.deleteAlias(DeleteAliasRequest.builder().aliasName(alias.aliasName()).build());
        }
      }
      client.scheduleKeyDeletion(
          ScheduleKeyDeletionRequest.builder()
              .keyId(keyId)
              .pendingWindowInDays(MINIMUM_PENDING_WINDOW_DAYS)
              .build());
    }

    @Override
    public void close() {
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }
}
