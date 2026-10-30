package com.salesforce.multicloudj.kms.ali;

import com.aliyun.kms20160120.Client;
import com.aliyun.kms20160120.models.DeleteAliasRequest;
import com.aliyun.kms20160120.models.ListAliasesByKeyIdRequest;
import com.aliyun.kms20160120.models.ListAliasesByKeyIdResponseBody;
import com.aliyun.kms20160120.models.ScheduleKeyDeletionRequest;
import com.aliyun.teaopenapi.models.Config;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.kms.client.AbstractKmsIT;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class AliKmsIT extends AbstractKmsIT {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    private static final String REGION = "cn-shanghai";
    private static final String ENDPOINT = "kms." + REGION + ".aliyuncs.com";
    // Must be the account that owns KMS_INSTANCE_ID when recording.
    private static final String ACCOUNT_ID = "1099202394511537";
    // Must be an existing KMS instance in REGION when recording: key policies are only supported
    // for keys in a KMS instance. It is a constant because it is part of recorded request bodies.
    private static final String KMS_INSTANCE_ID = "kst-shh6580b7e6n9od3n9aj0";

    private final int port = ThreadLocalRandom.current().nextInt(2000, 20000);
    private Client kmsClient;

    @Override
    public AbstractKms createKms() {
      String accessKeyId =
          System.getenv().getOrDefault("ALIBABACLOUD_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
      String accessKeySecret =
          System.getenv().getOrDefault("ALIBABACLOUD_ACCESS_KEY_SECRET", "FAKE_SECRET_KEY");
      String securityToken =
          System.getenv().getOrDefault("SECURITY_TOKEN", "FAKE_SESSION_TOKEN");

      Config config =
          new Config()
              .setAccessKeyId(accessKeyId)
              .setAccessKeySecret(accessKeySecret)
              .setSecurityToken(securityToken)
              .setType("sts")
              .setEndpoint(ENDPOINT)
              .setRegionId(REGION)
              .setProtocol("http")
              .setHttpProxy("http://" + TestsUtil.WIREMOCK_HOST + ":" + (port + 1));
      try {
        kmsClient = new Client(config);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create KMS client", e);
      }
      return new AliKms.Builder()
          .withRegion(REGION)
          .withKeyContainer(KMS_INSTANCE_ID)
          .withKmsClient(kmsClient)
          .build();
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public List<String> getWiremockExtensions() {
      return List.of(AliRpcAuthStrippingTransformer.class.getName());
    }

    @Override
    public String getKmsEndpoint() {
      return "http://" + ENDPOINT;
    }

    @Override
    public List<String> getRecordingCaptureHeaders() {
      // The SDK signs with ACS3-HMAC-SHA256, which sends the API name in this header rather than
      // as an Action query parameter; without it, calls with the same parameters are ambiguous.
      return List.of("x-acs-action");
    }

    @Override
    public String getKeyName(String testName) {
      return "multicloudj-13-" + testName;
    }

    @Override
    public String getNonexistentKeyId() {
      return "00000000-0000-0000-0000-000000000000";
    }

    @Override
    public KeyPrincipal getAdminPrincipal() {
      // Account-wide access (acs:ram::<uid>:*), as in the default key policy, so the recording
      // identity keeps control of the key after its policy is replaced.
      return KeyPrincipal.of(PrincipalType.ACCOUNT, ACCOUNT_ID);
    }

    @Override
    public KeyPrincipal getGranteePrincipal() {
      // Must be an existing RAM role in ACCOUNT_ID when recording; RAM ARNs must be lowercase.
      return KeyPrincipal.of(
          PrincipalType.IDENTITY, "acs:ram::" + ACCOUNT_ID + ":role/chameleon-test-2");
    }

    @Override
    public void deleteKey(KeyRef keyRef) {
      try {
        // Aliases outlive a key that is only scheduled for deletion, so they are deleted first to
        // let the next recording create the same alias names.
        ListAliasesByKeyIdResponseBody.ListAliasesByKeyIdResponseBodyAliases aliases =
            kmsClient
                .listAliasesByKeyId(new ListAliasesByKeyIdRequest().setKeyId(keyRef.getKeyId()))
                .getBody()
                .getAliases();
        if (aliases != null && aliases.getAlias() != null) {
          for (ListAliasesByKeyIdResponseBody.ListAliasesByKeyIdResponseBodyAliasesAlias alias :
              aliases.getAlias()) {
            kmsClient.deleteAlias(new DeleteAliasRequest().setAliasName(alias.getAliasName()));
          }
        }
        kmsClient.scheduleKeyDeletion(
            new ScheduleKeyDeletionRequest().setKeyId(keyRef.getKeyId()).setPendingWindowInDays(7));
      } catch (Exception e) {
        throw new RuntimeException("Failed to schedule key deletion", e);
      }
    }

    @Override
    public void close() {}
  }
}
