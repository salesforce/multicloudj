package com.salesforce.multicloudj.kms.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.kms.client.AbstractKmsIT;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

// Runs only in record mode until its WireMock recordings are committed; CI runs mvn verify and
// replay without recordings would fail.
@EnabledIfSystemProperty(named = "record", matches = ".*")
public class GcpKmsIT extends AbstractKmsIT {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements AbstractKmsIT.Harness {
    private static final String PROJECT_ID = "substrate-sdk-gcp-poc1";

    // The key ring must already exist when recording: key rings cannot be deleted, so the suite
    // creates its keys in a long-lived ring rather than creating one per run.
    private static final String KEY_RING =
        "projects/" + PROJECT_ID + "/locations/us-west1/keyRings/multicloudj-kms-it";

    // Crypto key ids can never be reused, even after their versions are destroyed, so this suffix
    // must change whenever the suite is re-recorded.
    private static final String RECORDING_SUFFIX = "r1";

    private static final String KMS_ENDPOINT = "https://cloudkms.googleapis.com";
    private static final String ADMIN_SERVICE_ACCOUNT =
        "chameleon@" + PROJECT_ID + ".iam.gserviceaccount.com";
    // Placeholder: must be an existing service account in the project when recording, because IAM
    // rejects policy members that do not exist.
    private static final String PLACEHOLDER_GRANTEE_SERVICE_ACCOUNT =
        "kms-it-grantee@" + PROJECT_ID + ".iam.gserviceaccount.com";

    private final int port = ThreadLocalRandom.current().nextInt(2000, 20000);
    private final List<KeyManagementServiceClient> clients = new ArrayList<>();
    private KeyManagementServiceClient client;

    @Override
    public AbstractKms createKms() {
      boolean recording = System.getProperty("record") != null;
      try {
        KeyManagementServiceSettings.Builder settings =
            KeyManagementServiceSettings.newHttpJsonBuilder()
                .setTransportChannelProvider(TestsUtilGcp.getTransportChannelProvider(port));
        if (!recording) {
          settings.setCredentialsProvider(
              FixedCredentialsProvider.create(
                  MockGoogleCredentialsFactory.createMockCredentials()));
        }
        client = KeyManagementServiceClient.create(settings.build());
        clients.add(client);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to create the KeyManagementServiceClient", e);
      }
      return new GcpKms.Builder().withKmsClient(client).withKeyContainer(KEY_RING).build();
    }

    @Override
    public String getProviderId() {
      return GcpConstants.PROVIDER_ID;
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
    public String getKmsEndpoint() {
      return KMS_ENDPOINT;
    }

    @Override
    public String getKeyName(String testName) {
      return "kms-it-" + testName + "-" + RECORDING_SUFFIX;
    }

    @Override
    public String getNonexistentKeyId() {
      return KEY_RING + "/cryptoKeys/does-not-exist";
    }

    @Override
    public KeyPrincipal getAdminPrincipal() {
      return KeyPrincipal.of(PrincipalType.IAM_ROLE, ADMIN_SERVICE_ACCOUNT);
    }

    @Override
    public KeyPrincipal getGranteePrincipal() {
      return KeyPrincipal.of(PrincipalType.IAM_ROLE, PLACEHOLDER_GRANTEE_SERVICE_ACCOUNT);
    }

    @Override
    public void deleteKey(KeyRef keyRef) {
      List<CryptoKeyVersion> versions = new ArrayList<>();
      client.listCryptoKeyVersions(keyRef.getKeyId()).iterateAll().forEach(versions::add);
      for (CryptoKeyVersion version : versions) {
        if (version.getState() != CryptoKeyVersionState.DESTROYED
            && version.getState() != CryptoKeyVersionState.DESTROY_SCHEDULED) {
          client.destroyCryptoKeyVersion(version.getName());
        }
      }
    }

    @Override
    public void close() {
      clients.forEach(KeyManagementServiceClient::close);
    }
  }
}
