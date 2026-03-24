package com.salesforce.multicloudj.registry.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.registry.client.AbstractRegistryIT;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.http.impl.client.CloseableHttpClient;

public class GcpRegistryIT extends AbstractRegistryIT {

  private static final String ENDPOINT = "https://us-central1-docker.pkg.dev";
  private static final String TEST_IMAGE_REF =
      "substrate-sdk-gcp-poc1/test-repo/hello-world:latest";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    int port = ThreadLocalRandom.current().nextInt(1000, 10000);

    @Override
    public AbstractRegistry createRegistryDriver() {
      boolean isRecordingEnabled = System.getProperty("record") != null;

      CloseableHttpClient ociHttpClient = TestsUtil.getProxyHttpClient(port);

      GcpRegistry.Builder builder =
          (GcpRegistry.Builder)
              new GcpRegistry.Builder().withRegistryEndpoint(ENDPOINT);

      GoogleCredentials credentials;
      if (isRecordingEnabled) {
        try {
          credentials = GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
          throw new RuntimeException("Failed to load GCP credentials for recording", e);
        }
      } else {
        credentials = MockGoogleCredentialsFactory.createMockCredentials();
      }

      return new GcpRegistry(builder, ociHttpClient, credentials);
    }

    @Override
    public String getEndpoint() {
      return ENDPOINT;
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
    public String getTestImageRef() {
      return TEST_IMAGE_REF;
    }

    @Override
    public List<String> getWiremockExtensions() {
      return List.of(
          "com.salesforce.multicloudj.registry.gcp.util.RegistryTokenRedactingTransformer");
    }

    @Override
    public void close() {}
  }

}
