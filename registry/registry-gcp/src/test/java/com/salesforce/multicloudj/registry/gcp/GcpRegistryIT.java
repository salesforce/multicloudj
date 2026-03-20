package com.salesforce.multicloudj.registry.gcp;

import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.registry.client.AbstractRegistryIT;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
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

      CloseableHttpClient ociHttpClient = TestsUtilGcp.getProxyHttpClient(port);

      GcpRegistry.Builder builder =
          (GcpRegistry.Builder)
              new GcpRegistry.Builder().withRegistryEndpoint(ENDPOINT);

      if (isRecordingEnabled) {
        return new GcpRegistry(builder, ociHttpClient);
      }

      CredentialsOverrider overrider =
          new CredentialsOverrider.Builder(CredentialsType.SESSION)
              .withSessionCredentials(
                  new StsCredentials("mock-key-id", "mock-secret", "mock-gcp-oauth2-token"))
              .build();
      builder.withCredentialsOverrider(overrider);

      return new GcpRegistry(builder, ociHttpClient);
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
