package com.salesforce.multicloudj.registry.aws;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.registry.client.AbstractRegistryIT;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.http.impl.client.CloseableHttpClient;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecr.EcrClient;

public class AwsRegistryIT extends AbstractRegistryIT {

  private static final String ENDPOINT = "https://654654370895.dkr.ecr.us-west-2.amazonaws.com";
  private static final String REGION = "us-west-2";
  private static final String TEST_IMAGE_REF = "test-registry:latest";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    int port = ThreadLocalRandom.current().nextInt(1000, 10000);
    SdkHttpClient sdkHttpClient;
    EcrClient ecrClient;

    @Override
    public AbstractRegistry createRegistryDriver() {
      CloseableHttpClient ociHttpClient = TestsUtilAws.getProxyHttpClient(port);
      sdkHttpClient = TestsUtilAws.getProxyClient("https", port);

      ecrClient =
          EcrClient.builder()
              .region(Region.of(REGION))
              .httpClient(sdkHttpClient)
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsSessionCredentials.create(
                          System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                          System.getenv()
                              .getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                          System.getenv()
                              .getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))))
              .build();

      AwsRegistry.Builder builder =
          (AwsRegistry.Builder)
              new AwsRegistry.Builder()
                  .withRegion(REGION)
                  .withRegistryEndpoint(ENDPOINT);

      return new AwsRegistry(builder, ecrClient, ociHttpClient);
    }

    @Override
    public String getEndpoint() {
      return ENDPOINT;
    }

    @Override
    public String getProviderId() {
      return AwsConstants.PROVIDER_ID;
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
          "com.salesforce.multicloudj.registry.aws.util.S3RedirectFollowingTransformer");
    }

    @Override
    public void close() {
      if (ecrClient != null) {
        ecrClient.close();
      }
      if (sdkHttpClient != null) {
        sdkHttpClient.close();
      }
    }
  }
}
