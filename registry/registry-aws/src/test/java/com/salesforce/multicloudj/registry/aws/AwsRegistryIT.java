package com.salesforce.multicloudj.registry.aws;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.registry.client.AbstractRegistryIT;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.http.impl.client.CloseableHttpClient;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.AuthorizationData;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenRequest;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenResponse;

public class AwsRegistryIT extends AbstractRegistryIT {

  private static final String ENDPOINT = "https://654654370895.dkr.ecr.us-west-2.amazonaws.com";
  private static final String REGION = "us-west-2";
  private static final String TEST_IMAGE_REF = "test-registry:latest";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
<<<<<<< HEAD
    int port = ThreadLocalRandom.current().nextInt(2000, 20000);
=======
    int port = ThreadLocalRandom.current().nextInt(20000, 60000);
>>>>>>> 896c17d1 (example test)
    EcrClient ecrClient;

    @Override
    public AbstractRegistry createRegistryDriver() {
      boolean isRecordingEnabled = System.getProperty("record") != null;
      CloseableHttpClient ociHttpClient = TestsUtil.getProxyHttpClient(port);

      if (isRecordingEnabled) {
        ecrClient =
            EcrClient.builder()
                .region(Region.of(REGION))
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsSessionCredentials.create(
                            System.getenv("AWS_ACCESS_KEY_ID"),
                            System.getenv("AWS_SECRET_ACCESS_KEY"),
                            System.getenv("AWS_SESSION_TOKEN"))))
                .build();
      } else {
        ecrClient = createMockEcrClient();
      }

      return new AwsRegistry.Builder()
          .withRegion(REGION)
          .withRegistryEndpoint(ENDPOINT)
          .withEcrClient(ecrClient)
          .withHttpClient(ociHttpClient)
          .build();
    }

    private static EcrClient createMockEcrClient() {
      String fakeToken =
          Base64.getEncoder()
              .encodeToString("AWS:fake_ecr_password".getBytes(StandardCharsets.UTF_8));
      GetAuthorizationTokenResponse response =
          GetAuthorizationTokenResponse.builder()
              .authorizationData(
                  AuthorizationData.builder()
                      .authorizationToken(fakeToken)
                      .expiresAt(Instant.now().plus(12, ChronoUnit.HOURS))
                      .proxyEndpoint(ENDPOINT)
                      .build())
              .build();

      EcrClient mockClient = mock(EcrClient.class);
      when(mockClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
          .thenReturn(response);
      return mockClient;
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
    }
  }
}
