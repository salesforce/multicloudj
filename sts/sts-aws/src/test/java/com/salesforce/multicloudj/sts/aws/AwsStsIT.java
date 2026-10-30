package com.salesforce.multicloudj.sts.aws;

import com.salesforce.multicloudj.common.aws.util.TestsUtilAws;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.client.AbstractStsIT;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.driver.AbstractStsVerifier;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

public class AwsStsIT extends AbstractStsIT {
  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements AbstractStsIT.Harness {
    SdkHttpClient httpClient;
    StsClient client;
    int port = ThreadLocalRandom.current().nextInt(2000, 20000);

    @Override
    public AbstractSts createStsDriver(boolean longTermCredentials) {
      httpClient = TestsUtilAws.getProxyClient("https", port);
      StsClientBuilder builder =
          StsClient.builder()
              .httpClient(httpClient)
              .region(Region.US_WEST_2)
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsSessionCredentials.create(
                          System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                          System.getenv()
                              .getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                          System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN"))))
              .endpointOverride(URI.create("https://sts.us-west-2.amazonaws.com"));
      if (longTermCredentials && System.getProperty("record") != null) {
        builder.credentialsProvider(ProfileCredentialsProvider.create());
      }

      client = builder.build();
      return new AwsSts().builder().build(client);
    }

    @Override
    public String getRoleName() {
      return "arn:aws:iam::654654370895:role/chameleon-jcloud-test";
    }

    @Override
    public String getStsEndpoint() {
      return "https://sts.us-west-2.amazonaws.com";
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
      return List.of("com.salesforce.multicloudj.sts.aws.ReplaceAuthHeaderTransformer");
    }

    @Override
    public boolean supportsGetAccessToken() {
      return true;
    }

    @Override
    public boolean supportsCloudNativeAuthReplay() {
      return true;
    }

    @Override
    public AbstractStsUtilities<?> createStsUtilitiesDriver() {
      // Signing is offline: explicit session credentials mean no network call is made here.
      AwsStsUtilities.Builder builder = new AwsStsUtilities().builder();
      builder.withRegion("us-west-2");
      builder.withCredentialsOverrider(
          new CredentialsOverrider.Builder(CredentialsType.SESSION)
              .withSessionCredentials(
                  new StsCredentials(
                      System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
                      System.getenv()
                          .getOrDefault("AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY"),
                      System.getenv().getOrDefault("AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN")))
              .build());
      return builder.build();
    }

    @Override
    public AbstractStsVerifier createStsVerifierDriver() {
      // The verifier replays the signed request to the STS endpoint named in the signed identity;
      // the proxy routes that call through the WireMock server. The JDK HttpClient speaks plain
      // HTTP to its proxy, so it targets the WireMock HTTP port (port + 1), not the HTTPS port.
      HttpClient verifierHttpClient =
          HttpClient.newBuilder()
              .proxy(ProxySelector.of(new InetSocketAddress(TestsUtil.WIREMOCK_HOST, port + 1)))
              .sslContext(TestsUtil.createTrustAllExtendedSSLContext())
              .build();
      return new AwsStsVerifier(new AwsStsVerifier().builder(), verifierHttpClient);
    }

    @Override
    public void close() {
      if (client != null) {
        client.close();
      }
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }
}
