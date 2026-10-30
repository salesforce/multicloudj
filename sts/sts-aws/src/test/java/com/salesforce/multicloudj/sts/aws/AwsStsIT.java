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
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
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
    HttpClient verifierHttpClient;
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
      verifierHttpClient =
          HttpClient.newBuilder()
              .proxy(ProxySelector.of(new InetSocketAddress(TestsUtil.WIREMOCK_HOST, port + 1)))
              .sslContext(createTrustAllExtendedSslContext())
              .build();
      return new AwsStsVerifier(new AwsStsVerifier().builder(), verifierHttpClient);
    }

    // The JDK HttpClient checks the server certificate name against the requested host. WireMock
    // presents its own certificate for every proxied host, so a trust manager that extends
    // X509ExtendedTrustManager is needed to opt out of that name check in tests.
    private static SSLContext createTrustAllExtendedSslContext() {
      try {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(
            null, new TrustManager[] {new TrustAllExtendedTrustManager()}, new SecureRandom());
        return sslContext;
      } catch (GeneralSecurityException e) {
        throw new IllegalStateException("Failed to create SSL context", e);
      }
    }

    private static final class TrustAllExtendedTrustManager extends X509ExtendedTrustManager {
      @Override
      public void checkClientTrusted(X509Certificate[] chain, String authType) {}

      @Override
      public void checkServerTrusted(X509Certificate[] chain, String authType) {}

      @Override
      public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {}

      @Override
      public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {}

      @Override
      public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

      @Override
      public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }
    }

    @Override
    public boolean supportsCloudNativeAuthReplay() {
      return true;
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
