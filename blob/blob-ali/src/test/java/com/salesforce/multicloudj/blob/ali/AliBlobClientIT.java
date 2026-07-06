package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClientBuilder;
import com.salesforce.multicloudj.blob.client.AbstractBlobClientIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.HostnameVerificationPolicy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TlsSocketStrategy;
import org.apache.hc.core5.http.HttpHost;

public class AliBlobClientIT extends AbstractBlobClientIT {

  private static final String endpoint =
      "https://oss-cn-shanghai.aliyuncs.com";
  private static final String region = "cn-shanghai";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    int port =
        ThreadLocalRandom.current().nextInt(2000, 20000);

    OSSClient ossClient;

    @Override
    public AbstractBlobClient<?> createBlobClient(
        boolean useValidCredentials) {

      String accessKeyId =
          System.getenv().getOrDefault(
              "ALIBABA_CLOUD_ACCESS_KEY_ID",
              "FAKE_ACCESS_KEY");
      String accessKeySecret =
          System.getenv().getOrDefault(
              "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
              "FAKE_SECRET_ACCESS_KEY");
      String sessionToken =
          System.getenv().getOrDefault(
              "ALIBABA_CLOUD_SECURITY_TOKEN",
              "FAKE_SECURITY_TOKEN");

      if (!useValidCredentials) {
        accessKeyId = "invalidAccessKey";
        accessKeySecret = "invalidSecretAccessKey";
        sessionToken = "invalidSessionToken";
      }

      StsCredentials sessionCreds =
          new StsCredentials(
              accessKeyId, accessKeySecret, sessionToken);
      CredentialsOverrider credentialsOverrider =
          new CredentialsOverrider.Builder(CredentialsType.SESSION)
              .withSessionCredentials(sessionCreds)
              .build();

      SSLContext sslContext = TestsUtil.createTrustAllSSLContext();
      TlsSocketStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
          .setSslContext(sslContext)
          .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
          .setHostVerificationPolicy(HostnameVerificationPolicy.CLIENT)
          .buildClassic();
      PoolingHttpClientConnectionManager connManager =
          PoolingHttpClientConnectionManagerBuilder.create()
              .setTlsSocketStrategy(tlsStrategy)
              .build();
      RequestConfig requestConfig = RequestConfig.custom()
          .setProxy(new HttpHost(TestsUtil.WIREMOCK_HOST, port + 1))
          .build();
      Apache5HttpClient httpClient = Apache5HttpClientBuilder.create()
          .connectionManager(connManager)
          .requestConfig(requestConfig)
          .build();

      ossClient = OSSClient.newBuilder()
          .region(region)
          .credentialsProvider(
              OssCredentialsProvider.getCredentialsProvider(credentialsOverrider, region))
          .endpoint(endpoint)
          .httpClient(httpClient)
          .build();

      AliBlobClient.Builder builder =
          new AliBlobClient.Builder();
      builder
          .withEndpoint(URI.create(endpoint))
          .withRegion(region)
          .withCredentialsOverrider(credentialsOverrider);

      return builder.build(ossClient);
    }

    @Override
    public String getEndpoint() {
      return endpoint;
    }

    @Override
    public String getProviderId() {
      return AliConstants.PROVIDER_ID;
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public java.util.List<String> getRecordingCaptureHeaders() {
      return java.util.List.of("Host");
    }

    @Override
    public void close() {
      if (ossClient != null) {
        try {
          ossClient.close();
        } catch (Exception e) {
          // best-effort cleanup
        }
      }
    }
  }
}
