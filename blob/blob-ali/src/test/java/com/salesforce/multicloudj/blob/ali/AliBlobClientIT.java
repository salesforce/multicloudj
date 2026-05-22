package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.comm.SignVersion;
import com.salesforce.multicloudj.blob.client.AbstractBlobClientIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;

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

    OSS ossClient;

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

      ClientBuilderConfiguration clientConfig =
          new ClientBuilderConfiguration();
      clientConfig.setSignatureVersion(SignVersion.V4);
      clientConfig.setVerifySSLEnable(false);
      clientConfig.setX509TrustManagers(
          new javax.net.ssl.X509TrustManager[] {
            (javax.net.ssl.X509TrustManager)
                TestsUtil.createTrustAllManager()[0]
          });
      clientConfig.setProxyHost(TestsUtil.WIREMOCK_HOST);
      clientConfig.setProxyPort(port + 1);

      ossClient =
          OSSClientBuilder.create()
              .region(region)
              .endpoint(endpoint)
              .clientConfiguration(clientConfig)
              .credentialsProvider(
                  OSSCredentialsProvider
                      .getCredentialsProvider(
                          credentialsOverrider, region))
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
        ossClient.shutdown();
      }
    }
  }
}
