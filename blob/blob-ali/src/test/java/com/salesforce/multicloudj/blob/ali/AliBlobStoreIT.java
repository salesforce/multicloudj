package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.hash.CRC64;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClientBuilder;
import com.salesforce.multicloudj.blob.client.AbstractBlobStoreIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
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

public class AliBlobStoreIT extends AbstractBlobStoreIT {

  private static final String endpoint = "https://oss-cn-shanghai.aliyuncs.com";
  private static final String bucketName = "chameleon-multicloudj-test";
  private static final String versionedBucketName = "chameleon-multicloudj-test-versioned";
  private static final String nonExistentBucketName = "java-bucket-does-not-exist";
  private static final String region = "cn-shanghai";

  /**
   * OSS sends this header only on copy operations (never on a plain upload PUT). Captured as a
   * recording match header to disambiguate copy-PUT vs upload-PUT to the same key.
   */
  private static final String OSS_COPY_SOURCE_HEADER = "x-oss-copy-source";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    int port = ThreadLocalRandom.current().nextInt(2000, 20000);

    OSSClient ossClient;

    @Override
    public AbstractBlobStore createBlobStore(
        boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket) {

      String accessKeyId =
          System.getenv().getOrDefault("ALIBABA_CLOUD_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
      String accessKeySecret =
          System.getenv().getOrDefault("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "FAKE_SECRET_ACCESS_KEY");
      String sessionToken =
          System.getenv().getOrDefault("ALIBABA_CLOUD_SECURITY_TOKEN", "FAKE_SESSION_TOKEN");

      if (!useValidCredentials) {
        accessKeyId = "invalidAccessKey";
        accessKeySecret = "invalidSecretAccessKey";
        sessionToken = "invalidSessionToken";
      }

      StsCredentials sessionCreds = new StsCredentials(accessKeyId, accessKeySecret, sessionToken);
      CredentialsOverrider credentialsOverrider =
          new CredentialsOverrider.Builder(CredentialsType.SESSION)
              .withSessionCredentials(sessionCreds)
              .build();

      String bucketNameToUse =
          useValidBucket
              ? (useVersionedBucket ? versionedBucketName : bucketName)
              : nonExistentBucketName;

      return createBlobStore(bucketNameToUse, credentialsOverrider);
    }

    private AbstractBlobStore createBlobStore(
        final String bucketName, final CredentialsOverrider credentialsOverrider) {

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

      // In record mode the test hits the real cn-shanghai endpoint. Under peak-hour congestion the
      // server can drop the connection mid-request (NoHttpResponseException); the SDK's default
      // retry then re-sends, which causes WireMock to record duplicate / out-of-order stubs (e.g.
      // two UploadPart stubs for the same part) that later break replay. Disable retries during
      // recording so a dropped request fails fast and cleanly rather than producing a misleading
      // stub by chance — just re-run the recording (ideally off-peak). Replay mode is unaffected
      // (it serves stubs locally and never retries against a real server).
      var ossClientBuilder =
          OSSClient.newBuilder()
              .region(region)
              .endpoint(endpoint)
              .credentialsProvider(
                  OSSCredentialsProvider.getCredentialsProvider(credentialsOverrider, region))
              .httpClient(httpClient);
      if (System.getProperty("record") != null) {
        ossClientBuilder.retryMaxAttempts(1);
      }
      ossClient = ossClientBuilder.build();

      AliBlobStore.Builder builder = new AliBlobStore.Builder();
      builder
          .withClient(ossClient)
          .withEndpoint(URI.create(endpoint))
          .withBucket(bucketName)
          .withRegion(region)
          .withCredentialsOverrider(credentialsOverrider);

      return builder.build();
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
    public String getMetadataHeader(String key) {
      return "x-oss-meta-" + key;
    }

    @Override
    public String getTaggingHeader() {
      return "x-oss-tagging";
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public String getKmsKeyId() {
      return null;
    }

    @Override
    public boolean isSha256Supported() {
      // Ali OSS's only native object checksum is CRC64-ECMA; it rejects SHA256 (and CRC32C) for
      // caller-supplied checksums (see AliTransformer.rejectUnsupportedChecksum). SHA256-specific
      // conformance tests are skipped via this capability flag rather than a provider guard.
      return false;
    }

    @Override
    public boolean isDirectoryUploadSupported() {
      // Ali implements directory operations only on the async blob store; the synchronous
      // AliBlobStore does not, so the sync directory conformance tests are skipped via this
      // capability flag (matching the AWS harness, which is also async-only for directory ops).
      return false;
    }

    @Override
    public String computeChecksum(byte[] content) {
      CRC64 crc64 = new CRC64();
      crc64.update(content, content.length);
      return String.valueOf(crc64.getValue());
    }

    @Override
    public java.util.List<String> getRecordingCaptureHeaders() {
      // OSS_COPY_SOURCE_HEADER disambiguates copy-PUT vs upload-PUT to the same key, and
      // distinguishes multiple copies to the same key by their differing source value. Inert
      // for non-copy requests (header absent -> no matcher added).
      return java.util.List.of("Host", OSS_COPY_SOURCE_HEADER);
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
