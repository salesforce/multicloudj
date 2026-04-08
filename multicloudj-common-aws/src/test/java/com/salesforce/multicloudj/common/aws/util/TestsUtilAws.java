package com.salesforce.multicloudj.common.aws.util;

import static com.salesforce.multicloudj.common.util.common.TestsUtil.WIREMOCK_HOST;

import com.salesforce.multicloudj.common.util.common.TestsUtil;
import java.net.URI;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.apache.internal.conn.SdkTlsSocketFactory;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

public class TestsUtilAws {

  public static class TrustAllHostNameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

  /**
   * Get the http client with forward proxy to wiremock server . local SDK to wiremock can be http
   * or https
   *
   * @return http client of aws sdk
   */
  public static SdkHttpClient getProxyClient(String scheme, int port) {

    ProxyConfiguration proxyConfig =
        ProxyConfiguration.builder()
            .endpoint(URI.create(scheme + "://" + WIREMOCK_HOST + ":" + port))
            .build();
    TrustManager[] trustAllCerts = TestsUtil.createTrustAllManager();

    SSLContext sslContext = TestsUtil.createTrustAllSSLContext();

    SdkTlsSocketFactory sslSocketFactory =
        new SdkTlsSocketFactory(sslContext, new TrustAllHostNameVerifier());

    return ApacheHttpClient.builder()
        .proxyConfiguration(proxyConfig)
        .tlsTrustManagersProvider(() -> trustAllCerts)
        .socketFactory(sslSocketFactory)
        .build();
  }

  /**
   * Get an async Netty HTTP client configured with a forward proxy to the WireMock server.
   *
   * @return async http client suitable for {@code S3AsyncClient}
   */
  public static NettyNioAsyncHttpClient.Builder getAsyncProxyClientBuilder(
      String scheme, int port) {
    return NettyNioAsyncHttpClient.builder()
        .proxyConfiguration(
            software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder()
                .host(WIREMOCK_HOST)
                .port(port)
                .scheme(scheme)
                .build())
        .tlsTrustManagersProvider(() -> TestsUtil.createTrustAllManager());
  }
}
