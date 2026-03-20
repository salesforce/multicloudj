package com.salesforce.multicloudj.common.aws.util;

import static com.salesforce.multicloudj.common.util.common.TestsUtil.WIREMOCK_HOST;

import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.apache.internal.conn.SdkTlsSocketFactory;

public class TestsUtilAws {

  private static TrustManager[] createTrustAllManager() {
    return new TrustManager[] {
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType) {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }
      }
    };
  }

  public static class TrustAllHostNameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

  /**
   * Gets an Apache HttpClient configured with a proxy to the WireMock server and trust-all SSL.
   * Used for services that communicate via raw HTTP (e.g., OCI registry) rather than AWS SDK
   * clients.
   *
   * @param port The base port for WireMock (proxy will use port+1)
   * @return A configured CloseableHttpClient
   */
  public static CloseableHttpClient getProxyHttpClient(int port) {
    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, createTrustAllManager(), new SecureRandom());
      return HttpClients.custom()
          .setProxy(new HttpHost(WIREMOCK_HOST, port + 1))
          .setSSLContext(sslContext)
          .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
          .build();
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Failed to create proxy HTTP client", e);
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
    TrustManager[] trustAllCerts = createTrustAllManager();

    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustAllCerts, new SecureRandom());

      SdkTlsSocketFactory sslSocketFactory =
          new SdkTlsSocketFactory(sslContext, new TrustAllHostNameVerifier());

      return ApacheHttpClient.builder()
          .proxyConfiguration(proxyConfig)
          .tlsTrustManagersProvider(() -> trustAllCerts)
          .socketFactory(sslSocketFactory)
          .build();
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException(e);
    }
  }
}
