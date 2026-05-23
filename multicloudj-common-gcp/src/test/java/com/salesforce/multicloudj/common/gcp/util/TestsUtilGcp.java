package com.salesforce.multicloudj.common.gcp.util;

import static com.salesforce.multicloudj.common.util.common.TestsUtil.WIREMOCK_HOST;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.httpjson.InstantiatingHttpJsonChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLSocketFactory;

/**
 * Utility class for GCP testing that provides methods to create test-friendly transport
 * configurations with SSL trust and proxy settings.
 */
public class TestsUtilGcp {

  private TestsUtilGcp() {}

  /**
   * Gets an HttpTransport configured with a proxy to the WireMock server. This allows HTTP/HTTPS
   * traffic to be intercepted for testing.
   *
   * @param port The base port for WireMock (proxy will use port+1)
   * @return A configured HttpTransport
   */
  public static HttpTransport getHttpTransport(int port) {
    try {
      SSLSocketFactory sslSocketFactory = TestsUtil.createTrustAllSSLContext().getSocketFactory();
      Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(WIREMOCK_HOST, port + 1));

      return new NetHttpTransport.Builder()
          .setProxy(proxy)
          .doNotValidateCertificate()
          .setSslSocketFactory(sslSocketFactory)
          .build();
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Failed to configure transport", e);
    }
  }

  /**
   * Gets a transport channel provider configured with a proxy to the WireMock server. This allows
   * HTTP/HTTPS traffic to be intercepted for testing.
   *
   * @param port The base port for WireMock (proxy will use port+1)
   * @return A configured TransportChannelProvider
   */
  public static TransportChannelProvider getTransportChannelProvider(int port) {
    return InstantiatingHttpJsonChannelProvider.newBuilder()
        .setHttpTransport(getHttpTransport(port))
        .build();
  }
}
