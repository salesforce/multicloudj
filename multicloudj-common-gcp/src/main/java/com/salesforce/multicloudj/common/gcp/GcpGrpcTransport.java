package com.salesforce.multicloudj.common.gcp;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxyDetector;
import java.net.InetSocketAddress;
import java.net.URI;

/** Configures the gRPC transport of gax-based GCP clients. */
public final class GcpGrpcTransport {

  private static final int DEFAULT_TLS_PORT = 443;

  private GcpGrpcTransport() {}

  /**
   * Returns a copy of {@code channelProvider} whose channel tunnels through {@code proxyEndpoint}
   * with HTTP CONNECT, keeping the provider's other channel settings.
   *
   * @param channelProvider the gRPC channel provider installed by the client's settings builder
   * @param proxyEndpoint the HTTP proxy to tunnel through
   * @return the proxied gRPC channel provider
   * @throws IllegalArgumentException if {@code channelProvider} is not an {@link
   *     InstantiatingGrpcChannelProvider}
   */
  public static TransportChannelProvider withProxy(
      TransportChannelProvider channelProvider, URI proxyEndpoint) {
    if (!(channelProvider instanceof InstantiatingGrpcChannelProvider)) {
      throw new IllegalArgumentException(
          "Expected an InstantiatingGrpcChannelProvider but got "
              + (channelProvider == null ? "null" : channelProvider.getClass().getName()));
    }
    ProxyDetector proxyDetector = proxyDetector(proxyEndpoint);
    return ((InstantiatingGrpcChannelProvider) channelProvider)
        .toBuilder()
        .setChannelConfigurator(channelBuilder -> channelBuilder.proxyDetector(proxyDetector))
        .build();
  }

  /**
   * Returns a proxy detector that routes every connection through {@code proxyEndpoint}.
   *
   * @param proxyEndpoint the HTTP proxy to tunnel through
   * @return the proxy detector
   */
  public static ProxyDetector proxyDetector(URI proxyEndpoint) {
    return target -> {
      if (!(target instanceof InetSocketAddress)) {
        return null;
      }
      // gRPC rejects an unresolved proxy address, so resolve it on each connection attempt
      // rather than once at configuration time.
      InetSocketAddress proxyAddress =
          new InetSocketAddress(proxyEndpoint.getHost(), proxyEndpoint.getPort());
      return HttpConnectProxiedSocketAddress.newBuilder()
          .setProxyAddress(proxyAddress)
          .setTargetAddress((InetSocketAddress) target)
          .build();
    };
  }

  /**
   * Converts an endpoint URI into the {@code host:port} form the gRPC channel requires, defaulting
   * to port 443 when the URI has none. A value without a host component, such as {@code
   * service.googleapis.com:443}, is already in that form and is returned unchanged.
   *
   * @param endpoint the configured endpoint
   * @return the endpoint as {@code host:port}
   */
  public static String endpoint(URI endpoint) {
    if (endpoint.getHost() == null) {
      return endpoint.toString();
    }
    int port = endpoint.getPort() == -1 ? DEFAULT_TLS_PORT : endpoint.getPort();
    return endpoint.getHost() + ":" + port;
  }
}
