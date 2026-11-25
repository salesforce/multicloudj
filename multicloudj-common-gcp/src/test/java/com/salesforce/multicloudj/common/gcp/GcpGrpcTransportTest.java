package com.salesforce.multicloudj.common.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.httpjson.InstantiatingHttpJsonChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxiedSocketAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import org.junit.jupiter.api.Test;

class GcpGrpcTransportTest {

  private static final URI PROXY = URI.create("http://127.0.0.1:3128");

  @Test
  void withProxyInstallsChannelConfiguratorAndKeepsProviderSettings() {
    InstantiatingGrpcChannelProvider original =
        InstantiatingGrpcChannelProvider.newBuilder().setMaxInboundMessageSize(1234).build();

    TransportChannelProvider proxied = GcpGrpcTransport.withProxy(original, PROXY);

    InstantiatingGrpcChannelProvider.Builder proxiedBuilder =
        assertInstanceOf(InstantiatingGrpcChannelProvider.class, proxied).toBuilder();
    assertNotNull(proxiedBuilder.getChannelConfigurator());
    assertEquals(1234, proxiedBuilder.getMaxInboundMessageSize());
    assertNull(original.toBuilder().getChannelConfigurator());
  }

  @Test
  void withProxyRejectsNonGrpcProvider() {
    TransportChannelProvider httpJson = InstantiatingHttpJsonChannelProvider.newBuilder().build();

    assertThrows(IllegalArgumentException.class, () -> GcpGrpcTransport.withProxy(httpJson, PROXY));
    assertThrows(IllegalArgumentException.class, () -> GcpGrpcTransport.withProxy(null, PROXY));
  }

  @Test
  void proxyDetectorTunnelsTargetThroughConfiguredProxy() throws Exception {
    InetSocketAddress target = InetSocketAddress.createUnresolved("service.googleapis.com", 443);

    ProxiedSocketAddress proxied = GcpGrpcTransport.proxyDetector(PROXY).proxyFor(target);

    HttpConnectProxiedSocketAddress connect =
        assertInstanceOf(HttpConnectProxiedSocketAddress.class, proxied);
    assertEquals(new InetSocketAddress("127.0.0.1", 3128), connect.getProxyAddress());
    assertEquals(target, connect.getTargetAddress());
  }

  @Test
  void proxyDetectorLeavesNonInetTargetsDirect() throws Exception {
    SocketAddress nonInetTarget = new SocketAddress() {};

    assertNull(GcpGrpcTransport.proxyDetector(PROXY).proxyFor(nonInetTarget));
  }

  @Test
  void endpointDefaultsToTlsPortWhenUriHasNone() {
    assertEquals(
        "service.googleapis.com:443",
        GcpGrpcTransport.endpoint(URI.create("https://service.googleapis.com")));
  }

  @Test
  void endpointKeepsExplicitPort() {
    assertEquals(
        "service.example.com:8443",
        GcpGrpcTransport.endpoint(URI.create("https://service.example.com:8443")));
  }

  @Test
  void endpointAlreadyInHostPortFormIsUnchanged() {
    assertEquals(
        "service.googleapis.com:443",
        GcpGrpcTransport.endpoint(URI.create("service.googleapis.com:443")));
  }
}
