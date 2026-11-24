package com.salesforce.multicloudj.pubsub.gcp;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;

/**
 * Stands in for an HTTP proxy on the loopback interface and returns the first request line a
 * client sends to it. The proxy never answers, so the client's call fails once the line is read.
 */
final class ProxyRequestCapture {

  private static final int ACCEPT_TIMEOUT_MILLIS = 30_000;

  /** A client call that is routed through the given proxy. */
  interface ProxiedCall {
    void run(URI proxyEndpoint) throws Exception;
  }

  private ProxyRequestCapture() {}

  static String firstRequestLine(ProxiedCall call) throws Exception {
    try (ServerSocket proxy = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      proxy.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
      URI proxyEndpoint = URI.create("http://127.0.0.1:" + proxy.getLocalPort());
      Thread caller =
          new Thread(
              () -> {
                try {
                  call.run(proxyEndpoint);
                } catch (Exception expected) {
                  // The proxy closes the connection without answering, so the call fails.
                }
              });
      caller.setDaemon(true);
      caller.start();
      try (Socket connection = proxy.accept()) {
        return new BufferedReader(new InputStreamReader(connection.getInputStream(), UTF_8))
            .readLine();
      }
    }
  }
}
