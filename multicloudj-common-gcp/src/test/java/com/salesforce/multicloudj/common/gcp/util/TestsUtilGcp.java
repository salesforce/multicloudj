package com.salesforce.multicloudj.common.gcp.util;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.httpjson.InstantiatingHttpJsonChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.grpc.ManagedChannelBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import static com.salesforce.multicloudj.common.util.common.TestsUtil.WIREMOCK_HOST;

/**
 * Utility class for GCP testing that provides methods to create test-friendly
 * transport configurations with SSL trust and proxy settings.
 */
public class TestsUtilGcp {

    private static final String TLS_PROTOCOL = "TLS";

    private TestsUtilGcp() {
        // Private constructor to prevent instantiation of utility class
    }

    /**
     * Creates a trust manager that accepts all certificates without validation.
     * For testing purposes only.
     *
     * @return An array containing a single trust-all X509TrustManager
     */
    private static TrustManager[] createTrustAllManager() {
        return new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        // No validation for testing
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        // No validation for testing
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
    }

    /**
     * Creates an SSL context that trusts all certificates.
     * For testing purposes only.
     *
     * @return A configured SSLContext
     * @throws SecurityConfigurationException if SSL context creation fails
     */
    private static SSLContext createTrustAllSSLContext() {
        try {
            SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
            sslContext.init(null, createTrustAllManager(), new java.security.SecureRandom());
            return sslContext;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new SecurityConfigurationException("Failed to create SSL context", e);
        }
    }

    /**
     * Gets an HttpTransport configured with a proxy to the WireMock server.
     * This allows HTTP/HTTPS traffic to be intercepted for testing.
     *
     * @param port The base port for WireMock (proxy will use port+1)
     * @return A configured HttpTransport
     * @throws SecurityConfigurationException if transport configuration fails
     */
    public static HttpTransport getHttpTransport(int port) {
        try {
            // Get SSL socket factory that trusts all certificates
            SSLSocketFactory sslSocketFactory = createTrustAllSSLContext().getSocketFactory();

            // Define proxy to WireMock
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(WIREMOCK_HOST, port + 1));

            // Set up HTTP transport with proxy and SSL settings
            return new NetHttpTransport.Builder()
                    .setProxy(proxy)
                    .doNotValidateCertificate()
                    .setSslSocketFactory(sslSocketFactory)
                    .build();
        } catch (GeneralSecurityException e) {
            throw new SecurityConfigurationException("Failed to configure transport", e);
        }
    }

    /**
     * Gets a transport channel provider configured with a proxy to the WireMock server.
     * This allows HTTP/HTTPS traffic to be intercepted for testing.
     *
     * @param port The base port for WireMock (proxy will use port+1)
     * @return A configured TransportChannelProvider
     * @throws SecurityConfigurationException if transport configuration fails
     */
    public static TransportChannelProvider getTransportChannelProvider(int port) {
        // Create and return channel provider using configured transport
        return InstantiatingHttpJsonChannelProvider.newBuilder()
                .setHttpTransport(getHttpTransport(port))
                .build();
    }

    /**
     * Gets a gRPC transport channel provider configured with a proxy to the WireMock server.
     * This allows gRPC traffic to be intercepted for testing.
     * 
     * Note: WireMock does not natively support gRPC protocol. This method is provided for
     * completeness but may require additional configuration or a gRPC-compatible proxy.
     *
     * @param port The base port for WireMock (proxy will use port+1)
     * @param endpoint The gRPC endpoint to connect to
     * @return A configured TransportChannelProvider for gRPC
     */
    public static TransportChannelProvider getGrpcTransportChannelProvider(int port, String endpoint) {
        // For gRPC, we need to use a plain text channel through the proxy
        // Note: This is a simplified implementation. Full gRPC proxying through WireMock
        // requires additional setup or a gRPC-aware proxy.
        return InstantiatingGrpcChannelProvider.newBuilder()
                .setEndpoint(WIREMOCK_HOST + ":" + port)
                .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                .build();
    }

    /**
     * Custom exception for security configuration issues.
     */
    public static class SecurityConfigurationException extends RuntimeException {
        public SecurityConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}