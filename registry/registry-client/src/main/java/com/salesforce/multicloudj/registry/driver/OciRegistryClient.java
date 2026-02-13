package com.salesforce.multicloudj.registry.driver;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** OCI Registry API v2 HTTP client; handles auth and registry operations. */
public class OciRegistryClient implements AutoCloseable {

    private static final String AUTH_SCHEME_ANONYMOUS = "anonymous";
    private static final String AUTH_SCHEME_BASIC = "basic";
    private static final String AUTH_SCHEME_BEARER = "bearer";

    private final String registryEndpoint;
    private final CloseableHttpClient httpClient;
    private final AuthProvider authProvider;
    private final BearerTokenExchange tokenExchange;

    /** Lock for thread-safe lazy initialization of cachedChallenge. */
    private final Object challengeLock = new Object();

    /** Cached auth challenge, lazily initialized with double-checked locking. */
    private volatile AuthChallenge cachedChallenge;

    /**
     * Creates a new OciRegistryClient.
     *
     * <p>The HTTP client is configured with an AuthStrippingInterceptor that removes Authorization
     * headers when following redirects to external hosts. This is required for registries like AWS ECR that redirect
     * blob downloads to S3 pre-signed URLs.
     *
     * @param registryEndpoint the registry base URL
     * @param authProvider the authentication provider
     */
    public OciRegistryClient(String registryEndpoint, AuthProvider authProvider) {
        this.registryEndpoint = registryEndpoint;
        this.authProvider = authProvider;
        this.httpClient = HttpClients.custom()
                .addInterceptorLast(new AuthStrippingInterceptor(registryEndpoint))
                .build();
        this.tokenExchange = new BearerTokenExchange(this.httpClient);
    }

    /**
     * HTTP request interceptor that strips Authorization headers when the request target
     * is not the registry host.
     *
     * <p>This is necessary because registries like AWS ECR redirect blob downloads to S3 pre-signed URLs,
     * which already contain authentication in query parameters. Sending an Authorization header to S3
     * causes a 400 error ("Only one auth mechanism allowed").
     */
    private static class AuthStrippingInterceptor implements HttpRequestInterceptor {
        private final String registryHost;

        AuthStrippingInterceptor(String registryEndpoint) {
            this.registryHost = extractHost(registryEndpoint);
        }

        @Override
        public void process(HttpRequest request, HttpContext context) {
            // Get the target host from the context (works for both initial requests and redirects)
            var targetHost = (org.apache.http.HttpHost) context.getAttribute(HttpClientContext.HTTP_TARGET_HOST);
            if (targetHost != null) {
                String requestHost = targetHost.getHostName();
                // Strip Authorization header if request is not to the registry host
                if (!registryHost.equalsIgnoreCase(requestHost)) {
                    request.removeHeaders(HttpHeaders.AUTHORIZATION);
                }
            }
        }

        private static String extractHost(String url) {
            String host = url.replaceFirst("^https?://", "");
            int slashIndex = host.indexOf('/');
            if (slashIndex > 0) {
                host = host.substring(0, slashIndex);
            }
            int colonIndex = host.indexOf(':');
            if (colonIndex > 0) {
                host = host.substring(0, colonIndex);
            }
            return host;
        }
    }

    /**
     * Builds HTTP Authorization header for the given repository.
     * Discovers auth requirements on first call and caches the result.
     *
     * @param repository the repository name (used for Bearer token scope)
     * @return the Authorization header value, or null if anonymous access (no auth required)
     * @throws IOException if authentication fails
     */
    public String getHttpAuthHeader(String repository) throws IOException {
        // Double-checked locking for thread-safe lazy initialization
        if (cachedChallenge == null) {
            synchronized (challengeLock) {
                if (cachedChallenge == null) {
                    cachedChallenge = AuthChallenge.discover(httpClient, registryEndpoint);
                }
            }
        }

        final String scheme = cachedChallenge.getScheme();
        if (StringUtils.isBlank(scheme)) {
            throw new IllegalArgumentException("Authentication scheme is blank or missing");
        }

        switch (scheme.toLowerCase()) {
            case AUTH_SCHEME_ANONYMOUS:
                return null;
            case AUTH_SCHEME_BASIC:
                return buildBasicAuthHeader();
            case AUTH_SCHEME_BEARER:
                return buildBearerAuthHeader(repository);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported authentication scheme: " + scheme);
        }
    }

    private String buildBasicAuthHeader() throws IOException {
        String username = authProvider.getAuthUsername();
        String token = authProvider.getAuthToken();
        String credentials = username + ":" + token;
        String encoded = Base64.getEncoder().encodeToString(
                credentials.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    private String buildBearerAuthHeader(String repository) throws IOException {
        // Get identity token from provider (e.g., GCP OAuth2 access token)
        String identityToken = authProvider.getAuthToken();

        // Exchange for registry-scoped bearer token
        String bearerToken = tokenExchange.getBearerToken(
                cachedChallenge, identityToken, repository, "pull");

        return "Bearer " + bearerToken;
    }

    /** Fetches the image manifest for the given repository and reference (tag or digest). */
    public Manifest fetchManifest(String repository, String reference) throws IOException {
        // TODO: need to be implemented
        throw new UnsupportedOperationException("fetchManifest() not yet implemented");
    }

    /** Downloads a blob (layer or config) by digest. */
    public InputStream downloadBlob(String repository, String digest) throws IOException {
        // TODO: need to be implemented
        throw new UnsupportedOperationException("downloadBlob() not yet implemented");
    }

    public String getRegistryEndpoint() {
        return registryEndpoint;
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
