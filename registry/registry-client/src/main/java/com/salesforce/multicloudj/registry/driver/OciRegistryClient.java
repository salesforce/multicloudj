package com.salesforce.multicloudj.registry.driver;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * OCI Registry API v2 HTTP client.
 * Handles auth (via AuthProvider) and registry operations (fetch manifest, download blob).
 */
public class OciRegistryClient implements AutoCloseable {

    private final String registryEndpoint;
    private final CloseableHttpClient httpClient;
    private final AuthProvider authProvider;

    public OciRegistryClient(String registryEndpoint, AuthProvider authProvider) {
        this(registryEndpoint, authProvider, null);
    }

    public OciRegistryClient(String registryEndpoint, AuthProvider authProvider, CloseableHttpClient httpClient) {
        this.registryEndpoint = registryEndpoint;
        this.authProvider = authProvider;
        this.httpClient = httpClient != null ? httpClient : HttpClients.createDefault();
    }

    /**
     * Builds HTTP Authorization header for the given repository.
     * Framework uses Basic auth; Bearer/token exchange can be added later.
     */
    public String getHttpAuthHeader(String repository) throws IOException {
        AuthCredentials creds = authProvider.getAuthCredentials();
        String credentialsStr = creds.getUsername() + ":" + creds.getToken();
        String encoded = Base64.getEncoder().encodeToString(credentialsStr.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    /**
     * Fetches the image manifest for the given repository and reference (tag or digest).
     * To be implemented with HTTP GET /v2/{repository}/manifests/{reference}.
     */
    public Manifest fetchManifest(String repository, String reference) throws IOException {
        throw new UnsupportedOperationException("fetchManifest() not yet implemented");
    }

    /**
     * Downloads a blob (layer or config) by digest.
     * To be implemented with HTTP GET /v2/{repository}/blobs/{digest}.
     */
    public InputStream downloadBlob(String repository, String digest) throws IOException {
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
