package com.salesforce.multicloudj.registry.driver;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** OCI Registry API v2 HTTP client; handles auth and registry operations. */
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

    /** Builds the HTTP Authorization header for the given repository. */
    public String getHttpAuthHeader(String repository) throws IOException {
        // TODO: Framework uses Basic auth; Bearer/token exchange needs to be implemented
        String username = authProvider.getAuthUsername();
        String token = authProvider.getAuthToken();
        String credentialsStr = username + ":" + token;
        String encoded = Base64.getEncoder().encodeToString(credentialsStr.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
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
