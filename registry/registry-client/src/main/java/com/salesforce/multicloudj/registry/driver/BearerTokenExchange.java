package com.salesforce.multicloudj.registry.driver;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.StringJoiner;

/**
 * Handles Bearer Token exchange for OAuth2-based authentication (e.g., GCP).
 * Similar to go-containerregistry's transport package.
 */
class BearerTokenExchange {
    private final String registryEndpoint;
    private final OciRegistryClient.AuthProvider authProvider;
    private final CloseableHttpClient httpClient;

    BearerTokenExchange(String registryEndpoint, OciRegistryClient.AuthProvider authProvider) {
        this.registryEndpoint = registryEndpoint;
        this.authProvider = authProvider;
        this.httpClient = HttpClients.createDefault();
    }

    /**
     * Pings the registry to discover authentication requirements.
     * Returns the WWW-Authenticate challenge.
     */
    AuthChallenge ping() throws IOException {
        String url = registryEndpoint + "/v2/";
        HttpGet request = new HttpGet(url);
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            
            if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
                // Parse WWW-Authenticate header
                String wwwAuth = response.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE) != null ?
                    response.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE).getValue() : null;
                return AuthChallenge.parse(wwwAuth);
            }
            
            // No auth required
            return AuthChallenge.basic();
        }
    }

    /**
     * Exchanges Identity Token for Bearer Token.
     */
    String getBearerToken(AuthChallenge challenge, List<String> scopes) throws IOException {
        if (challenge == null || !"Bearer".equalsIgnoreCase(challenge.getScheme())) {
            throw new IOException("Not a Bearer challenge");
        }

        String identityToken = authProvider.getAuthToken();
        if (identityToken == null || identityToken.isEmpty()) {
            throw new IOException("Identity token not available");
        }

        // Build token exchange request
        String tokenUrl = challenge.getRealm() + "?service=" + challenge.getService();
        StringJoiner scopeJoiner = new StringJoiner(" ");
        for (String scope : scopes) {
            scopeJoiner.add(scope);
        }
        if (scopeJoiner.length() > 0) {
            tokenUrl += "&scope=" + java.net.URLEncoder.encode(scopeJoiner.toString(), StandardCharsets.UTF_8);
        }

        HttpPost request = new HttpPost(tokenUrl);
        request.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + 
            java.util.Base64.getEncoder().encodeToString(
                ("oauth2accesstoken:" + identityToken).getBytes(StandardCharsets.UTF_8)));
        request.setHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        request.setEntity(new StringEntity("grant_type=refresh_token&service=" + 
            challenge.getService() + "&scope=" + java.net.URLEncoder.encode(scopeJoiner.toString(), StandardCharsets.UTF_8)));

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                JsonObject tokenResponse = JsonParser.parseString(json).getAsJsonObject();
                if (tokenResponse.has("token")) {
                    return tokenResponse.get("token").getAsString();
                }
            }
            throw new IOException("Failed to exchange token: " + response.getStatusLine().getStatusCode());
        }
    }

    void close() throws IOException {
        httpClient.close();
    }
}
