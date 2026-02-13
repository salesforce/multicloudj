package com.salesforce.multicloudj.registry.driver;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import org.apache.http.client.utils.URIBuilder;

/**
 * Handles OAuth2 Bearer Token exchange for OCI registries.
 * Exchanges identity tokens for registry-scoped bearer tokens.
 */
public class BearerTokenExchange {

  private final CloseableHttpClient httpClient;

  /**
   * Creates a new BearerTokenExchange instance.
   *
   * @param httpClient the HTTP client to use for requests
   */
  public BearerTokenExchange(CloseableHttpClient httpClient) {
    this.httpClient = httpClient;
  }

  /**
   * Exchanges an identity token for a registry-scoped bearer token.
   *
   * @param challenge the authentication challenge from AuthChallenge.discover()
   * @param identityToken the identity token (e.g., OAuth2 access token from GCP)
   * @param repository the repository to request access for
   * @param actions the actions to request (e.g., "pull", "push")
   * @return the bearer token for use in Authorization header
   * @throws IOException if the token exchange fails
   */
  public String getBearerToken(AuthChallenge challenge, String identityToken,
                               String repository, String... actions) throws IOException {
    if (challenge == null || !challenge.isBearer()) {
      throw new IllegalArgumentException("Bearer token exchange requires a Bearer challenge");
    }

    String realm = challenge.getRealm();
    if (StringUtils.isBlank(realm)) {
      throw new IOException("Bearer challenge missing realm");
    }

    // Build token request URL using URIBuilder for proper encoding
    URI tokenUri = buildTokenUri(realm, challenge, repository, actions);

    HttpGet request = new HttpGet(tokenUri);
    // Use identity token as Bearer auth for the token endpoint
    request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + identityToken);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        String errorBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        throw new IOException("Token exchange failed: HTTP " + statusCode + " - " + errorBody);
      }

      String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
      
      JsonObject json;
      try {
        json = JsonParser.parseString(responseBody).getAsJsonObject();
      } catch (JsonSyntaxException e) {
        throw new IOException("Invalid JSON response from token endpoint: " + responseBody, e);
      }

      // Token can be in "token" (Docker Hub, AWS ECR) or "access_token" (GCP Artifact Registry) field
      if (json.has("token") && !json.get("token").isJsonNull()) {
        return json.get("token").getAsString();
      } else if (json.has("access_token") && !json.get("access_token").isJsonNull()) {
        return json.get("access_token").getAsString();
      }

      throw new IOException("Token response missing token field");
    }
  }

  private URI buildTokenUri(String realm, AuthChallenge challenge, String repository, String[] actions)
          throws IOException {
    try {
      URIBuilder uriBuilder = new URIBuilder(realm);
      
      if (challenge.getService() != null) {
        uriBuilder.addParameter("service", challenge.getService());
      }

      // Build scope: repository:<name>:<actions>
      if (repository != null && actions.length > 0) {
        String scope = "repository:" + repository + ":" + String.join(",", actions);
        uriBuilder.addParameter("scope", scope);
      } else if (challenge.getScope() != null) {
        // Fallback: use scope from the original WWW-Authenticate header
        uriBuilder.addParameter("scope", challenge.getScope());
      }

      return uriBuilder.build();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid token endpoint URL: " + realm, e);
    }
  }
}
