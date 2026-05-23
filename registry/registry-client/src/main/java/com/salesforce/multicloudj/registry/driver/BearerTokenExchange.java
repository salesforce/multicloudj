package com.salesforce.multicloudj.registry.driver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * Handles OAuth2 Bearer Token exchange for OCI registries. Exchanges identity tokens for
 * registry-scoped bearer tokens.
 */
public class BearerTokenExchange {

  private static final String TOKEN_FIELD = "token";
  private static final String ACCESS_TOKEN_FIELD = "access_token";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
   * @throws InvalidArgumentException if challenge is not Bearer or realm is missing
   * @throws UnAuthorizedException if token exchange fails with non-200 status
   * @throws UnknownException if the request fails or response is invalid
   */
  public String getBearerToken(
      AuthChallenge challenge, String identityToken, String repository, String... actions) {
    if (challenge == null || !challenge.isBearer()) {
      throw new InvalidArgumentException("Bearer token exchange requires a Bearer challenge");
    }

    String realm = challenge.getRealm();
    if (StringUtils.isBlank(realm)) {
      throw new InvalidArgumentException("Bearer challenge missing realm");
    }

    URI tokenUri = buildTokenUri(realm, challenge, repository, actions);
    HttpGet request = new HttpGet(tokenUri);
    request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + identityToken);
    try {
      return executeTokenRequest(request);
    } catch (IOException e) {
      throw new UnknownException("Token exchange request failed", e);
    }
  }

  private String executeTokenRequest(HttpGet request) throws IOException {
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        String errorBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        throw new UnAuthorizedException(
            "Token exchange failed: HTTP " + statusCode + " - " + errorBody);
      }

      String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

      ObjectNode json;
      try {
        json = (ObjectNode) OBJECT_MAPPER.readTree(responseBody);
      } catch (JsonProcessingException e) {
        throw new UnknownException(
            "Invalid JSON response from token endpoint: " + responseBody, e);
      }

      // Token field is "token" (Docker Hub, AWS ECR) or "access_token" (GCP Artifact Registry)
      if (json.has(TOKEN_FIELD) && !json.get(TOKEN_FIELD).isNull()) {
        return json.get(TOKEN_FIELD).asText();
      } else if (json.has(ACCESS_TOKEN_FIELD) && !json.get(ACCESS_TOKEN_FIELD).isNull()) {
        return json.get(ACCESS_TOKEN_FIELD).asText();
      }

      throw new UnknownException("Token response missing token field");
    }
  }

  private URI buildTokenUri(
      String realm, AuthChallenge challenge, String repository, String[] actions) {
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
      throw new InvalidArgumentException("Invalid token endpoint URL: " + realm, e);
    }
  }
}
