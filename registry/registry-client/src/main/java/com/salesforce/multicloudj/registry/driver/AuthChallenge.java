package com.salesforce.multicloudj.registry.driver;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses HTTP WWW-Authenticate header to determine authentication requirements.
 * Supports both Basic and Bearer authentication schemes.
 * Also supports anonymous access when no authentication is required.
 */
@Getter
public final class AuthChallenge {

  private static final String ANONYMOUS_SCHEME = "Anonymous";
  private static final String BEARER_SCHEME = "Bearer";
  private static final Pattern SCHEME_PATTERN = Pattern.compile("^(Basic|Bearer)\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern PARAM_PATTERN = Pattern.compile("(\\w+)=\"([^\"]*)\"");

  /** The authentication scheme (Basic, Bearer, or Anonymous). */
  private final String scheme;

  /** The realm URL for token exchange (Bearer auth). */
  private final String realm;

  /** The service identifier. */
  private final String service;

  /** The requested scope. */
  private final String scope;

  private AuthChallenge(String scheme, String realm, String service, String scope) {
    this.scheme = scheme;
    this.realm = realm;
    this.service = service;
    this.scope = scope;
  }

  /**
   * Creates an AuthChallenge representing anonymous access (no auth required).
   *
   * @return an anonymous AuthChallenge
   */
  public static AuthChallenge anonymous() {
    return new AuthChallenge(ANONYMOUS_SCHEME, null, null, null);
  }

  /**
   * Discovers authentication requirements by pinging the registry.
   * Sends GET /v2/ and parses the WWW-Authenticate header from 401 response.
   *
   * @param httpClient the HTTP client to use for the request
   * @param registryEndpoint the registry base URL
   * @return AuthChallenge describing the authentication requirements (including anonymous)
   * @throws IOException if the request fails
   */
  public static AuthChallenge discover(CloseableHttpClient httpClient, String registryEndpoint) throws IOException {
    String url = registryEndpoint + "/v2/";
    HttpGet request = new HttpGet(url);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == HttpStatus.SC_OK) {
        // No authentication required (anonymous access)
        return anonymous();
      }

      if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
        // Parse WWW-Authenticate header
        if (response.containsHeader(HttpHeaders.WWW_AUTHENTICATE)) {
          String authHeader = response.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE).getValue();
          return parse(authHeader);
        }
        throw new IOException("Registry returned 401 without WWW-Authenticate header");
      }
      throw new IOException("Unexpected response from registry ping: HTTP " + statusCode);
    }
  }

  /**
   * Parses a WWW-Authenticate header value.
   *
   * @param header the WWW-Authenticate header value
   * @return parsed AuthChallenge
   * @throws IllegalArgumentException if the header cannot be parsed
   */
  public static AuthChallenge parse(String header) {
    if (StringUtils.isBlank(header)) {
      throw new IllegalArgumentException("WWW-Authenticate header is empty");
    }

    Matcher schemeMatcher = SCHEME_PATTERN.matcher(header);
    if (!schemeMatcher.find()) {
      throw new UnsupportedOperationException("Unsupported authentication scheme in: " + header);
    }

    String scheme = schemeMatcher.group(1);
    String paramsPart = header.substring(schemeMatcher.end());

    Map<String, String> params = new HashMap<>();
    Matcher paramMatcher = PARAM_PATTERN.matcher(paramsPart);
    while (paramMatcher.find()) {
      params.put(paramMatcher.group(1).toLowerCase(), paramMatcher.group(2));
    }

    return new AuthChallenge(
        scheme,
        params.get("realm"),
        params.get("service"),
        params.get("scope")
    );
  }

  /**
   * Returns true if this is a Bearer authentication challenge.
   */
  public boolean isBearer() {
    return BEARER_SCHEME.equalsIgnoreCase(scheme);
  }
}
