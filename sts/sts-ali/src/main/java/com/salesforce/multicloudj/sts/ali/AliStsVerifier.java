package com.salesforce.multicloudj.sts.ali;

import com.google.auto.service.AutoService;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractStsVerifier;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Alibaba Cloud implementation of the STS verifier. It validates a signed identity by replaying the
 * presigned {@code GetCallerIdentity} request against Alibaba Cloud STS and returning the caller
 * identity parsed from the STS response.
 *
 * <p>The signed identity is a fully signed STS URL whose query string carries the action, version,
 * every signed parameter, and the RPC signature. Alibaba Cloud RPC signatures cover the HTTP
 * method, so the request is replayed with the same GET method used to sign it; STS then validates
 * the signature and returns the identity of the credentials that produced it.
 */
@AutoService(AbstractStsVerifier.class)
public class AliStsVerifier extends AbstractStsVerifier {
  private final HttpClient httpClient;

  public AliStsVerifier() {
    this(new Builder());
  }

  public AliStsVerifier(Builder builder) {
    super(builder);
    this.httpClient = buildHttpClient(builder);
  }

  /** Constructor that accepts a preconfigured HTTP client, used by tests. */
  public AliStsVerifier(Builder builder, HttpClient httpClient) {
    super(builder);
    this.httpClient = httpClient;
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  protected CallerIdentity validateSignedAuthRequest(
      String signedIdentity, ValidateOptions options) {
    if (signedIdentity == null || signedIdentity.isEmpty()) {
      throw new InvalidArgumentException("signedIdentity cannot be empty");
    }

    URI identityUri = URI.create(signedIdentity);
    Map<String, String> params = parseQuery(identityUri.getRawQuery());
    verifyExpectedCustomHeaders(params, options);

    HttpRequest request =
        HttpRequest.newBuilder(identityUri).GET().build();
    HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new UnknownException("failed to replay STS request", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UnknownException("interrupted while replaying STS request", e);
    }

    if (response.statusCode() != 200) {
      throw new UnAuthorizedException(
          "STS returned HTTP " + response.statusCode() + ": " + response.body());
    }

    return parseCallerIdentity(response.body());
  }

  private static void verifyExpectedCustomHeaders(
      Map<String, String> params, ValidateOptions options) {
    if (options == null) {
      return;
    }
    for (Map.Entry<String, String> expected : options.getExpectedCustomHeaders().entrySet()) {
      String actual = params.get(expected.getKey());
      if (actual == null) {
        throw new InvalidArgumentException(
            "expected custom header \"" + expected.getKey() + "\" not found in signed request");
      }
      if (!actual.equals(expected.getValue())) {
        throw new InvalidArgumentException(
            "custom header \"" + expected.getKey() + "\" has an unexpected value");
      }
    }
  }

  private static CallerIdentity parseCallerIdentity(String responseBody) {
    try {
      JsonObject json = JsonParser.parseString(responseBody).getAsJsonObject();
      String accountId = optString(json, "AccountId");
      String arn = optString(json, "Arn");
      String userId = optString(json, "UserId");
      if (accountId == null && arn == null && userId == null) {
        throw new UnknownException("STS response did not contain a caller identity");
      }
      return new CallerIdentity(userId, arn, accountId);
    } catch (SubstrateSdkException e) {
      throw e;
    } catch (Exception e) {
      throw new UnknownException("failed to parse STS response", e);
    }
  }

  private static String optString(JsonObject json, String field) {
    return json.has(field) && !json.get(field).isJsonNull() ? json.get(field).getAsString() : null;
  }

  private static Map<String, String> parseQuery(String rawQuery) {
    Map<String, String> params = new LinkedHashMap<>();
    if (rawQuery == null || rawQuery.isEmpty()) {
      return params;
    }
    for (String pair : rawQuery.split("&")) {
      int idx = pair.indexOf('=');
      if (idx < 0) {
        params.put(URLDecoder.decode(pair, StandardCharsets.UTF_8), "");
        continue;
      }
      String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
      String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
      params.put(key, value);
    }
    return params;
  }

  private static HttpClient buildHttpClient(Builder builder) {
    HttpClient.Builder clientBuilder = HttpClient.newBuilder();
    if (builder.getProxyEndpoint() != null) {
      URI proxy = builder.getProxyEndpoint();
      clientBuilder.proxy(
          ProxySelector.of(new InetSocketAddress(proxy.getHost(), proxy.getPort())));
    }
    return clientBuilder.build();
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    return ExceptionHandler.build(UnknownException.class, t);
  }

  public static class Builder extends AbstractStsVerifier.Builder<AliStsVerifier, Builder> {
    protected Builder() {
      providerId("ali");
    }

    @Override
    public Builder self() {
      return this;
    }

    /** Builds a verifier backed by the supplied HTTP client, used by tests. */
    public AliStsVerifier build(HttpClient httpClient) {
      return new AliStsVerifier(this, httpClient);
    }

    @Override
    public AliStsVerifier build() {
      return new AliStsVerifier(this);
    }
  }
}
