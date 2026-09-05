package com.salesforce.multicloudj.sts.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractStsVerifier;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.io.IOException;
import java.io.StringReader;
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
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * AWS implementation of the STS verifier. It validates a signed identity by replaying the presigned
 * {@code GetCallerIdentity} request against AWS STS and returning the caller identity parsed from
 * the STS response.
 *
 * <p>The signed identity is a URL rooted at the STS endpoint whose query string carries the STS
 * action, version, and every signed request header. Replaying that request lets AWS STS validate
 * the SigV4 signature and return the identity of the credentials that produced it.
 */
@AutoService(AbstractStsVerifier.class)
public class AwsStsVerifier extends AbstractStsVerifier {
  private static final String ACTION_PARAM = "Action";
  private static final String VERSION_PARAM = "Version";
  private static final String DEFAULT_API_ACTION_NAME = "GetCallerIdentity";
  private static final String DEFAULT_API_VERSION = "2011-06-15";

  private final HttpClient httpClient;
  private final URI endpointOverride;

  public AwsStsVerifier() {
    this(new Builder());
  }

  public AwsStsVerifier(Builder builder) {
    super(builder);
    this.endpointOverride = builder.getEndpoint();
    this.httpClient = buildHttpClient(builder);
  }

  /** Constructor that accepts a preconfigured HTTP client, used by tests. */
  public AwsStsVerifier(Builder builder, HttpClient httpClient) {
    super(builder);
    this.endpointOverride = builder.getEndpoint();
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

    // The signed request carries its headers as query parameters. Custom headers are validated
    // against those signed values before the request is replayed.
    verifyExpectedCustomHeaders(params, options);

    // Action=GetCallerIdentity&Version=2011-06-15 is the AWS Query-form body that was signed. It is
    // reconstructed here so the replayed POST matches what AWS STS expects.
    String action = params.getOrDefault(ACTION_PARAM, DEFAULT_API_ACTION_NAME);
    String version = params.getOrDefault(VERSION_PARAM, DEFAULT_API_VERSION);
    String body = ACTION_PARAM + "=" + action + "&" + VERSION_PARAM + "=" + version;

    URI replayTarget = endpointOverride != null ? endpointOverride : stripQuery(identityUri);
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(replayTarget).POST(HttpRequest.BodyPublishers.ofString(body));
    for (Map.Entry<String, String> param : params.entrySet()) {
      if (ACTION_PARAM.equals(param.getKey()) || VERSION_PARAM.equals(param.getKey())) {
        continue;
      }
      // The JDK HTTP client rejects restricted headers (host, content-length, ...). host was not
      // signed into the identity, so any restricted name here is unexpected and simply skipped.
      try {
        requestBuilder.header(param.getKey(), param.getValue());
      } catch (IllegalArgumentException restrictedHeader) {
        // Skip headers the HTTP client refuses to set.
      }
    }

    HttpResponse<String> response;
    try {
      response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
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
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      // Harden the XML parser against external entity attacks; the STS response has no DTD.
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
      factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
      factory.setXIncludeAware(false);
      factory.setExpandEntityReferences(false);
      Document document =
          factory.newDocumentBuilder().parse(new InputSource(new StringReader(responseBody)));
      String arn = elementText(document, "Arn");
      String userId = elementText(document, "UserId");
      String account = elementText(document, "Account");
      if (arn == null && userId == null && account == null) {
        throw new UnknownException("STS response did not contain a caller identity");
      }
      return new CallerIdentity(userId, arn, account);
    } catch (SubstrateSdkException e) {
      throw e;
    } catch (Exception e) {
      throw new UnknownException("failed to parse STS response", e);
    }
  }

  private static String elementText(Document document, String tagName) {
    NodeList nodes = document.getElementsByTagName(tagName);
    return nodes.getLength() > 0 ? nodes.item(0).getTextContent() : null;
  }

  private static URI stripQuery(URI uri) {
    String base = uri.toString();
    int queryIndex = base.indexOf('?');
    return queryIndex >= 0 ? URI.create(base.substring(0, queryIndex)) : uri;
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

  public static class Builder extends AbstractStsVerifier.Builder<AwsStsVerifier, Builder> {
    protected Builder() {
      providerId("aws");
    }

    @Override
    public Builder self() {
      return this;
    }

    /** Builds a verifier backed by the supplied HTTP client, used by tests. */
    public AwsStsVerifier build(HttpClient httpClient) {
      return new AwsStsVerifier(this, httpClient);
    }

    @Override
    public AwsStsVerifier build() {
      return new AwsStsVerifier(this);
    }
  }
}
