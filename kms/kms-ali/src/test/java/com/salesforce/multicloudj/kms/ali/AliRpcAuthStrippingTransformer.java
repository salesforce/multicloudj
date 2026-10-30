package com.salesforce.multicloudj.kms.ali;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.MultiValuePattern;
import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites recorded KMS requests so they replay and carry no credentials. KMS RPC requests are
 * signed in the query string, so each request has a fresh nonce, timestamp and signature plus the
 * caller's access key id and security token. Those parameters are dropped from the recorded
 * matcher, and the request is matched on its remaining query parameters instead.
 */
public class AliRpcAuthStrippingTransformer extends StubMappingTransformer {

  private static final Set<String> PER_REQUEST_PARAMS =
      Set.of("AccessKeyId", "SecurityToken", "Signature", "SignatureNonce", "Timestamp");

  // Added by the shared recording setup; the parameter count depends on whether a session token
  // was sent, which differs between recording and credential-free replay.
  private static final String QUERY_PARAM_COUNT_HEADER = "X-Query-Param-Count";

  @Override
  public String getName() {
    return "ali-rpc-auth-stripping-transformer";
  }

  @Override
  public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
    RequestPattern request = stubMapping.getRequest();
    RequestPatternBuilder rebuilt;
    String url = request.getUrl();
    if (url != null) {
      int queryStart = url.indexOf('?');
      rebuilt =
          RequestPatternBuilder.newRequestPattern(
              request.getMethod(),
              urlPathEqualTo(queryStart < 0 ? url : url.substring(0, queryStart)));
      if (queryStart >= 0) {
        for (String pair : url.substring(queryStart + 1).split("&")) {
          int equals = pair.indexOf('=');
          String name = decode(equals < 0 ? pair : pair.substring(0, equals));
          if (!name.isEmpty() && !PER_REQUEST_PARAMS.contains(name)) {
            rebuilt.withQueryParam(
                name, equalTo(equals < 0 ? "" : decode(pair.substring(equals + 1))));
          }
        }
      }
    } else {
      rebuilt =
          RequestPatternBuilder.newRequestPattern(request.getMethod(), request.getUrlMatcher());
    }
    // The recorder usually stores the query as separate parameter matchers rather than in the URL.
    if (request.getQueryParameters() != null) {
      for (Map.Entry<String, MultiValuePattern> param : request.getQueryParameters().entrySet()) {
        if (!PER_REQUEST_PARAMS.contains(param.getKey())) {
          rebuilt.withQueryParam(param.getKey(), param.getValue());
        }
      }
    }
    if (request.getHeaders() != null) {
      for (Map.Entry<String, MultiValuePattern> header : request.getHeaders().entrySet()) {
        if (!QUERY_PARAM_COUNT_HEADER.equalsIgnoreCase(header.getKey())) {
          rebuilt.withHeader(header.getKey(), header.getValue());
        }
      }
    }
    if (request.getBodyPatterns() != null) {
      for (ContentPattern<?> body : request.getBodyPatterns()) {
        rebuilt.withRequestBody(body);
      }
    }
    stubMapping.setRequest(rebuilt.build());
    return stubMapping;
  }

  private static String decode(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }
}
