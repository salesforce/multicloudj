package com.salesforce.multicloudj.registry.aws;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AuthStrippingInterceptorTest {

  private static final String REGISTRY_ENDPOINT =
      "https://123456789012.dkr.ecr.us-east-1.amazonaws.com";
  private static final String REGISTRY_HOST = "123456789012.dkr.ecr.us-east-1.amazonaws.com";
  private static final String EXAMPLE_ENDPOINT_HOST = "registry.example.com";
  private static final String S3_HOST = "s3.amazonaws.com";
  private static final String GET = "GET";
  private static final String BEARER_TOKEN = "Bearer token123";
  private static final String PATH_BLOB = "/v2/repo/blobs/sha256:abc";

  private AuthStrippingInterceptor interceptor;

  @BeforeEach
  void setUp() {
    interceptor = new AuthStrippingInterceptor(REGISTRY_ENDPOINT);
  }

  @Test
  void testProcess_KeepsAuthHeader_WhenTargetIsRegistryHost() {
    HttpRequest request = requestWithAuth(PATH_BLOB);
    HttpContext context = contextWithHost(REGISTRY_HOST);

    interceptor.process(request, context);

    assertTrue(request.containsHeader(HttpHeaders.AUTHORIZATION));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "s3.amazonaws.com",
        "prod-us-east-1-starport-layer-bucket.s3.us-east-1.amazonaws.com"
      })
  void testProcess_StripsAuthHeader_WhenTargetIsExternalHost(String externalHost) {
    HttpRequest request = requestWithAuth(PATH_BLOB);
    HttpContext context = contextWithHost(externalHost);

    interceptor.process(request, context);

    assertFalse(request.containsHeader(HttpHeaders.AUTHORIZATION));
  }

  @Test
  void testProcess_NoOp_WhenTargetHostIsNull() {
    HttpRequest request = requestWithAuth(PATH_BLOB);
    HttpContext context = new BasicHttpContext();

    interceptor.process(request, context);

    assertTrue(request.containsHeader(HttpHeaders.AUTHORIZATION));
  }

  @Test
  void testProcess_CaseInsensitiveHostComparison() {
    HttpRequest request = requestWithAuth(PATH_BLOB);
    HttpContext context = contextWithHost(REGISTRY_HOST.toUpperCase());

    interceptor.process(request, context);

    assertTrue(request.containsHeader(HttpHeaders.AUTHORIZATION));
  }

  @Test
  void testProcess_NoAuthHeader_NoError() {
    HttpRequest request = new BasicHttpRequest(GET, PATH_BLOB);
    HttpContext context = contextWithHost(S3_HOST);

    interceptor.process(request, context);

    assertFalse(request.containsHeader(HttpHeaders.AUTHORIZATION));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://registry.example.com/",
        "https://registry.example.com:443",
        "https://registry.example.com:443/v2/",
        "http://registry.example.com"
      })
  void testExtractHost_CorrectlyParsesVariousUrlFormats(String endpoint) {
    AuthStrippingInterceptor localInterceptor = new AuthStrippingInterceptor(endpoint);
    HttpRequest request = requestWithAuth("/v2/");
    HttpContext context = contextWithHost(EXAMPLE_ENDPOINT_HOST);

    localInterceptor.process(request, context);

    assertTrue(
        request.containsHeader(HttpHeaders.AUTHORIZATION),
        "Should correctly extract host from: " + endpoint);
  }

  private HttpRequest requestWithAuth(String path) {
    HttpRequest request = new BasicHttpRequest(GET, path);
    request.setHeader(HttpHeaders.AUTHORIZATION, BEARER_TOKEN);
    return request;
  }

  private HttpContext contextWithHost(String host) {
    HttpContext context = new BasicHttpContext();
    context.setAttribute(HttpClientContext.HTTP_TARGET_HOST, new HttpHost(host, 443, "https"));
    return context;
  }
}
