package com.salesforce.multicloudj.sts.ali;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AliStsVerifierTest {

  private static final String IDENTITY =
      "https://sts.cn-hangzhou.aliyuncs.com/?Action=GetCallerIdentity&Version=2015-04-01"
          + "&Format=JSON&Signature=abc123&x-target-resource=my-service";

  private static final String RESPONSE_JSON =
      "{\"AccountId\":\"123456789012\","
          + "\"UserId\":\"29417383920\","
          + "\"Arn\":\"acs:ram::123456789012:user/Alice\","
          + "\"RequestId\":\"req-1\"}";

  @Test
  void providerId() {
    Assertions.assertEquals("ali", new AliStsVerifier().getProviderId());
  }

  @Test
  @SuppressWarnings("unchecked")
  void returnsCallerIdentityFromStsResponse() throws Exception {
    HttpClient httpClient = okClient();

    CallerIdentity identity =
        new AliStsVerifier.Builder().build(httpClient).verifySignedAuthRequest(IDENTITY);

    Assertions.assertEquals("29417383920", identity.getUserId());
    Assertions.assertEquals("acs:ram::123456789012:user/Alice", identity.getCloudResourceName());
    Assertions.assertEquals("123456789012", identity.getAccountId());
  }

  @Test
  void rejectsEmptySignedIdentity() {
    AliStsVerifier verifier = new AliStsVerifier.Builder().build(mock(HttpClient.class));
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier.verifySignedAuthRequest(""));
  }

  @Test
  @SuppressWarnings("unchecked")
  void nonOkStatusThrowsUnauthorized() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(400);
    when(response.body()).thenReturn("{\"Code\":\"SignatureDoesNotMatch\"}");
    when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);

    AliStsVerifier verifier = new AliStsVerifier.Builder().build(httpClient);
    Assertions.assertThrows(
        UnAuthorizedException.class, () -> verifier.verifySignedAuthRequest(IDENTITY));
  }

  @Test
  void matchingExpectedCustomHeaderPasses() throws Exception {
    HttpClient httpClient = okClient();
    ValidateOptions options =
        ValidateOptions.builder()
            .withExpectedCustomHeader("x-target-resource", "my-service")
            .build();

    CallerIdentity identity =
        new AliStsVerifier.Builder().build(httpClient).verifySignedAuthRequest(IDENTITY, options);

    Assertions.assertEquals("123456789012", identity.getAccountId());
  }

  @Test
  void mismatchedExpectedCustomHeaderFails() {
    AliStsVerifier verifier = new AliStsVerifier.Builder().build(mock(HttpClient.class));
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeader("x-target-resource", "other").build();

    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier.verifySignedAuthRequest(IDENTITY, options));
  }

  @Test
  void buildWithProxyCreatesRealClient() {
    AliStsVerifier verifier =
        new AliStsVerifier.Builder()
            .withRegion("cn-hangzhou")
            .withProxyEndpoint(URI.create("http://localhost:8888"))
            .build();
    Assertions.assertEquals("ali", verifier.getProviderId());
    Assertions.assertNotNull(verifier.builder());
  }

  @Test
  void mapExceptionWrapsAsUnknown() {
    SubstrateSdkException mapped =
        new AliStsVerifier().mapException(new RuntimeException("boom"));
    Assertions.assertInstanceOf(UnknownException.class, mapped);
  }

  @Test
  @SuppressWarnings("unchecked")
  void ioFailureWrappedAsUnknown() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    when(httpClient.send(any(), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("connection reset"));

    AliStsVerifier verifier = new AliStsVerifier.Builder().build(httpClient);
    Assertions.assertThrows(
        UnknownException.class, () -> verifier.verifySignedAuthRequest(IDENTITY));
  }

  @Test
  @SuppressWarnings("unchecked")
  void responseWithoutIdentityThrowsUnknown() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn("{\"RequestId\":\"req-1\"}");
    when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);

    AliStsVerifier verifier = new AliStsVerifier.Builder().build(httpClient);
    Assertions.assertThrows(
        UnknownException.class, () -> verifier.verifySignedAuthRequest(IDENTITY));
  }

  @Test
  @SuppressWarnings("unchecked")
  void malformedJsonThrowsUnknown() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn("not json at all");
    when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);

    AliStsVerifier verifier = new AliStsVerifier.Builder().build(httpClient);
    Assertions.assertThrows(
        UnknownException.class, () -> verifier.verifySignedAuthRequest(IDENTITY));
  }

  @SuppressWarnings("unchecked")
  private static HttpClient okClient() throws IOException, InterruptedException {
    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn(RESPONSE_JSON);
    when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);
    return httpClient;
  }
}
