package com.salesforce.multicloudj.sts.aws;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AwsStsVerifierTest {

  private static final String IDENTITY =
      "https://sts.us-west-2.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15"
          + "&x-amz-date=20250101T000000Z&authorization=AWS4-HMAC-SHA256%20Credential%3Dabc"
          + "&x-target-resource=my-service";

  private static final String RESPONSE_XML =
      "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
          + "<GetCallerIdentityResult>"
          + "<Arn>arn:aws:iam::123456789012:user/Alice</Arn>"
          + "<UserId>AIDAEXAMPLE</UserId>"
          + "<Account>123456789012</Account>"
          + "</GetCallerIdentityResult>"
          + "</GetCallerIdentityResponse>";

  @Test
  void providerId() {
    Assertions.assertEquals("aws", new AwsStsVerifier().getProviderId());
  }

  @Test
  @SuppressWarnings("unchecked")
  void returnsCallerIdentityFromStsResponse() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn(RESPONSE_XML);
    when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);

    CallerIdentity identity =
        new AwsStsVerifier.Builder().build(httpClient).verifySignedAuthRequest(IDENTITY);

    Assertions.assertEquals("AIDAEXAMPLE", identity.getUserId());
    Assertions.assertEquals(
        "arn:aws:iam::123456789012:user/Alice", identity.getCloudResourceName());
    Assertions.assertEquals("123456789012", identity.getAccountId());
  }

  @Test
  void rejectsEmptySignedIdentity() {
    AwsStsVerifier verifier = new AwsStsVerifier.Builder().build(mock(HttpClient.class));
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier.verifySignedAuthRequest(""));
  }

  @Test
  @SuppressWarnings("unchecked")
  void nonOkStatusThrowsUnauthorized() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(403);
    when(response.body()).thenReturn("<Error><Code>AccessDenied</Code></Error>");
    when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);

    AwsStsVerifier verifier = new AwsStsVerifier.Builder().build(httpClient);
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
        new AwsStsVerifier.Builder().build(httpClient).verifySignedAuthRequest(IDENTITY, options);

    Assertions.assertEquals("123456789012", identity.getAccountId());
  }

  @Test
  void mismatchedExpectedCustomHeaderFails() {
    AwsStsVerifier verifier = new AwsStsVerifier.Builder().build(mock(HttpClient.class));
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeader("x-target-resource", "other").build();

    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier.verifySignedAuthRequest(IDENTITY, options));
  }

  @Test
  void missingExpectedCustomHeaderFails() {
    AwsStsVerifier verifier = new AwsStsVerifier.Builder().build(mock(HttpClient.class));
    ValidateOptions options =
        ValidateOptions.builder().withExpectedCustomHeader("x-absent", "value").build();

    Assertions.assertThrows(
        InvalidArgumentException.class, () -> verifier.verifySignedAuthRequest(IDENTITY, options));
  }

  @SuppressWarnings("unchecked")
  private static HttpClient okClient() throws IOException, InterruptedException {
    HttpClient httpClient = mock(HttpClient.class);
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn(RESPONSE_XML);
    when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);
    return httpClient;
  }
}
