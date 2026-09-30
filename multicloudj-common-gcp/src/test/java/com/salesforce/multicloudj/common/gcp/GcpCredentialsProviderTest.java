package com.salesforce.multicloudj.common.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class GcpCredentialsProviderTest {

  @Test
  void testGetCredentialsWithNullOverrider() {
    Credentials credentials = GcpCredentialsProvider.getCredentials(null);
    assertNull(credentials, "Credentials should be null when overrider is null");
  }

  @Test
  void testGetCredentialsWithNullType() {
    CredentialsOverrider overrider = new CredentialsOverrider.Builder(null).build();
    Credentials credentials = GcpCredentialsProvider.getCredentials(overrider);
    assertNull(credentials, "Credentials should be null when type is null");
  }

  @Test
  void testGetCredentialsWithSessionType() {
    // Arrange
    String testToken = "test-security-token-12345";
    StsCredentials stsCredentials =
        new StsCredentials("test-access-key-id", "test-access-key-secret", testToken);

    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(stsCredentials)
            .build();

    // Act
    Credentials credentials = GcpCredentialsProvider.getCredentials(overrider);

    // Assert
    assertNotNull(credentials, "Credentials should not be null");
    assertTrue(credentials instanceof GoogleCredentials, "Credentials should be GoogleCredentials");
  }

  @Test
  void testGetCredentialsWithSessionTypeAndNullSessionCredentials() {
    // Arrange
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION).build();

    // Act & Assert - Should throw NullPointerException when sessionCredentials is null
    assertThrows(
        NullPointerException.class,
        () -> {
          GcpCredentialsProvider.getCredentials(overrider);
        },
        "NullPointerException expected when sessionCredentials is null");
  }

  @Test
  void testGetCredentialsWithAssumeRoleWebIdentityType() {
    // Arrange
    String audience =
        "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/"
            + "test-pool/providers/test-provider";
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole(audience)
            .withWebIdentityTokenSupplier(() -> "mock-web-identity-token")
            .build();

    // Act
    Credentials credentials = GcpCredentialsProvider.getCredentials(overrider);

    // Assert
    assertNotNull(credentials, "Credentials should not be null");
    assertTrue(
        credentials instanceof IdentityPoolCredentials,
        "Credentials should be IdentityPoolCredentials");
    assertEquals(
        audience,
        ((IdentityPoolCredentials) credentials).getAudience(),
        "Audience should be sourced from the overrider role");
  }

  @Test
  void testWebIdentityTokenSupplierInvokedOnTokenRefresh() throws Exception {
    // Arrange - the supplier drives the subject token on each refresh, so that
    // IdentityPoolCredentials always sees a fresh subject token.
    AtomicInteger invocationCount = new AtomicInteger(0);
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("//iam.googleapis.com/projects/123/locations/global/"
                + "workloadIdentityPools/test-pool/providers/test-provider")
            .withWebIdentityTokenSupplier(
                () -> {
                  invocationCount.incrementAndGet();
                  return "mock-web-identity-token";
                })
            .build();

    IdentityPoolCredentials credentials =
        (IdentityPoolCredentials) GcpCredentialsProvider.getCredentials(overrider);
    assertNotNull(credentials);

    // Act - retrieving the subject token drives the supplier.
    String subjectToken = credentials.retrieveSubjectToken();

    // Assert - the supplier feeds the retrieved subject token.
    assertEquals("mock-web-identity-token", subjectToken);
    assertTrue(
        invocationCount.get() >= 1, "Supplier should be invoked to retrieve subject token");
  }

  @Test
  void testJwtSubjectTokenTypeInferredForJwtToken() {
    // Arrange - a raw JWT-style token should be exchanged as a JWT subject token.
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("//iam.googleapis.com/projects/123/locations/global/"
                + "workloadIdentityPools/test-pool/providers/test-provider")
            .withWebIdentityTokenSupplier(() -> "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjMifQ.sig")
            .build();

    // Act
    IdentityPoolCredentials credentials =
        (IdentityPoolCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Assert
    assertNotNull(credentials);
    assertEquals(
        "urn:ietf:params:oauth:token-type:jwt",
        credentials.getSubjectTokenType(),
        "A JWT token should be exchanged with the JWT subject token type");
  }

  @Test
  void testAwsSubjectTokenTypeInferredForUrlEncodedSignedRequest() {
    // Arrange - a pre-signed GetCallerIdentity token is a URL-encoded JSON envelope, so it starts
    // with an encoded opening brace ("%7B").
    String signedRequest =
        "%7B%22url%22%3A%22https%3A%2F%2Fsts.amazonaws.com%22%2C%22method%22%3A%22POST%22%7D";
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("//iam.googleapis.com/projects/123/locations/global/"
                + "workloadIdentityPools/test-pool/providers/test-provider")
            .withWebIdentityTokenSupplier(() -> signedRequest)
            .build();

    // Act
    IdentityPoolCredentials credentials =
        (IdentityPoolCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Assert
    assertNotNull(credentials);
    assertEquals(
        "urn:ietf:params:aws:token-type:aws4_request",
        credentials.getSubjectTokenType(),
        "A URL-encoded signed request should be exchanged with the AWS4 subject token type");
  }

  @Test
  void testAwsSubjectTokenTypeInferredForUnencodedSignedRequest() {
    // Arrange - a signed GetCallerIdentity envelope supplied as raw (unencoded) JSON starts with a
    // literal opening brace.
    String signedRequest = "{\"url\":\"https://sts.amazonaws.com\",\"method\":\"POST\"}";
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("//iam.googleapis.com/projects/123/locations/global/"
                + "workloadIdentityPools/test-pool/providers/test-provider")
            .withWebIdentityTokenSupplier(() -> signedRequest)
            .build();

    // Act
    IdentityPoolCredentials credentials =
        (IdentityPoolCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Assert
    assertNotNull(credentials);
    assertEquals(
        "urn:ietf:params:aws:token-type:aws4_request",
        credentials.getSubjectTokenType(),
        "An unencoded signed request should be exchanged with the AWS4 subject token type");
  }

  @Test
  void testGetCredentialsWithAssumeRoleWebIdentityAndNullTokenSupplier() {
    // Arrange
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole("//iam.googleapis.com/projects/123/locations/global/"
                + "workloadIdentityPools/test-pool/providers/test-provider")
            .build();

    // Act & Assert
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcpCredentialsProvider.getCredentials(overrider),
            "IllegalArgumentException expected when webIdentityTokenSupplier is null");
    assertTrue(ex.getMessage().contains("webIdentityTokenSupplier"));
  }
}
