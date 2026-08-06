package com.salesforce.multicloudj.common.gcp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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
  void testSessionCredentialsByValueWithoutExpirationKeepExistingType() {
    // Arrange
    CredentialsOverrider overrider =
        sessionOverrider(new StsCredentials("key-id", "key-secret", "test-token"));

    // Act
    Credentials credentials = GcpCredentialsProvider.getCredentials(overrider);

    // Assert - callers that pass no expiration must keep the credentials they have always had.
    assertEquals(
        GoogleCredentials.class,
        credentials.getClass(),
        "Session credentials supplied by value should stay plain GoogleCredentials");
    AccessToken accessToken = ((GoogleCredentials) credentials).getAccessToken();
    assertEquals("test-token", accessToken.getTokenValue());
    assertNull(accessToken.getExpirationTime(), "No expiration was declared, so none is invented");
    assertDoesNotThrow(
        ((GoogleCredentials) credentials)::refreshIfExpired,
        "A token with no expiration is treated as fresh and is never renewed");
  }

  @Test
  void testSessionCredentialsByValueIgnoreTheDeclaredExpiration() throws Exception {
    // Arrange
    Instant expiration = Instant.now().plus(Duration.ofHours(1));
    CredentialsOverrider overrider =
        sessionOverrider(new StsCredentials("key-id", "key-secret", "test-token", expiration));

    // Act
    Credentials credentials = GcpCredentialsProvider.getCredentials(overrider);

    // Assert - the expiration only schedules renewal, and credentials supplied by value have no
    // renewal path, so declaring one must leave this path exactly as it is without one.
    assertEquals(GoogleCredentials.class, credentials.getClass());
    AccessToken accessToken = ((GoogleCredentials) credentials).getAccessToken();
    assertEquals("test-token", accessToken.getTokenValue());
    assertNull(accessToken.getExpirationTime());
    assertDoesNotThrow(
        ((GoogleCredentials) credentials)::refreshIfExpired,
        "Declaring an expiration must not put the credentials on a renewal path they lack");
    assertEquals(
        List.of("Bearer test-token"),
        credentials
            .getRequestMetadata(URI.create("https://storage.googleapis.com"))
            .get("Authorization"));
  }

  @Test
  void testSessionCredentialsByValueWithAPastExpirationStillWork() {
    // Arrange
    Instant expiration = Instant.now().minus(Duration.ofHours(1));
    CredentialsOverrider overrider =
        sessionOverrider(new StsCredentials("key-id", "key-secret", "test-token", expiration));

    // Act
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Assert - the service is the authority on whether a token still works, so a stale declared
    // expiration must not fail the call before the service has been asked.
    assertNull(credentials.getAccessToken().getExpirationTime());
    assertDoesNotThrow(credentials::refreshIfExpired);
    assertEquals("test-token", credentials.getAccessToken().getTokenValue());
  }

  @Test
  void testSessionCredentialsSupplierProducesRefreshableCredentials() {
    // Arrange
    CredentialsOverrider overrider =
        sessionSupplierOverrider(() -> sessionCredentials("test-token", Duration.ofHours(1)));

    // Act
    Credentials credentials = GcpCredentialsProvider.getCredentials(overrider);

    // Assert - the call sites hand the result to StorageOptions and some cast it to
    // GoogleCredentials, so the refreshable form has to remain a GoogleCredentials.
    assertInstanceOf(RefreshableSessionCredentials.class, credentials);
    assertInstanceOf(GoogleCredentials.class, credentials);
  }

  @Test
  void testScopingRefreshableSessionCredentialsKeepsThemRefreshable() {
    // Arrange - callers scope the credentials before handing them to a client, which must not
    // trade the refreshable credentials for a fixed set.
    CredentialsOverrider overrider =
        sessionSupplierOverrider(() -> sessionCredentials("test-token", Duration.ofHours(1)));
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Act
    GoogleCredentials scoped =
        credentials.createScoped(List.of("https://www.googleapis.com/auth/cloud-platform"));

    // Assert
    assertInstanceOf(RefreshableSessionCredentials.class, scoped);
  }

  @Test
  void testSessionCredentialsSupplierIsInvokedLazily() throws Exception {
    // Arrange
    AtomicInteger invocations = new AtomicInteger(0);
    CredentialsOverrider overrider =
        sessionSupplierOverrider(
            () -> {
              invocations.incrementAndGet();
              return sessionCredentials("test-token", Duration.ofHours(1));
            });

    // Act
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Assert - building a client must not reach out for credentials.
    assertEquals(0, invocations.get(), "Supplier should not run while building the credentials");
    assertNull(credentials.getAccessToken(), "No token should exist before one is needed");

    // Act - the first time a token is needed the supplier drives it.
    credentials.refreshIfExpired();

    // Assert
    assertEquals(1, invocations.get(), "Supplier should run when a token is first needed");
    assertEquals("test-token", credentials.getAccessToken().getTokenValue());
  }

  @Test
  void testExpiredTokenIsRenewedFromSessionCredentialsSupplier() throws Exception {
    // Arrange - the first credentials handed out have already lapsed, the next ones are valid.
    AtomicInteger invocations = new AtomicInteger(0);
    CredentialsOverrider overrider =
        sessionSupplierOverrider(
            () ->
                invocations.incrementAndGet() == 1
                    ? sessionCredentials("lapsed-token", Duration.ofMinutes(-5))
                    : sessionCredentials("renewed-token", Duration.ofHours(1)));
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    credentials.refreshIfExpired();
    assertEquals(1, invocations.get());
    assertEquals("lapsed-token", credentials.getAccessToken().getTokenValue());

    // Act - the held token has lapsed, so asking for a usable token goes back to the supplier once
    // the minimum interval between supplier invocations has passed.
    Thread.sleep(RefreshableSessionCredentials.MINIMUM_REFRESH_INTERVAL.toMillis() + 100);
    credentials.refreshIfExpired();

    // Assert - the renewed token is the one now presented to the service.
    assertEquals(2, invocations.get(), "A lapsed token should send the provider to the supplier");
    assertEquals("renewed-token", credentials.getAccessToken().getTokenValue());
    Map<String, List<String>> metadata =
        credentials.getRequestMetadata(URI.create("https://storage.googleapis.com"));
    assertEquals(List.of("Bearer renewed-token"), metadata.get("Authorization"));

    // Act & Assert - a token that is still valid is reused rather than renewed.
    credentials.refreshIfExpired();
    assertEquals(2, invocations.get(), "A valid token should be reused");
  }

  @Test
  void testAlreadyExpiredSuppliedCredentialsDoNotInvokeTheSupplierOnEveryRefresh()
      throws Exception {
    // Arrange - the supplier only ever hands back credentials that have already lapsed, which the
    // auth library asks to renew on every single request.
    AtomicInteger invocations = new AtomicInteger(0);
    CredentialsOverrider overrider =
        sessionSupplierOverrider(
            () -> {
              invocations.incrementAndGet();
              return sessionCredentials("lapsed-token", Duration.ofMinutes(-5));
            });
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Act
    for (int i = 0; i < 20; i++) {
      credentials.refreshIfExpired();
    }

    // Assert - the supplier is driven at a bounded rate rather than once per attempt, and the
    // token handed back keeps the expiration it really has.
    assertEquals(
        1,
        invocations.get(),
        "A supplier returning lapsed credentials must not be invoked once per refresh attempt");
    AccessToken accessToken = credentials.getAccessToken();
    assertEquals("lapsed-token", accessToken.getTokenValue());
    assertTrue(
        accessToken.getExpirationTime().toInstant().isBefore(Instant.now()),
        "The reused token must keep its real expiration rather than a fabricated one");
  }

  @Test
  void testSessionCredentialsSupplierWithoutExpirationStillProducesExpiringToken()
      throws Exception {
    // Arrange
    Instant before = Instant.now();
    CredentialsOverrider overrider =
        sessionSupplierOverrider(
            () -> new StsCredentials("key-id", "key-secret", "test-token", null));
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Act
    credentials.refreshIfExpired();

    // Assert - a token with no expiration would never be renewed, so one is synthesized.
    AccessToken accessToken = credentials.getAccessToken();
    assertEquals("test-token", accessToken.getTokenValue());
    assertNotNull(accessToken.getExpirationTime(), "A synthesized expiration should be applied");
    Instant expiration = accessToken.getExpirationTime().toInstant();
    Duration lifetime = RefreshableSessionCredentials.DEFAULT_TOKEN_LIFETIME;
    assertTrue(expiration.isAfter(Instant.now()), "The credentials should be usable");
    assertFalse(
        expiration.isBefore(before.plus(lifetime).minusSeconds(1)),
        "The synthesized expiration should be roughly one default token lifetime away");
    assertFalse(
        expiration.isAfter(Instant.now().plus(lifetime)),
        "The synthesized expiration should not exceed the default token lifetime");
  }

  @Test
  void testSessionCredentialsSupplierReturningNullFails() {
    // Arrange
    CredentialsOverrider overrider = sessionSupplierOverrider(() -> null);
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Act & Assert
    IOException ex = assertThrows(IOException.class, credentials::refreshIfExpired);
    assertTrue(ex.getMessage().contains("no credentials"), ex.getMessage());
  }

  @Test
  void testSessionCredentialsSupplierReturningNoTokenFails() {
    // Arrange
    CredentialsOverrider overrider =
        sessionSupplierOverrider(() -> new StsCredentials("key-id", "key-secret", "  "));
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Act & Assert
    IOException ex = assertThrows(IOException.class, credentials::refreshIfExpired);
    assertTrue(ex.getMessage().contains("no security token"), ex.getMessage());
  }

  @Test
  void testSessionCredentialsSupplierFailureIsSurfaced() {
    // Arrange
    CredentialsOverrider overrider =
        sessionSupplierOverrider(
            () -> {
              throw new IllegalStateException("session credentials unavailable");
            });
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);

    // Act & Assert
    IllegalStateException ex =
        assertThrows(IllegalStateException.class, credentials::refreshIfExpired);
    assertEquals("session credentials unavailable", ex.getMessage());
  }

  @Test
  void testSessionCredentialsSupplierTakesPrecedenceOverFixedCredentials() throws Exception {
    // Arrange
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key-id", "key-secret", "fixed-token"))
            .withSessionCredentialsSupplier(
                () -> sessionCredentials("supplied-token", Duration.ofHours(1)))
            .build();

    // Act
    GoogleCredentials credentials =
        (GoogleCredentials) GcpCredentialsProvider.getCredentials(overrider);
    credentials.refreshIfExpired();

    // Assert
    assertInstanceOf(RefreshableSessionCredentials.class, credentials);
    assertEquals("supplied-token", credentials.getAccessToken().getTokenValue());
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

  private static CredentialsOverrider sessionOverrider(StsCredentials sessionCredentials) {
    return new CredentialsOverrider.Builder(CredentialsType.SESSION)
        .withSessionCredentials(sessionCredentials)
        .build();
  }

  private static CredentialsOverrider sessionSupplierOverrider(
      Supplier<StsCredentials> sessionCredentialsSupplier) {
    return new CredentialsOverrider.Builder(CredentialsType.SESSION)
        .withSessionCredentialsSupplier(sessionCredentialsSupplier)
        .build();
  }

  private static StsCredentials sessionCredentials(String securityToken, Duration validFor) {
    return new StsCredentials("key-id", "key-secret", securityToken, Instant.now().plus(validFor));
  }
}
