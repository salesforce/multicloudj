package com.salesforce.multicloudj.common.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.auth.RequestMetadataCallback;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.AccessToken;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsRefresher;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.SecurityLevel;
import io.grpc.Status;
import io.grpc.auth.MoreCallCredentials;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class RefreshableSessionCredentialsTest {

  private static final URI URI_UNDER_TEST = URI.create("https://storage.example.com/resource");
  private static final Executor DIRECT = Runnable::run;

  /** Returns whatever is currently set, counting calls; throws when a failure is set. */
  private static final class ControlledSupplier implements Supplier<StsCredentials> {
    final AtomicInteger calls = new AtomicInteger();
    final AtomicReference<StsCredentials> value = new AtomicReference<>();
    final AtomicReference<RuntimeException> failure = new AtomicReference<>();

    ControlledSupplier(StsCredentials initial) {
      value.set(initial);
    }

    @Override
    public StsCredentials get() {
      calls.incrementAndGet();
      RuntimeException toThrow = failure.get();
      if (toThrow != null) {
        throw toThrow;
      }
      return value.get();
    }
  }

  private static CredentialsRefresher refresherFor(Supplier<StsCredentials> supplier) {
    return new CredentialsOverrider.Builder(CredentialsType.SESSION)
        .withSessionCredentialsSupplier(supplier)
        .withRefreshInterval(Duration.ofHours(1))
        .build()
        .getCredentialsRefresher();
  }

  private static StsCredentials plain(String token) {
    return new StsCredentials("access-key-id", "access-key-secret", token);
  }

  private static RefreshableSessionCredentials credentialsFor(Supplier<StsCredentials> supplier) {
    return new RefreshableSessionCredentials(refresherFor(supplier));
  }

  @Test
  void requestMetadataCarriesTheSecurityTokenAsBearerAndIgnoresKeyAndSecret() throws Exception {
    RefreshableSessionCredentials credentials =
        credentialsFor(new ControlledSupplier(plain("token-1")));

    Map<String, List<String>> metadata = credentials.getRequestMetadata(URI_UNDER_TEST);

    assertEquals(List.of("Bearer token-1"), metadata.get("Authorization"));
    assertFalse(metadata.toString().contains("access-key-id"));
    assertFalse(metadata.toString().contains("access-key-secret"));
  }

  @Test
  void everyRequestPullsFromTheRefresherAndTheSupplierIsCalledOnce() throws Exception {
    ControlledSupplier supplier = new ControlledSupplier(plain("token-1"));
    RefreshableSessionCredentials credentials = credentialsFor(supplier);
    assertEquals(0, supplier.calls.get(), "nothing is fetched until the first request");

    for (int i = 0; i < 5; i++) {
      assertEquals(
          List.of("Bearer token-1"),
          credentials.getRequestMetadata(URI_UNDER_TEST).get("Authorization"));
    }

    assertEquals(1, supplier.calls.get());
  }

  @Test
  void blankOrMissingSecurityTokenIsRejected() {
    RefreshableSessionCredentials blank = credentialsFor(new ControlledSupplier(plain("  ")));
    RefreshableSessionCredentials missing = credentialsFor(new ControlledSupplier(plain(null)));

    assertThrows(UnAuthorizedException.class, () -> blank.getRequestMetadata(URI_UNDER_TEST));
    assertThrows(UnAuthorizedException.class, () -> missing.getRequestMetadata(URI_UNDER_TEST));
  }

  @Test
  void supplierFailurePropagatesFromTheSynchronousMethods() {
    ControlledSupplier supplier = new ControlledSupplier(plain("token-1"));
    supplier.failure.set(new IllegalStateException("issuer unavailable"));
    RefreshableSessionCredentials credentials = credentialsFor(supplier);

    UnAuthorizedException thrown =
        assertThrows(
            UnAuthorizedException.class, () -> credentials.getRequestMetadata(URI_UNDER_TEST));
    assertInstanceOf(IllegalStateException.class, thrown.getCause());
    assertThrows(UnAuthorizedException.class, credentials::refreshIfExpired);
    assertThrows(UnAuthorizedException.class, credentials::refreshAccessToken);
  }

  @Test
  void asynchronousRequestMetadataDeliversTheMetadata() {
    RefreshableSessionCredentials credentials =
        credentialsFor(new ControlledSupplier(plain("token-1")));
    RecordingCallback callback = new RecordingCallback();

    credentials.getRequestMetadata(URI_UNDER_TEST, DIRECT, callback);

    assertNull(callback.failure);
    assertEquals(List.of("Bearer token-1"), callback.metadata.get("Authorization"));
  }

  @Test
  void asynchronousRequestMetadataWithoutAnExecutorRunsOnTheCallingThread() {
    RefreshableSessionCredentials credentials =
        credentialsFor(new ControlledSupplier(plain("token-1")));
    RecordingCallback callback = new RecordingCallback();

    credentials.getRequestMetadata(URI_UNDER_TEST, null, callback);

    assertEquals(List.of("Bearer token-1"), callback.metadata.get("Authorization"));
  }

  @Test
  void asynchronousSupplierFailureIsReportedToTheCallbackAndNotThrown() {
    ControlledSupplier supplier = new ControlledSupplier(plain("token-1"));
    supplier.failure.set(new IllegalStateException("issuer unavailable"));
    RefreshableSessionCredentials credentials = credentialsFor(supplier);
    RecordingCallback callback = new RecordingCallback();

    credentials.getRequestMetadata(URI_UNDER_TEST, DIRECT, callback);

    assertNull(callback.metadata);
    assertInstanceOf(UnAuthorizedException.class, callback.failure);
  }

  @Test
  void aRejectedExecutorIsReportedToTheCallback() {
    RefreshableSessionCredentials credentials =
        credentialsFor(new ControlledSupplier(plain("token-1")));
    RecordingCallback callback = new RecordingCallback();
    Executor rejecting =
        command -> {
          throw new RejectedExecutionException("shut down");
        };

    credentials.getRequestMetadata(URI_UNDER_TEST, rejecting, callback);

    assertNull(callback.metadata);
    assertInstanceOf(RejectedExecutionException.class, callback.failure);
  }

  @Test
  void refreshIfExpiredPopulatesTheCachedTokenFromTheRefresher() throws Exception {
    ControlledSupplier supplier = new ControlledSupplier(plain("token-1"));
    RefreshableSessionCredentials credentials = credentialsFor(supplier);
    assertNull(credentials.getAccessToken(), "the library cache is empty before any refresh");

    credentials.refreshIfExpired();
    credentials.refreshIfExpired();

    AccessToken token = credentials.getAccessToken();
    assertEquals("token-1", token.getTokenValue());
    assertNull(
        token.getExpirationTime(), "an unknown expiry is not invented for credentials without one");
    assertEquals(1, supplier.calls.get(), "the refresher serves repeat refreshes from its cache");
  }

  @Test
  void refreshedTokenWithoutASuppliedExpirationHasNone() throws Exception {
    RefreshableSessionCredentials credentials =
        credentialsFor(
            new ControlledSupplier(new StsCredentials("key", "secret", "token-1", null)));

    credentials.refreshIfExpired();

    assertNull(credentials.getAccessToken().getExpirationTime());
  }

  @Test
  void refreshedTokenCarriesTheSuppliedExpiration() throws Exception {
    Instant expiration = Instant.ofEpochMilli(System.currentTimeMillis() + 7_200_000L);
    RefreshableSessionCredentials credentials =
        credentialsFor(
            new ControlledSupplier(
                new StsCredentials("key", "secret", "token-1", expiration)));

    credentials.refreshIfExpired();

    assertEquals(Date.from(expiration), credentials.getAccessToken().getExpirationTime());
    assertEquals(
        List.of("Bearer token-1"),
        credentials.getRequestMetadata(URI_UNDER_TEST).get("Authorization"));
  }

  @Test
  void httpAdapterSetsTheAuthorizationHeaderOnEveryRequestWithOneSupplierCall() throws Exception {
    ControlledSupplier supplier = new ControlledSupplier(plain("token-1"));
    HttpCredentialsAdapter adapter = new HttpCredentialsAdapter(credentialsFor(supplier));
    MockHttpTransport transport = new MockHttpTransport();

    for (int i = 0; i < 3; i++) {
      HttpRequest request =
          transport
              .createRequestFactory(adapter)
              .buildGetRequest(new GenericUrl("https://storage.example.com/o"));
      assertEquals("Bearer token-1", request.getHeaders().getAuthorization());
    }

    assertEquals(1, supplier.calls.get());
  }

  @Test
  void grpcCallCredentialsAttachTheBearerTokenToTheMetadata() {
    ControlledSupplier supplier = new ControlledSupplier(plain("token-1"));
    CallCredentials callCredentials = MoreCallCredentials.from(credentialsFor(supplier));
    MetadataCapture capture = new MetadataCapture();

    callCredentials.applyRequestMetadata(new FixedRequestInfo(), DIRECT, capture);
    callCredentials.applyRequestMetadata(new FixedRequestInfo(), DIRECT, capture);

    assertNull(capture.failure);
    assertEquals(
        "Bearer token-1",
        capture.headers.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)));
    assertEquals(1, supplier.calls.get());
  }

  @Test
  void grpcCallCredentialsFailTheCallWhenTheSupplierFails() {
    ControlledSupplier supplier = new ControlledSupplier(plain("token-1"));
    supplier.failure.set(new IllegalStateException("issuer unavailable"));
    CallCredentials callCredentials = MoreCallCredentials.from(credentialsFor(supplier));
    MetadataCapture capture = new MetadataCapture();

    callCredentials.applyRequestMetadata(new FixedRequestInfo(), DIRECT, capture);

    assertNull(capture.headers);
    assertNotNull(capture.failure);
    assertEquals(Status.Code.UNAUTHENTICATED, capture.failure.getCode());
    assertInstanceOf(UnAuthorizedException.class, capture.failure.getCause());
  }

  @Test
  void declaresRequestMetadataAndReturnsItselfWhenScoped() {
    RefreshableSessionCredentials credentials =
        credentialsFor(new ControlledSupplier(plain("token-1")));

    assertTrue(credentials.hasRequestMetadata());
    assertSame(credentials, credentials.createScoped(List.of("scope")));
  }

  @Test
  void copyMethodsThatWouldDropTheRefresherFailClosed() {
    RefreshableSessionCredentials credentials =
        credentialsFor(new ControlledSupplier(plain("token-1")));

    assertThrows(
        UnSupportedOperationException.class, () -> credentials.createWithQuotaProject("project"));
    assertThrows(UnSupportedOperationException.class, credentials::toBuilder);
  }

  @Test
  void copyMethodsThatReturnTheSameInstanceKeepTheRefresher() {
    RefreshableSessionCredentials credentials =
        credentialsFor(new ControlledSupplier(plain("token-1")));

    assertSame(credentials, credentials.createScoped(List.of("scope")));
    assertSame(credentials, credentials.createScoped(List.of("scope"), List.of("default")));
    assertSame(credentials, credentials.createScoped("scope"));
    assertSame(credentials, credentials.createDelegated("user"));
    assertSame(credentials, credentials.createWithCustomRetryStrategy(false));
  }

  @Test
  void toStringDoesNotExposeTheToken() throws Exception {
    RefreshableSessionCredentials credentials =
        credentialsFor(new ControlledSupplier(plain("secret-token-value")));
    credentials.refreshIfExpired();

    assertFalse(credentials.toString().contains("secret-token-value"));
  }

  @Test
  void equalityFollowsTheSharedRefresher() {
    CredentialsRefresher refresher = refresherFor(new ControlledSupplier(plain("token-1")));

    RefreshableSessionCredentials a = new RefreshableSessionCredentials(refresher);
    RefreshableSessionCredentials b = new RefreshableSessionCredentials(refresher);
    RefreshableSessionCredentials other = credentialsFor(new ControlledSupplier(plain("token-1")));

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, other);
  }

  @Test
  void nullRefresherIsRejected() {
    assertThrows(InvalidArgumentException.class, () -> new RefreshableSessionCredentials(null));
  }

  private static final class RecordingCallback implements RequestMetadataCallback {
    Map<String, List<String>> metadata;
    Throwable failure;

    @Override
    public void onSuccess(Map<String, List<String>> metadata) {
      this.metadata = metadata;
    }

    @Override
    public void onFailure(Throwable exception) {
      this.failure = exception;
    }
  }

  private static final class MetadataCapture extends CallCredentials.MetadataApplier {
    Metadata headers;
    Status failure;

    @Override
    public void apply(Metadata headers) {
      this.headers = headers;
    }

    @Override
    public void fail(Status status) {
      this.failure = status;
    }
  }

  private static final class FixedRequestInfo extends CallCredentials.RequestInfo {
    private static final MethodDescriptor.Marshaller<String> MARSHALLER =
        new MethodDescriptor.Marshaller<>() {
          @Override
          public InputStream stream(String value) {
            return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public String parse(InputStream stream) {
            return "";
          }
        };

    private final MethodDescriptor<String, String> method =
        MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("example.Service/Method")
            .setRequestMarshaller(MARSHALLER)
            .setResponseMarshaller(MARSHALLER)
            .build();

    @Override
    public MethodDescriptor<?, ?> getMethodDescriptor() {
      return method;
    }

    @Override
    public SecurityLevel getSecurityLevel() {
      return SecurityLevel.PRIVACY_AND_INTEGRITY;
    }

    @Override
    public String getAuthority() {
      return "storage.example.com";
    }

    @Override
    public Attributes getTransportAttrs() {
      return Attributes.EMPTY;
    }
  }
}
