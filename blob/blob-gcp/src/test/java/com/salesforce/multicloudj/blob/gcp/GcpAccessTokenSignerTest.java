package com.salesforce.multicloudj.blob.gcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.Credentials;
import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class GcpAccessTokenSignerTest {

  private static final String TOKEN = "ya29.test-token";
  private static final String EMAIL = "signer@test-project.iam.gserviceaccount.com";
  private static final String SIGN_BLOB_URL =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/" + EMAIL + ":signBlob";

  /** Records every request and answers tokeninfo and signBlob calls with canned responses. */
  private static final class RecordingTransport extends MockHttpTransport {
    private final int tokenInfoStatus;
    private final String tokenInfoBody;
    private final List<String> urls = new ArrayList<>();
    private final List<String> bodies = new ArrayList<>();
    private final List<String> authorizations = new ArrayList<>();

    RecordingTransport(int tokenInfoStatus, String tokenInfoBody) {
      this.tokenInfoStatus = tokenInfoStatus;
      this.tokenInfoBody = tokenInfoBody;
    }

    @Override
    public LowLevelHttpRequest buildRequest(String method, String url) {
      return new MockLowLevelHttpRequest(url) {
        @Override
        public LowLevelHttpResponse execute() throws IOException {
          urls.add(url);
          ByteArrayOutputStream body = new ByteArrayOutputStream();
          if (getStreamingContent() != null) {
            getStreamingContent().writeTo(body);
          }
          bodies.add(body.toString(StandardCharsets.UTF_8));
          authorizations.add(getFirstHeaderValue("Authorization"));

          MockLowLevelHttpResponse response =
              new MockLowLevelHttpResponse().setContentType("application/json");
          if (url.startsWith(GcpAccessTokenSigner.TOKEN_INFO_URL)) {
            return response.setStatusCode(tokenInfoStatus).setContent(tokenInfoBody);
          }
          String signature = Base64.getEncoder()
              .encodeToString("signature".getBytes(StandardCharsets.UTF_8));
          return response.setContent(
              "{\"keyId\":\"key-1\",\"signedBlob\":\"" + signature + "\"}");
        }
      };
    }
  }

  private static RecordingTransport tokenInfoReturning(String body) {
    return new RecordingTransport(200, body);
  }

  private static StorageOptions options(Credentials credentials, MockHttpTransport transport) {
    return StorageOptions.newBuilder()
        .setProjectId("test-project")
        .setCredentials(credentials)
        .setTransportOptions(
            HttpTransportOptions.newBuilder().setHttpTransportFactory(() -> transport).build())
        .build();
  }

  private static GoogleCredentials tokenCredentials(String token) {
    return GoogleCredentials.create(new AccessToken(token, null));
  }

  @Test
  void resolvesEmailFromTokenInfoAndSignsWithIamUsingTheToken() {
    RecordingTransport transport =
        tokenInfoReturning("{\"email\":\"" + EMAIL + "\",\"azp\":\"123\"}");

    Optional<ServiceAccountSigner> signer =
        new GcpAccessTokenSigner().signerFor(options(tokenCredentials(TOKEN), transport));

    assertTrue(signer.isPresent());
    assertEquals(EMAIL, signer.get().getAccount());
    assertEquals(GcpAccessTokenSigner.TOKEN_INFO_URL, transport.urls.get(0));
    assertEquals("access_token=" + TOKEN, transport.bodies.get(0));

    byte[] signature = signer.get().sign("string-to-sign".getBytes(StandardCharsets.UTF_8));

    assertArrayEquals("signature".getBytes(StandardCharsets.UTF_8), signature);
    assertEquals(SIGN_BLOB_URL, transport.urls.get(1));
    assertEquals("Bearer " + TOKEN, transport.authorizations.get(1));
  }

  @Test
  void reusesSignerForSameTokenAndResolvesAgainAfterRotation() {
    RecordingTransport transport = tokenInfoReturning("{\"email\":\"" + EMAIL + "\"}");
    GcpAccessTokenSigner tokenSigner = new GcpAccessTokenSigner();

    ServiceAccountSigner first =
        tokenSigner.signerFor(options(tokenCredentials(TOKEN), transport)).get();
    ServiceAccountSigner second =
        tokenSigner.signerFor(options(tokenCredentials(TOKEN), transport)).get();

    assertSame(first, second);
    assertEquals(1, transport.urls.size());

    tokenSigner.signerFor(options(tokenCredentials("ya29.rotated"), transport));

    assertEquals(2, transport.urls.size());
    assertEquals("access_token=ya29.rotated", transport.bodies.get(1));
  }

  @Test
  void concurrentCallsForANewTokenShareOneTokenInfoLookup() throws Exception {
    AtomicInteger tokenInfoCalls = new AtomicInteger();
    MockHttpTransport slowTokenInfo = new MockHttpTransport() {
      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) {
        return new MockLowLevelHttpRequest(url) {
          @Override
          public LowLevelHttpResponse execute() throws IOException {
            tokenInfoCalls.incrementAndGet();
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return new MockLowLevelHttpResponse()
                .setContentType("application/json")
                .setContent("{\"email\":\"" + EMAIL + "\"}");
          }
        };
      }
    };
    GcpAccessTokenSigner tokenSigner = new GcpAccessTokenSigner();
    StorageOptions storageOptions = options(tokenCredentials(TOKEN), slowTokenInfo);
    int threads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<ServiceAccountSigner>> results = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        results.add(executor.submit(() -> {
          start.await();
          return tokenSigner.signerFor(storageOptions).get();
        }));
      }
      start.countDown();

      ServiceAccountSigner first = results.get(0).get(10, TimeUnit.SECONDS);
      for (Future<ServiceAccountSigner> result : results) {
        assertSame(first, result.get(10, TimeUnit.SECONDS));
      }
    } finally {
      executor.shutdownNow();
    }
    assertEquals(1, tokenInfoCalls.get());
  }

  @Test
  void throwsInvalidArgumentWhenTokenInfoHasNoEmail() {
    RecordingTransport transport = tokenInfoReturning("{\"azp\":\"123\",\"sub\":\"123\"}");

    InvalidArgumentException e = assertThrows(InvalidArgumentException.class,
        () -> new GcpAccessTokenSigner().signerFor(options(tokenCredentials(TOKEN), transport)));

    assertTrue(e.getMessage().contains("'userinfo.email' scope"));
  }

  @Test
  void surfacesTokenInfoHttpErrorWithItsStatusCodeWithoutTheToken() {
    SubstrateSdkException e = assertThrows(SubstrateSdkException.class,
        () -> new GcpAccessTokenSigner().signerFor(options(tokenCredentials(TOKEN),
            new RecordingTransport(400, "{\"error\":\"invalid_token\"}"))));

    HttpResponseException cause = assertInstanceOf(HttpResponseException.class, e.getCause());
    assertEquals(400, cause.getStatusCode());
    assertFalse(e.getMessage().contains(TOKEN));
    assertFalse(cause.getMessage().contains(TOKEN));
  }

  @Test
  void doesNotLogTheTokenInTheTokenInfoRequestBody() {
    RecordingTransport transport = tokenInfoReturning("{\"email\":\"" + EMAIL + "\"}");
    Logger httpLogger = Logger.getLogger(HttpTransport.class.getName());
    Level previousLevel = httpLogger.getLevel();
    List<String> logged = new ArrayList<>();
    Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        logged.add(record.getMessage());
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
    handler.setLevel(Level.ALL);
    httpLogger.setLevel(Level.ALL);
    httpLogger.addHandler(handler);
    try {
      new GcpAccessTokenSigner().signerFor(options(tokenCredentials(TOKEN), transport));
    } finally {
      httpLogger.removeHandler(handler);
      httpLogger.setLevel(previousLevel);
    }

    assertTrue(logged.stream().noneMatch(message -> message != null && message.contains(TOKEN)),
        logged.toString());
  }

  @Test
  void skipsCredentialsThatCanSignThemselves() throws Exception {
    RecordingTransport transport = tokenInfoReturning("{}");
    ServiceAccountCredentials serviceAccount = ServiceAccountCredentials.newBuilder()
        .setClientEmail(EMAIL)
        .setPrivateKey(KeyPairGenerator.getInstance("RSA").generateKeyPair().getPrivate())
        .build();

    assertFalse(new GcpAccessTokenSigner()
        .signerFor(options(serviceAccount, transport)).isPresent());
    assertTrue(transport.urls.isEmpty());
  }

  @Test
  void skipsWhenOptionsOrTokenAreUnavailable() {
    RecordingTransport transport = tokenInfoReturning("{}");
    GcpAccessTokenSigner tokenSigner = new GcpAccessTokenSigner();

    assertFalse(tokenSigner.signerFor(null).isPresent());
    assertFalse(tokenSigner.signerFor(
        options(GoogleCredentials.newBuilder().build(), transport)).isPresent());
    assertTrue(transport.urls.isEmpty());
  }
}
