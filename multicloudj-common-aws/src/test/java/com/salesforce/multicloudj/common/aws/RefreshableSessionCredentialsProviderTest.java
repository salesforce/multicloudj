package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class RefreshableSessionCredentialsProviderTest {

  private static final Duration INTERVAL = Duration.ofHours(1);

  private static AwsCredentialsProvider providerFor(Supplier<StsCredentials> supplier) {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.REFRESHABLE_SESSION)
            .withSessionCredentialsSupplier(supplier)
            .withRefreshInterval(INTERVAL)
            .build();
    return CredentialsProvider.getCredentialsProvider(overrider, Region.US_WEST_2);
  }

  @Test
  public void testTokenProducesSessionCredentials() {
    AwsCredentials credentials =
        providerFor(() -> new StsCredentials("key", "secret", "token")).resolveCredentials();

    AwsSessionCredentials session =
        Assertions.assertInstanceOf(AwsSessionCredentials.class, credentials);
    Assertions.assertEquals("key", session.accessKeyId());
    Assertions.assertEquals("secret", session.secretAccessKey());
    Assertions.assertEquals("token", session.sessionToken());
    Assertions.assertTrue(session.expirationTime().isEmpty());
  }

  @Test
  public void testBlankTokenProducesBasicCredentials() {
    for (String token : new String[] {null, "", "  "}) {
      AwsCredentials credentials =
          providerFor(() -> new StsCredentials("key", "secret", token)).resolveCredentials();

      AwsBasicCredentials basic =
          Assertions.assertInstanceOf(AwsBasicCredentials.class, credentials);
      Assertions.assertEquals("key", basic.accessKeyId());
      Assertions.assertEquals("secret", basic.secretAccessKey());
    }
  }

  @Test
  public void testUnknownExpiryLeavesSessionCredentialsWithoutExpiry() {
    AwsCredentials credentials =
        providerFor(() -> new StsCredentials("key", "secret", "token", null)).resolveCredentials();

    AwsSessionCredentials session =
        Assertions.assertInstanceOf(AwsSessionCredentials.class, credentials);
    Assertions.assertTrue(session.expirationTime().isEmpty());
  }

  @Test
  public void testExpiryIsCarriedOntoSessionCredentials() {
    Instant expiration = Instant.now().plus(Duration.ofHours(2));

    AwsCredentials credentials =
        providerFor(() -> new StsCredentials("key", "secret", "token", expiration))
            .resolveCredentials();

    AwsSessionCredentials session =
        Assertions.assertInstanceOf(AwsSessionCredentials.class, credentials);
    Assertions.assertEquals(expiration, session.expirationTime().orElseThrow());
  }

  @Test
  public void testCachingIsLeftToTheRefresher() {
    AtomicInteger calls = new AtomicInteger();
    AwsCredentialsProvider provider =
        providerFor(() -> new StsCredentials("key" + calls.incrementAndGet(), "secret", "token"));

    AwsCredentials first = provider.resolveCredentials();
    AwsCredentials second = provider.resolveCredentials();

    Assertions.assertEquals(1, calls.get());
    Assertions.assertEquals("key1", first.accessKeyId());
    Assertions.assertEquals("key1", second.accessKeyId());
  }

  @Test
  public void testFirstFetchFailurePropagatesAndIsNotRetriedImmediately() {
    AtomicInteger calls = new AtomicInteger();
    AwsCredentialsProvider provider =
        providerFor(
            () -> {
              if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("issuer unavailable");
              }
              return new StsCredentials("key", "secret", "token");
            });

    UnAuthorizedException thrown =
        Assertions.assertThrows(UnAuthorizedException.class, provider::resolveCredentials);
    Assertions.assertInstanceOf(IllegalStateException.class, thrown.getCause());

    UnAuthorizedException repeated =
        Assertions.assertThrows(UnAuthorizedException.class, provider::resolveCredentials);
    Assertions.assertSame(thrown, repeated.getCause());
    Assertions.assertEquals(1, calls.get());
  }

  @Test
  public void testAlreadyExpiredCredentialsAreRejectedAndNotRetriedImmediately() {
    AtomicInteger calls = new AtomicInteger();
    AwsCredentialsProvider provider =
        providerFor(
            () -> {
              calls.incrementAndGet();
              return new StsCredentials(
                  "key", "secret", "token", Instant.now().minus(Duration.ofMinutes(1)));
            });

    Assertions.assertThrows(UnAuthorizedException.class, provider::resolveCredentials);
    Assertions.assertThrows(UnAuthorizedException.class, provider::resolveCredentials);
    Assertions.assertEquals(1, calls.get());
  }

  @Test
  public void testSdkClientSignsEveryCallWithTheSuppliedCredentials() {
    AtomicInteger supplierCalls = new AtomicInteger();
    AtomicInteger resolveCalls = new AtomicInteger();
    AwsCredentialsProvider refreshable =
        providerFor(
            () -> {
              supplierCalls.incrementAndGet();
              return new StsCredentials("AKIDSUPPLIED", "secret", "supplied-token");
            });
    AwsCredentialsProvider counting =
        () -> {
          resolveCalls.incrementAndGet();
          return refreshable.resolveCredentials();
        };
    List<SdkHttpRequest> requests = new ArrayList<>();

    try (S3Client s3 =
        S3Client.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(counting)
            .httpClient(new RecordingHttpClient(requests))
            .build()) {
      s3.headBucket(b -> b.bucket("bucket"));
      s3.headBucket(b -> b.bucket("bucket"));
    }

    Assertions.assertEquals(2, requests.size());
    Assertions.assertEquals(2, resolveCalls.get());
    Assertions.assertEquals(1, supplierCalls.get());
    for (SdkHttpRequest request : requests) {
      String authorization = request.firstMatchingHeader("Authorization").orElseThrow();
      Assertions.assertTrue(authorization.contains("Credential=AKIDSUPPLIED/"), authorization);
      Assertions.assertEquals(
          "supplied-token", request.firstMatchingHeader("x-amz-security-token").orElseThrow());
    }
  }

  private static final class RecordingHttpClient implements SdkHttpClient {
    private final List<SdkHttpRequest> requests;

    private RecordingHttpClient(List<SdkHttpRequest> requests) {
      this.requests = requests;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      requests.add(request.httpRequest());
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return HttpExecuteResponse.builder()
              .response(SdkHttpResponse.builder().statusCode(200).build())
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}
  }
}
