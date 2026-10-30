package com.salesforce.multicloudj.sts.aws;

import com.salesforce.multicloudj.sts.driver.FlowCollector;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.ByteArrayInputStream;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Locale;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/**
 * Signs a request produced by {@link AwsStsUtilities} with the AWS SDK's own SigV4 signer so tests
 * can assert that the hand-rolled signature equals the SDK's. This class delegates all signing to
 * the SDK and does not implement any part of the algorithm itself.
 *
 * <p>{@link Aws4Signer} is used rather than the newer {@code AwsV4HttpSigner} because the newer
 * signer always adds and signs an {@code x-amz-content-sha256} header, whereas {@link Aws4Signer}
 * signs only the headers present on the request.
 */
final class SigV4Oracle {
  private static final String SERVICE_SIGNING_NAME = "sts";
  // Headers the signer under test produces; the SDK signer must derive them itself.
  private static final Set<String> SIGNER_OWNED_HEADERS =
      Set.of("authorization", "x-amz-date", "x-amz-security-token");

  private SigV4Oracle() {}

  /** Returns the Authorization header value the AWS SDK signer computes for the signed request. */
  static String authorization(
      SignedAuthRequest signed, StsCredentials creds, String region, Clock clock) {
    HttpRequest request = signed.getRequest();
    byte[] body = bodyOf(request);

    SdkHttpFullRequest.Builder sdkRequest =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.fromValue(request.method()))
            .uri(request.uri());
    request
        .headers()
        .map()
        .forEach(
            (name, values) -> {
              if (!SIGNER_OWNED_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                sdkRequest.putHeader(name, values);
              }
            });

    sdkRequest.contentStreamProvider(() -> new ByteArrayInputStream(body));
    AwsSessionCredentials credentials =
        AwsSessionCredentials.create(
            creds.getAccessKeyId(), creds.getAccessKeySecret(), creds.getSecurityToken());

    SdkHttpFullRequest result =
        Aws4Signer.create()
            .sign(
                sdkRequest.build(),
                Aws4SignerParams.builder()
                    .awsCredentials(credentials)
                    .signingName(SERVICE_SIGNING_NAME)
                    .signingRegion(Region.of(region))
                    .signingClockOverride(clock)
                    .build());
    return result.firstMatchingHeader("Authorization").orElseThrow();
  }

  private static byte[] bodyOf(HttpRequest request) {
    return request
        .bodyPublisher()
        .map(
            publisher -> {
              FlowCollector<ByteBuffer> collector = new FlowCollector<>();
              publisher.subscribe(collector);
              return collector.items().join().stream()
                  .map(ByteBuffer::array)
                  .reduce(new byte[0], SigV4Oracle::concat);
            })
        .orElse(new byte[0]);
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
