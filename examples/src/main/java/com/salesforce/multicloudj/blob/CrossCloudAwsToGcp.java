package com.salesforce.multicloudj.blob;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.examples.AppConfig;
import com.salesforce.multicloudj.sts.client.StsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-cloud example: a workload on a source substrate accessing a bucket on a target substrate.
 *
 * <p>The workload runs on the source substrate with its ambient identity. The target substrate's
 * workload identity federation exchanges a signed caller-identity request from the source for a
 * short-lived access token, so the same {@code BucketClient} API works against the target with no
 * static keys.
 *
 * <p>Both substrates come from configuration: the target is the configured {@code provider} and
 * the source is {@code crosscloud.source.provider} (see {@code examples.properties}). The subject
 * token built here is the signed request envelope the target's federation endpoint verifies.
 */
public class CrossCloudAwsToGcp {
  private static final Logger logger = LoggerFactory.getLogger(CrossCloudAwsToGcp.class);

  public static void main(String[] args) {
    // Never hardcode provider ids or environment-specific values; resolve them from configuration
    // (see examples.properties).
    String sourceProvider = AppConfig.get("crosscloud.source.provider");
    String sourceRegion = AppConfig.get("crosscloud.source.region");
    String targetProvider = AppConfig.provider();
    String bucket = AppConfig.get("crosscloud.target.bucket");
    // The audience is the full resource name of the target's federation provider. The bucket role
    // is granted directly to this federated principal, so no service account sits in the middle.
    String audience = AppConfig.get("crosscloud.target.audience");

    Supplier<String> webIdentityTokenSupplier =
        () -> buildSubjectToken(sourceProvider, sourceRegion, audience);

    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole(audience)
            .withWebIdentityTokenSupplier(webIdentityTokenSupplier)
            .build();

    BucketClient bucketClient =
        BucketClient.builder(targetProvider)
            .withBucket(bucket)
            .withCredentialsOverrider(overrider)
            .build();

    ListBlobsPageResponse page =
        bucketClient.listPage(ListBlobsPageRequest.builder().withMaxResults(10).build());
    page.getBlobs().forEach(b -> logger.info("{}", b.getKey()));
  }

  // Signs a caller-identity request with the workload's ambient source identity, then shapes the
  // signed request into the URL-encoded JSON envelope that the target's federation endpoint
  // expects as a subject token.
  private static String buildSubjectToken(String sourceProvider, String region, String audience) {
    // The target requires the audience to travel inside the signed headers, so it is bound to the
    // signature and the request cannot be replayed against any other target.
    // The target replays the signed request from its URL and headers alone, with no body, so the
    // action and version must be signed as query parameters rather than in a form body.
    SignOptions options =
        SignOptions.builder()
            .withCustomHeader(AppConfig.get("crosscloud.target.audience.header"), audience)
            .withActionInQueryString(true)
            .build();

    // No credentials are supplied, so the signer resolves the workload's identity from the source
    // provider's ambient default credential chain.
    StsUtilities stsUtil = StsUtilities.builder(sourceProvider).withRegion(region).build();

    // Passing null means "just sign a GetCallerIdentity request, there is no
    // service payload to hash." The library fills in Action=GetCallerIdentity.
    SignedAuthRequest signed = stsUtil.newCloudNativeAuthSignedRequest(null, options);
    HttpRequest signedRequest = signed.getRequest();

    // Shape the signed request into the federation envelope:
    // { "url": ..., "method": ..., "headers": [ { "key":..., "value":... } ] }
    JsonArray headers = new JsonArray();
    signedRequest
        .headers()
        .map()
        .forEach(
            (name, values) -> {
              JsonObject header = new JsonObject();
              // The signer canonicalizes header names to lowercase before signing, so the
              // envelope must carry lowercase keys to match the signed-headers list that the
              // verifier recomputes.
              header.addProperty("key", name.toLowerCase());
              header.addProperty("value", values.get(0));
              headers.add(header);
            });

    // The signer always signs the Host header, but the JDK HTTP client manages Host itself and does
    // not expose it in the request headers. The verifier recomputes the signature and requires the
    // host header, so add it back from the request URI.
    JsonObject hostHeader = new JsonObject();
    hostHeader.addProperty("key", "host");
    hostHeader.addProperty("value", signedRequest.uri().getHost());
    headers.add(hostHeader);

    JsonObject envelope = new JsonObject();
    envelope.addProperty("url", signedRequest.uri().toString());
    envelope.addProperty("method", signedRequest.method());
    envelope.add("headers", headers);

    // The federation endpoint reads the subject token URL-encoded and infers the signed-request
    // token type from the leading brace, so the encoded envelope is what it expects.
    return URLEncoder.encode(envelope.toString(), StandardCharsets.UTF_8);
  }
}
