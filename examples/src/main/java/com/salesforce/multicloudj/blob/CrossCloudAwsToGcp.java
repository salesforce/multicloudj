package com.salesforce.multicloudj.blob;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.sts.client.StsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * Cross-cloud example: an AWS workload accessing Google Cloud Storage.
 *
 * <p>The pod runs on AWS with an ambient AWS role (e.g. IRSA). Google Cloud Workload Identity
 * Federation exchanges a signed AWS GetCallerIdentity request for a short-lived GCP access token,
 * so the same {@code BucketClient} API works against GCP with no static GCP keys.
 */
public class CrossCloudAwsToGcp {

  private static final String REGION = "us-west-2";

  private static final String BUCKET = "palsfdc";

  // The audience is the full Workload Identity Pool provider resource name. We grant the bucket
  // role directly to this pool principal (direct pool access), so no service account sits in the
  // middle.
  private static final String AUDIENCE =
      "//iam.googleapis.com/projects/599653580068/locations/global/workloadIdentityPools/"
          + "substrate-sdk/providers/substrate-gcp";

  public static void main(String[] args) {
    Supplier<String> webIdentityTokenSupplier = CrossCloudAwsToGcp::buildSubjectToken;

    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
            .withRole(AUDIENCE)
            .withWebIdentityTokenSupplier(webIdentityTokenSupplier)
            .build();

    BucketClient bucketClient =
        BucketClient.builder("gcp")
            .withBucket(BUCKET)
            .withCredentialsOverrider(overrider)
            .build();

    ListBlobsPageResponse page =
        bucketClient.listPage(ListBlobsPageRequest.builder().withMaxResults(10).build());
    page.getBlobs().forEach(b -> System.out.println(b.getKey()));
  }

  // Signs a GetCallerIdentity request with the pod's AWS role, then shapes the
  // signed request into the URL-encoded JSON envelope that Google STS expects
  // as an AWS4 subject token.
  private static String buildSubjectToken() {
    // Google requires the audience to travel inside the signed headers, so it is
    // bound to the signature and the request cannot be replayed against any other
    // target.
    // GCP replays the signed request from its URL and headers alone, with no body, so the action
    // and version must be signed as query parameters rather than in a form body.
    SignOptions options =
        SignOptions.builder()
            .withCustomHeader("x-goog-cloud-target-resource", AUDIENCE)
            .withActionInQueryString(true)
            .build();

    // No credentials are supplied, so the signer resolves the pod's AWS role from the ambient
    // default credential chain (IRSA, environment, container, or instance metadata).
    StsUtilities stsUtil = StsUtilities.builder("aws").withRegion(REGION).build();

    // Passing null means "just sign a GetCallerIdentity request, there is no
    // service payload to hash." The library fills in Action=GetCallerIdentity.
    SignedAuthRequest signed = stsUtil.newCloudNativeAuthSignedRequest(null, options);
    HttpRequest signedRequest = signed.getRequest();

    // Shape the signed request into Google's GetCallerIdentity envelope:
    // { "url": ..., "method": ..., "headers": [ { "key":..., "value":... } ] }
    JsonArray headers = new JsonArray();
    signedRequest
        .headers()
        .map()
        .forEach(
            (name, values) -> {
              JsonObject header = new JsonObject();
              // SigV4 canonicalizes header names to lowercase before signing, so the envelope
              // must carry lowercase keys to match the SignedHeaders list that the verifier
              // recomputes.
              header.addProperty("key", name.toLowerCase());
              header.addProperty("value", values.get(0));
              headers.add(header);
            });

    // SigV4 always signs the Host header, but the JDK HTTP client manages Host itself and does not
    // expose it in the request headers. Google recomputes the signature and requires the host
    // header, so add it back from the request URI.
    JsonObject hostHeader = new JsonObject();
    hostHeader.addProperty("key", "host");
    hostHeader.addProperty("value", signedRequest.uri().getHost());
    headers.add(hostHeader);

    JsonObject envelope = new JsonObject();
    envelope.addProperty("url", signedRequest.uri().toString());
    envelope.addProperty("method", signedRequest.method());
    envelope.add("headers", headers);

    // Google STS reads the subject token URL-encoded. The provider infers the
    // AWS4 token type from the leading brace, so the encoded envelope is what
    // it expects.
    return URLEncoder.encode(envelope.toString(), StandardCharsets.UTF_8);
  }
}
