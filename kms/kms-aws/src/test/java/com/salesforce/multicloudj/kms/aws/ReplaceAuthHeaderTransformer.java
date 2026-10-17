package com.salesforce.multicloudj.kms.aws;

import com.github.tomakehurst.wiremock.extension.requestfilter.RequestFilterAction;
import com.github.tomakehurst.wiremock.extension.requestfilter.RequestWrapper;
import com.github.tomakehurst.wiremock.extension.requestfilter.StubRequestFilterV2;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

/**
 * Re-signs requests forwarded by WireMock while recording, so the proxy's rewritten Host header
 * still produces a valid signature. Replay does not need signatures and passes requests through.
 */
public class ReplaceAuthHeaderTransformer implements StubRequestFilterV2 {

  private static final String HOST = "kms.us-west-2.amazonaws.com";
  private static final String REGION = "us-west-2";

  @Override
  public String getName() {
    return "kms-replace-auth-header-transformer";
  }

  @Override
  public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
    if (System.getProperty("record") == null) {
      return RequestFilterAction.continueWith(request);
    }

    SignedRequest signedRequest;
    try {
      signedRequest = computeSignedRequest(request);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    RequestWrapper.Builder builder = RequestWrapper.create();
    signedRequest
        .request()
        .headers()
        .forEach(
            (headerName, headerValues) -> {
              if (headerName.equalsIgnoreCase("Authorization")
                  || headerName.startsWith("X-Amz-")
                  || headerName.equalsIgnoreCase("Host")) {
                builder.transformHeader(headerName, (input) -> headerValues);
              }
            });

    return RequestFilterAction.continueWith(builder.wrap(request));
  }

  private SignedRequest computeSignedRequest(Request request) throws URISyntaxException {
    SdkHttpFullRequest.Builder requestToSign =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.valueOf(request.getMethod().toString()))
            .contentStreamProvider(() -> new ByteArrayInputStream(request.getBody()))
            .uri(new URI(request.getAbsoluteUrl()));

    request
        .getHeaders()
        .all()
        .forEach(
            header -> {
              String headerKey = header.key();
              if (headerKey != null
                  && !headerKey.equalsIgnoreCase("Authorization")
                  && !headerKey.equalsIgnoreCase("Host")
                  && !headerKey.toLowerCase().startsWith("x-amz-")) {
                requestToSign.putHeader(headerKey, header.values());
              }
            });

    requestToSign.putHeader("Host", HOST);
    requestToSign.putHeader("Content-Length", String.valueOf(request.getBody().length));

    return AwsV4HttpSigner.create()
        .sign(
            r ->
                r.identity(ProfileCredentialsProvider.create().resolveCredentials())
                    .request(requestToSign.build())
                    .payload(requestToSign.contentStreamProvider())
                    .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "kms")
                    .putProperty(AwsV4HttpSigner.REGION_NAME, REGION));
  }
}
