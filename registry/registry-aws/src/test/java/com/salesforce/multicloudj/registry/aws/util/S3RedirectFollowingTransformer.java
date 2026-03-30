package com.salesforce.multicloudj.registry.aws.util;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves cross-host S3 redirects that WireMock cannot replay.
 *
 * <p>AWS ECR returns 307 redirects to S3 pre-signed URLs for blob downloads. Since S3 is a
 * different host, WireMock's proxy recording cannot capture the S3 response correctly. This
 * transformer runs at recording time: it follows the S3 URL directly, downloads the blob, and
 * replaces the 307 stub with a 200 containing the actual content as base64Body.
 */
public class S3RedirectFollowingTransformer extends StubMappingTransformer {

  private static final Logger logger =
      LoggerFactory.getLogger(S3RedirectFollowingTransformer.class);

  @Override
  public StubMapping transform(StubMapping stub, FileSource files, Parameters parameters) {
    ResponseDefinition response = stub.getResponse();
    if (response == null) {
      return stub;
    }

    // Only process redirect responses
    int status = response.getStatus();
    if (status != 307 && status != 302) {
      return stub;
    }

    // Only process redirects to S3
    HttpHeaders headers = response.getHeaders();
    if (headers == null) {
      return stub;
    }
    HttpHeader locationHeader = headers.getHeader("Location");
    if (!locationHeader.isPresent()) {
      return stub;
    }
    String location = locationHeader.firstValue();
    if (!isS3Url(location)) {
      return stub;
    }

    // Follow the S3 pre-signed URL and inline the blob content
    try {
      byte[] content = downloadContent(location);
      stub.setResponse(
          aResponse()
              .withStatus(200)
              .withHeader("Content-Type", "application/octet-stream")
              .withBase64Body(Base64.getEncoder().encodeToString(content))
              .build());
    } catch (Exception e) {
      logger.warn("Failed to resolve S3 redirect, keeping original response", e);
    }

    return stub;
  }

  private static boolean isS3Url(String url) {
    return url.contains(".s3.") || url.contains("s3.amazonaws.com");
  }

  /** Downloads content directly from S3 (pre-signed URL includes auth in query params). */
  private static byte[] downloadContent(String url) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(30000);
    connection.setReadTimeout(30000);

    int responseCode = connection.getResponseCode();
    if (responseCode != 200) {
      throw new RuntimeException("S3 download failed with HTTP " + responseCode);
    }

    try (InputStream is = connection.getInputStream()) {
      return is.readAllBytes();
    } finally {
      connection.disconnect();
    }
  }

  @Override
  public String getName() {
    return "s3-redirect-following-transformer";
  }

  @Override
  public boolean applyGlobally() {
    return true;
  }
}
