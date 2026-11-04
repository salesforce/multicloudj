package com.salesforce.multicloudj.blob.gcp.util;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.matching.BinaryEqualToPattern;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * WireMock transformer that converts multipart upload request body matchers from exact binary
 * matching to regex-based matching. This allows replay tests to work despite randomized multipart
 * boundaries.
 *
 * <p>For GCP Storage multipart uploads, the boundary string changes on every request. This
 * transformer regex-escapes the recorded body and replaces the specific boundary with a wildcard
 * pattern, so any boundary is accepted during replay.
 *
 * <p>Requests to the batch endpoint carry per-part values that change on every request (part
 * Content-Length, idempotency token, client version and authorization headers). For those requests
 * only the part order, the {@code content-id} of each part and each part's HTTP request line are
 * matched; everything else in a part is wildcarded. If the recorded body is not a well-formed
 * batch, the generic boundary handling is used.
 */
public class MultipartBoundaryTransformer extends StubMappingTransformer {

  private static final Pattern BOUNDARY_PATTERN =
      Pattern.compile("--(__END_OF_PART__[a-f0-9-]+__)");

  private static final String BOUNDARY_REGEX = "--__END_OF_PART__[a-f0-9-]+__";

  private static final String BATCH_URL = "/batch/storage/v1";

  // Matches any text that does not run into the next part boundary.
  private static final String WITHIN_PART_REGEX = "(?:(?!--__END_OF_PART__)[\\s\\S])*?";

  private static final Pattern CONTENT_ID_PATTERN = Pattern.compile("\r\ncontent-id: (\\d+)\r\n");

  private static final Pattern REQUEST_LINE_PATTERN =
      Pattern.compile("\r\n\r\n([A-Z]+ \\S+ HTTP/1\\.1)\r\n");

  @Override
  public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
    List<ContentPattern<?>> bodyPatterns = stubMapping.getRequest().getBodyPatterns();
    if (bodyPatterns == null || bodyPatterns.isEmpty()) {
      return stubMapping;
    }

    List<ContentPattern<?>> newPatterns = new ArrayList<>();
    boolean modified = false;

    for (ContentPattern<?> pattern : bodyPatterns) {
      if (pattern instanceof BinaryEqualToPattern) {
        String base64Content = pattern.getExpected();
        byte[] binaryContent = Base64.getDecoder().decode(base64Content);
        String textContent = new String(binaryContent, StandardCharsets.UTF_8);

        Matcher matcher = BOUNDARY_PATTERN.matcher(textContent);
        if (matcher.find()) {
          String boundaryLiteral = "--" + matcher.group(1);
          String batchRegex =
              BATCH_URL.equals(stubMapping.getRequest().getUrl())
                  ? buildBatchRegex(textContent, boundaryLiteral)
                  : null;
          if (batchRegex != null) {
            newPatterns.add(new RegexPattern(batchRegex));
            modified = true;
            continue;
          }
          // Replace boundary with wildcard, then quote the remaining literal parts
          String[] parts = textContent.split(Pattern.quote(boundaryLiteral), -1);
          StringBuilder regex = new StringBuilder("(?s)");
          for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
              regex.append(BOUNDARY_REGEX);
            }
            regex.append(Pattern.quote(parts[i]));
          }
          newPatterns.add(new RegexPattern(regex.toString()));
          modified = true;
          continue;
        }
      }
      newPatterns.add(pattern);
    }

    if (modified) {
      bodyPatterns.clear();
      bodyPatterns.addAll(newPatterns);
    }

    return stubMapping;
  }

  /**
   * Builds a regex that matches a batch body on part order, {@code content-id} and request line
   * only, or returns null if the body is not a well-formed batch.
   */
  private static String buildBatchRegex(String textContent, String boundaryLiteral) {
    String[] sections = textContent.split(Pattern.quote(boundaryLiteral), -1);
    if (sections.length < 3
        || !sections[0].isEmpty()
        || !sections[sections.length - 1].trim().equals("--")) {
      return null;
    }
    StringBuilder regex = new StringBuilder("(?s)");
    for (int i = 1; i < sections.length - 1; i++) {
      Matcher contentId = CONTENT_ID_PATTERN.matcher(sections[i]);
      Matcher requestLine = REQUEST_LINE_PATTERN.matcher(sections[i]);
      if (!contentId.find() || !requestLine.find()) {
        return null;
      }
      regex
          .append(BOUNDARY_REGEX)
          .append(WITHIN_PART_REGEX)
          .append(Pattern.quote("\r\ncontent-id: " + contentId.group(1) + "\r\n"))
          .append(WITHIN_PART_REGEX)
          .append(Pattern.quote("\r\n\r\n" + requestLine.group(1) + "\r\n"))
          .append(WITHIN_PART_REGEX);
    }
    return regex.append(BOUNDARY_REGEX).append("--\\s*").toString();
  }

  @Override
  public String getName() {
    return "multipart-boundary-transformer";
  }
}
