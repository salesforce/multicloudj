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
 */
public class MultipartBoundaryTransformer extends StubMappingTransformer {

  private static final Pattern BOUNDARY_PATTERN =
      Pattern.compile("--(__END_OF_PART__[a-f0-9-]+__)");

  private static final String BOUNDARY_REGEX = "--__END_OF_PART__[a-f0-9-]+__";

  @Override
  public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
    String url = stubMapping.getRequest().getUrl();
    if (url == null || !url.contains("uploadType=multipart")) {
      return stubMapping;
    }

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
          // Split body by the recorded boundary, escape each segment, rejoin with wildcard
          String[] segments = textContent.split(Pattern.quote(boundaryLiteral), -1);
          StringBuilder regex = new StringBuilder("(?s)");
          for (int i = 0; i < segments.length; i++) {
            regex.append(Pattern.quote(segments[i]));
            if (i < segments.length - 1) {
              regex.append(BOUNDARY_REGEX);
            }
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

  @Override
  public String getName() {
    return "multipart-boundary-transformer";
  }
}
