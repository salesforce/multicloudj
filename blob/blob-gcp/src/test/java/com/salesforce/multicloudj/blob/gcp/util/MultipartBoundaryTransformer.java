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
 * transformer extracts the actual content and metadata from the recorded request and creates a
 * regex pattern that matches any boundary while verifying the important parts (JSON metadata and
 * file content).
 */
public class MultipartBoundaryTransformer extends StubMappingTransformer {

  // Pattern to detect multipart boundaries in the format: --__END_OF_PART__<uuid>__
  private static final Pattern BOUNDARY_PATTERN =
      Pattern.compile("--(__END_OF_PART__[a-f0-9-]+__)");

  @Override
  public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
    // Only process mappings with uploadType=multipart in the URL
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
        // Decode the base64 binary content
        String base64Content = pattern.getExpected();
        byte[] binaryContent = Base64.getDecoder().decode(base64Content);
        String textContent = new String(binaryContent, StandardCharsets.UTF_8);

        // Extract the boundary string
        Matcher matcher = BOUNDARY_PATTERN.matcher(textContent);
        if (matcher.find()) {
          String originalBoundary = matcher.group(1);

          // Extract the important parts: JSON metadata and file content
          String[] parts = textContent.split("--" + Pattern.quote(originalBoundary));
          if (parts.length >= 3) {
            // parts[1] contains JSON metadata, parts[2] contains file content
            String jsonPart = extractJsonContent(parts[1]);
            String filePart = extractFileContent(parts[2]);

            if (jsonPart != null && filePart != null) {
              // Create a regex pattern that matches any boundary but verifies content
              String regexPattern = createFlexiblePattern(jsonPart, filePart);
              newPatterns.add(new RegexPattern(regexPattern));
              modified = true;
              continue;
            }
          }
        }
      }
      // Keep original pattern if we couldn't transform it
      newPatterns.add(pattern);
    }

    if (modified) {
      bodyPatterns.clear();
      bodyPatterns.addAll(newPatterns);
    }

    return stubMapping;
  }

  /**
   * Extracts JSON content from a multipart part. The part contains headers followed by JSON.
   */
  private String extractJsonContent(String part) {
    // Find the JSON object (starts with { and ends with })
    int jsonStart = part.indexOf('{');
    int jsonEnd = part.lastIndexOf('}');
    if (jsonStart >= 0 && jsonEnd > jsonStart) {
      return part.substring(jsonStart, jsonEnd + 1).trim();
    }
    return null;
  }

  /**
   * Extracts file content from a multipart part. The part contains headers followed by the actual
   * file data.
   */
  private String extractFileContent(String part) {
    // Skip headers (they end with \r\n\r\n)
    int contentStart = part.indexOf("\r\n\r\n");
    if (contentStart >= 0) {
      contentStart += 4; // Skip the \r\n\r\n
      int contentEnd = part.lastIndexOf('\r');
      if (contentEnd > contentStart) {
        return part.substring(contentStart, contentEnd).trim();
      }
    }
    return null;
  }

  /**
   * Creates a flexible regex pattern that matches multipart requests with any boundary string but
   * verifies the JSON metadata and file content are present.
   */
  private String createFlexiblePattern(String jsonContent, String fileContent) {
    // Escape special regex characters in the content
    String escapedJson = Pattern.quote(jsonContent);
    String escapedFile = Pattern.quote(fileContent);

    // Create a pattern that:
    // 1. Matches any boundary string (--__END_OF_PART__<anything>__)
    // 2. Verifies the JSON content is present
    // 3. Verifies the file content is present
    return "(?s).*--__END_OF_PART__[a-f0-9-]+__.*"
        + "Content-Type: application/json.*"
        + escapedJson
        + ".*--__END_OF_PART__[a-f0-9-]+__.*"
        + "Content-Type: application/octet-stream.*"
        + escapedFile
        + ".*--__END_OF_PART__[a-f0-9-]+__--.*";
  }

  @Override
  public String getName() {
    return "multipart-boundary-transformer";
  }
}
