package com.salesforce.multicloudj.blob.gcp.util;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for GCP blob store test transformations.
 * Handles post-recording transformation of WireMock stub files to convert
 * exact URLs with UUIDs to regex patterns for multipart upload requests.
 */
public class GcpBlobStoreTestUtil {

    /**
     * Post-recording file transformation: reads recorded stub JSON files and converts
     * exact URLs containing multipart UUIDs to URL patterns with regex.
     */
    public static void transformMultipartStubFiles() {
        boolean isRecordingEnabled = System.getProperty("record") != null;
        if (!isRecordingEnabled) {
            return; // Only transform after recording
        }

        try {
            String rootDir = "src/test/resources";
            File mappingsDir = new File(rootDir, "mappings");

            if (!mappingsDir.exists() || !mappingsDir.isDirectory()) {
                return;
            }

            System.out.println("[GcpBlobStoreTestUtil] Post-recording transformation: scanning stub files for multipart URLs");

            File[] mappingFiles = mappingsDir.listFiles((dir, name) -> name.endsWith(".json"));
            if (mappingFiles == null || mappingFiles.length == 0) {
                return;
            }

            // UUID pattern: 8-4-4-4-12 hex digits
            Pattern uuidPattern = Pattern.compile(
                "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
            );


            for (File mappingFile : mappingFiles) {
                try {
                    String jsonContent = Files.readString(mappingFile.toPath(), StandardCharsets.UTF_8);

                    // Only target multipart upload recordings:
                    // URL must contain "multipart-" in the path (e.g., "conformance-tests/multipart-...")
                    // This ensures we only transform multipart upload test recordings, not other resumable uploads
                    boolean isMultipartUrl = jsonContent.contains("\"url\"") &&
                                            jsonContent.contains("multipart-") &&
                                            uuidPattern.matcher(jsonContent).find();
                    boolean isMultipartUpload = jsonContent.contains("uploadType=multipart") && jsonContent.contains("multipart-");
                    boolean hasUploadId = jsonContent.contains("upload_id=") && jsonContent.contains("multipart-");

                    // Only transform if it's a multipart upload (must have "multipart-" in URL)
                    if (isMultipartUrl || isMultipartUpload) {
                        String transformedJson = transformStubFileContent(jsonContent, uuidPattern, isMultipartUpload, hasUploadId);

                        if (!transformedJson.equals(jsonContent)) {
                            Files.write(
                                mappingFile.toPath(),
                                transformedJson.getBytes(StandardCharsets.UTF_8)
                            );
                        }
                    }
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
        }
    }

    /**
     * Transforms a stub JSON file content by converting exact URLs with UUIDs to URL patterns,
     * making upload_id parameters flexible, and removing body patterns for multipart uploads.
     */
    private static String transformStubFileContent(String jsonContent, Pattern uuidPattern, 
                                                     boolean isMultipartUpload, boolean hasUploadId) {
        try {
            String transformedJson = jsonContent;

            // Step 1: Transform URL to urlPattern if it contains multipart with UUID or has upload_id
            Pattern urlFieldPattern = Pattern.compile(
                "\"url\"\\s*:\\s*\"([^\"]+)\"",
                Pattern.DOTALL
            );
            Matcher urlMatcher = urlFieldPattern.matcher(transformedJson);

            if (urlMatcher.find()) {
                String originalUrl = urlMatcher.group(1);
                boolean needsTransformation = false;
                String urlPatternRegex = originalUrl;

                // Step 1: Handle multipart UUID transformation first
                if (originalUrl.contains("multipart-") && uuidPattern.matcher(originalUrl).find()) {
                    // Convert URL to regex pattern (this already handles escaping)
                    urlPatternRegex = convertUrlToRegexPattern(originalUrl, uuidPattern);
                    needsTransformation = true;
                    System.out.println("[GcpBlobStoreTestUtil]   Original URL: " + originalUrl);
                    System.out.println("[GcpBlobStoreTestUtil]   Pattern: " + urlPatternRegex);
                } else {
                    // Escape the URL for JSON first
                    urlPatternRegex = escapeRegexSpecialCharsForJson(originalUrl);
                }

                // Step 2: Handle upload_id replacement AFTER escaping (only for multipart uploads)
                // The pattern [^&]+ should be inserted literally - brackets don't need JSON escaping
                // But + needs to be escaped for regex (as \+) and then JSON-escaped (as \\+)
                if (hasUploadId && originalUrl.contains("upload_id=") && originalUrl.contains("multipart-")) {
                    // Replace upload_id value with pattern [^&]+
                    // In regex: [^&]+
                    // In JSON string: [^&]\+ (only + needs escaping for regex)
                    // In Java string: "[^&]\\\\+" (backslash needs escaping in Java string)
                    String uploadIdPattern = "[^&]\\\\+";  // This becomes [^&]\+ in JSON, which is regex [^&]+
                    urlPatternRegex = urlPatternRegex.replaceAll(
                        Pattern.quote("upload_id=") + "[^&]+", 
                        "upload_id=" + uploadIdPattern
                    );
                    needsTransformation = true;
                    System.out.println("[GcpBlobStoreTestUtil]   Made upload_id flexible in URL");
                }

                if (needsTransformation) {
                    // Replace "url": "..." with "urlPattern": "..."
                    // Pattern is already JSON-escaped (backslashes doubled from convertUrlToRegexPattern/escapeRegexSpecialCharsForJson)
                    // Only need to escape quotes if they appear in the pattern
                    String jsonEscapedPattern = urlPatternRegex.replace("\"", "\\\"");
                    transformedJson = urlMatcher.replaceFirst(
                        "\"urlPattern\" : \"" + jsonEscapedPattern + "\""
                    );
                }
            }

            // Step 2: Remove body patterns for multipart uploads (boundaries change)
            if (isMultipartUpload && transformedJson.contains("\"bodyPatterns\"")) {
                // Find the bodyPatterns field and remove it properly
                // Pattern: "bodyPatterns" : [ ... ] with optional leading comma
                // We need to match the entire array structure, which can be nested
                Pattern bodyPatternsWithComma = Pattern.compile(
                    ",\\s*\"bodyPatterns\"\\s*:\\s*\\[[^\\[\\]]*(?:\\[[^\\[\\]]*\\][^\\[\\]]*)*\\]",
                    Pattern.DOTALL
                );
                Matcher bodyMatcher = bodyPatternsWithComma.matcher(transformedJson);
                if (bodyMatcher.find()) {
                    transformedJson = bodyMatcher.replaceFirst("");
                    System.out.println("[GcpBlobStoreTestUtil]   Removed body pattern field for multipart upload");
                } else {
                    // Try without leading comma (in case it's the last field before closing brace)
                    Pattern bodyPatternsNoComma = Pattern.compile(
                        "\"bodyPatterns\"\\s*:\\s*\\[[^\\[\\]]*(?:\\[[^\\[\\]]*\\][^\\[\\]]*)*\\]",
                        Pattern.DOTALL
                    );
                    Matcher bodyMatcherNoComma = bodyPatternsNoComma.matcher(transformedJson);
                    if (bodyMatcherNoComma.find()) {
                        int start = bodyMatcherNoComma.start();
                        String before = transformedJson.substring(0, start);
                        String after = transformedJson.substring(bodyMatcherNoComma.end());
                        // Remove trailing comma and whitespace before bodyPatterns
                        before = before.replaceAll(",\\s*$", "");
                        transformedJson = before + after;
                        System.out.println("[GcpBlobStoreTestUtil]   Removed body pattern field for multipart upload");
                    }
                }
            }

            return transformedJson;
        } catch (Exception e) {
            System.err.println("[GcpBlobStoreTestUtil] ERROR transforming stub content: " + e.getMessage());
            e.printStackTrace();
            return jsonContent;
        }
    }

    /**
     * Escapes regex special characters for JSON.
     * Note: We don't escape ? because in URLs it's a literal query parameter separator.
     * Only escape characters that are regex metacharacters when used in a regex pattern.
     */
    private static String escapeRegexSpecialCharsForJson(String pattern) {
        return pattern.replace("\\", "\\\\")
                     .replace(".", "\\\\.")
                     .replace("^", "\\\\^")
                     .replace("$", "\\\\$")
                     .replace("*", "\\\\*")
                     .replace("+", "\\\\+")
                     .replace("(", "\\\\(")
                     .replace(")", "\\\\)")
                     .replace("{", "\\\\{")
                     .replace("}", "\\\\}")
                     .replace("|", "\\\\|")
                     .replace("[", "\\\\[")
                     .replace("]", "\\\\]");
    }

    /**
     * Converts a URL with UUIDs to a regex pattern.
     * Replaces UUIDs with [^/&%]+ and part numbers with \d+
     * Note: This produces a pattern that will be written to JSON, so backslashes need JSON escaping.
     */
    private static String convertUrlToRegexPattern(String url, Pattern uuidPattern) {
        // Use placeholders to protect regex patterns during escaping
        final String UUID_PLACEHOLDER = "___UUID_PLACEHOLDER___";
        final String PART_NUMBER_PLACEHOLDER = "___PART_NUMBER_PLACEHOLDER___";

        // Step 1: Replace UUIDs with placeholder
        String pattern = uuidPattern.matcher(url).replaceAll(UUID_PLACEHOLDER);

        // Step 2: Replace part numbers with placeholder
        pattern = pattern.replaceAll("/part-(\\d+)", "/part-" + PART_NUMBER_PLACEHOLDER);

        // Step 3: Escape all regex special characters for JSON
        // In JSON strings, backslash is an escape character, so:
        //   - To get \d in the regex, we need \\d in JSON
        // So we're essentially adding ONE backslash before each special char for regex,
        // and then JSON escaping will keep it as ONE backslash when parsed
        // Note: We don't escape ? because in URLs it's a literal query parameter separator
        pattern = pattern.replace("\\", "\\\\")
                         .replace(".", "\\\\.")
                         .replace("^", "\\\\^")
                         .replace("$", "\\\\$")
                         .replace("*", "\\\\*")
                         .replace("+", "\\\\+")
                         .replace("(", "\\\\(")
                         .replace(")", "\\\\)")
                         .replace("{", "\\\\{")
                         .replace("}", "\\\\}")
                         .replace("|", "\\\\|")
                         .replace("[", "\\\\[")
                         .replace("]", "\\\\]");

        // Step 4: Replace placeholders with actual regex patterns
        // UUID pattern: [^/&%]+ - brackets don't need escaping in JSON strings
        // But we escaped them above, so the replacement will have escaped brackets
        // We need to unescape them after replacement
        pattern = pattern.replace(UUID_PLACEHOLDER, "\\\\[^/&%\\\\]+");
        // After replacement, unescape the brackets for the UUID pattern
        pattern = pattern.replace("\\\\[^/&%\\\\]+", "[^/&%]+");
        // Part number pattern: Use [0-9]+ instead of \d+ to avoid JSON escape issues
        pattern = pattern.replace(PART_NUMBER_PLACEHOLDER, "\\\\[0-9\\\\]+");
        // After replacement, unescape the brackets for the part number pattern
        pattern = pattern.replace("\\\\[0-9\\\\]+", "[0-9]+");

        return pattern;
    }
}

