package com.salesforce.multicloudj.pubsub.ali;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.common.Json;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provider-neutral WireMock stub transformer that scrubs recorded stubs before they are persisted:
 * it keeps only an allowlisted set of RESPONSE headers -- dropping every other response header, so
 * a credential or session token a cloud service echoes back in an unforeseen header is never
 * committed -- and redacts caller-configured literal values wherever they appear in the recorded
 * stub -- request matchers as well as the response -- so an account-scoped identifier (which can
 * appear in a request-body matcher, not just responses) can be replaced with a placeholder.
 *
 * <p>Both are driven by comma-separated system properties: {@link #ALLOW_HEADERS_PROPERTY} lists
 * the response header names to keep (case-insensitive); any header not on that allowlist is
 * dropped. {@link #REDACT_VALUES_PROPERTY} lists {@code raw=placeholder} pairs to replace; a
 * {@code raw} that is a single word-character token (for example a numeric account id) is replaced
 * only on word boundaries so it is not over-redacted inside a longer identifier. When both
 * properties are unset/empty this transformer is a no-op. It is registered per-recording rather
 * than globally ({@link #applyGlobally()} returns {@code false}). All names and values are supplied
 * by the caller, so this class hardcodes none and stays free of any provider-specific data.
 */
public class SensitiveHeaderScrubbingTransformer extends StubMappingTransformer {

  /** System property naming the comma-separated allowlist of response headers to keep in stubs. */
  public static final String ALLOW_HEADERS_PROPERTY = "multicloudj.wiremock.allowResponseHeaders";

  /**
   * System property naming comma-separated {@code raw=placeholder} pairs whose {@code raw} literal
   * is replaced with {@code placeholder} across the recorded stub (request matchers and response).
   * A backslash escapes a literal {@code ,}, {@code =}, or {@code \} so raw values or placeholders
   * may contain those characters; plain unescaped {@code raw=placeholder} pairs stay valid.
   */
  public static final String REDACT_VALUES_PROPERTY = "multicloudj.wiremock.redactValues";

  @Override
  public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
    Set<String> allowList = readAllowList();
    Map<String, String> redactions = readRedactions();
    if (allowList.isEmpty() && redactions.isEmpty()) {
      return stubMapping;
    }
    // 1. Structurally drop every response header that is not on the allowlist, so a header that
    // could carry a credential or session token is never committed even if it was not foreseen.
    ResponseDefinition response = stubMapping.getResponse();
    if (!allowList.isEmpty() && response != null && response.getHeaders() != null) {
      List<HttpHeader> kept = new ArrayList<>();
      boolean removedAny = false;
      for (HttpHeader header : response.getHeaders().all()) {
        if (allowList.contains(header.key().toLowerCase(Locale.ROOT))) {
          kept.add(header);
        } else {
          removedAny = true;
        }
      }
      if (removedAny) {
        stubMapping.setResponse(
            ResponseDefinitionBuilder.like(response).withHeaders(new HttpHeaders(kept)).build());
      }
    }
    // 2. Redact configured literals across the ENTIRE stub -- request matchers as well as the
    // response -- by round-tripping through JSON. The account id, for one, sits in a request-body
    // matcher (a resource identifier), not just response bodies, so a response-only redaction would
    // leave the recorded matcher unmatchable on replay.
    if (!redactions.isEmpty()) {
      String json = Json.write(stubMapping);
      for (Map.Entry<String, String> entry : redactions.entrySet()) {
        json = redact(json, entry.getKey(), entry.getValue());
      }
      stubMapping = StubMapping.buildFrom(json);
    }
    return stubMapping;
  }

  /** Parses the allowlist system property into a set of lower-cased header names to keep. */
  private static Set<String> readAllowList() {
    Set<String> allowList = new HashSet<>();
    String raw = System.getProperty(ALLOW_HEADERS_PROPERTY);
    if (raw == null || raw.trim().isEmpty()) {
      return allowList;
    }
    for (String name : raw.split(",")) {
      String trimmed = name.trim().toLowerCase(Locale.ROOT);
      if (!trimmed.isEmpty()) {
        allowList.add(trimmed);
      }
    }
    return allowList;
  }

  /**
   * Replaces {@code raw} with {@code placeholder} in the SERIALIZED {@code json}. Because the
   * search runs against serialized JSON, both operands are first converted to their
   * JSON-string-escaped form: a {@code raw} value containing a quote, backslash, or control
   * character is escaped inside the stub, and an unescaped replacement could corrupt the JSON.
   * Escaping both keeps the search matchable and the output valid JSON. When {@code raw} is a
   * single word-character token (for example a numeric account id) the replacement is
   * word-boundary-aware, so the literal is swapped only where it stands alone and never where it is
   * a substring of a longer identifier such as a message id, checksum, or request id (word
   * characters are JSON-safe, so their escaped form is unchanged). A literal that contains
   * delimiters is already delimiter-bounded, so it falls back to a plain substring replace.
   */
  private static String redact(String json, String raw, String placeholder) {
    String escapedRaw = jsonEscape(raw);
    String escapedPlaceholder = jsonEscape(placeholder);
    if (raw.matches("\\w+")) {
      return json.replaceAll(
          "\\b" + Pattern.quote(escapedRaw) + "\\b", Matcher.quoteReplacement(escapedPlaceholder));
    }
    return json.replace(escapedRaw, escapedPlaceholder);
  }

  /**
   * Returns the JSON-string-escaped body of {@code value}: the characters exactly as they appear
   * inside a serialized JSON string (quotes, backslashes, and control characters escaped). It
   * serializes with the same mapper that produced the stub JSON and strips the surrounding quotes,
   * so the escaped form is guaranteed to match how the value appears in the recorded stub.
   */
  private static String jsonEscape(String value) {
    String quoted = Json.write(value);
    return quoted.substring(1, quoted.length() - 1);
  }

  /**
   * Parses {@link #REDACT_VALUES_PROPERTY} into an ordered map of raw literal to placeholder. Pairs
   * are separated by {@code ,} and each pair is split on its first {@code =}; a backslash escapes a
   * literal {@code ,}, {@code =}, or {@code \} so raw values or placeholders may contain those
   * characters (for example base64 tokens ending in {@code =}). Plain unescaped
   * {@code raw=placeholder} pairs remain valid.
   */
  private static Map<String, String> readRedactions() {
    Map<String, String> redactions = new LinkedHashMap<>();
    String raw = System.getProperty(REDACT_VALUES_PROPERTY);
    if (raw == null || raw.trim().isEmpty()) {
      return redactions;
    }
    for (String pair : splitUnescaped(raw, ',')) {
      int eq = indexOfUnescaped(pair, '=');
      if (eq > 0) {
        String key = unescape(pair.substring(0, eq)).trim();
        String value = unescape(pair.substring(eq + 1)).trim();
        if (!key.isEmpty()) {
          redactions.put(key, value);
        }
      }
    }
    return redactions;
  }

  /**
   * Splits {@code input} on every occurrence of {@code delimiter} that is not escaped by a
   * preceding backslash. Escape sequences are left intact in the returned parts so a later
   * {@link #unescape} call can resolve them.
   */
  private static List<String> splitUnescaped(String input, char delimiter) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (escaped) {
        current.append(c);
        escaped = false;
      } else if (c == '\\') {
        current.append(c);
        escaped = true;
      } else if (c == delimiter) {
        parts.add(current.toString());
        current.setLength(0);
      } else {
        current.append(c);
      }
    }
    parts.add(current.toString());
    return parts;
  }

  /** Returns the index of the first {@code target} in {@code s} not escaped by a backslash. */
  private static int indexOfUnescaped(String s, char target) {
    boolean escaped = false;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == target) {
        return i;
      }
    }
    return -1;
  }

  /** Resolves backslash escapes ({@code \x} becomes {@code x}) in {@code s}. */
  private static String unescape(String s) {
    StringBuilder sb = new StringBuilder(s.length());
    boolean escaped = false;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (escaped) {
        sb.append(c);
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else {
        sb.append(c);
      }
    }
    if (escaped) {
      sb.append('\\');
    }
    return sb.toString();
  }

  @Override
  public String getName() {
    return "sensitive-header-scrubbing-transformer";
  }

  @Override
  public boolean applyGlobally() {
    return false;
  }
}
