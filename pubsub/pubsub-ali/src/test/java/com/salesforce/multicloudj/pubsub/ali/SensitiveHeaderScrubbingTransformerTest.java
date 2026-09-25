package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.common.Json;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SensitiveHeaderScrubbingTransformerTest {

  private final SensitiveHeaderScrubbingTransformer transformer =
      new SensitiveHeaderScrubbingTransformer();

  @AfterEach
  void clearProps() {
    System.clearProperty(SensitiveHeaderScrubbingTransformer.ALLOW_HEADERS_PROPERTY);
    System.clearProperty(SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY);
  }

  @Test
  void redactsConfiguredValueInBothRequestMatcherAndResponse() {
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, "1234567890123456=account-id");
    // The account id appears in a request-body matcher (a resource identifier) AND a response body
    // -- a response-only redaction would leave the matcher unmatchable on replay.
    String json =
        "{"
            + "\"request\":{\"method\":\"PUT\",\"url\":\"/topics/t/subscriptions/s\","
            + "\"bodyPatterns\":[{\"equalToXml\":"
            + "\"<E>acs:mns:cn-shanghai:1234567890123456:queues/q</E>\"}]},"
            + "\"response\":{\"status\":200,"
            + "\"body\":\"<X>1234567890123456.mns.cn-shanghai.aliyuncs.com</X>\"}"
            + "}";

    StubMapping out = transformer.transform(StubMapping.buildFrom(json), null, null);

    String outJson = Json.write(out);
    assertFalse(outJson.contains("1234567890123456"), "account id must be redacted everywhere");
    assertTrue(outJson.contains("account-id"), "placeholder must replace the account id");
  }

  @Test
  void redactsAccountIdOnlyOnWordBoundaries() {
    System.setProperty(SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, "12345=ACCOUNT");
    // "12345" stands alone in the host but is only a substring of the request id: the request id
    // must survive untouched so the recorded stub stays byte-accurate on replay.
    String json =
        "{\"request\":{\"method\":\"GET\",\"url\":\"/q\"},"
            + "\"response\":{\"status\":200,"
            + "\"headers\":{\"x-mns-request-id\":\"AB12345CD\"},"
            + "\"body\":\"12345.mns.cn-shanghai.aliyuncs.com\"}}";

    StubMapping out = transformer.transform(StubMapping.buildFrom(json), null, null);

    String outJson = Json.write(out);
    assertTrue(outJson.contains("ACCOUNT.mns"), "standalone account id must be redacted");
    assertTrue(outJson.contains("AB12345CD"), "account id inside a longer id must be preserved");
  }

  @Test
  void appliesMultipleRedactionPairsInOnePass() {
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY,
        "1234567890123456=account-id,cn-shanghai=REGION");
    String json =
        "{\"request\":{\"method\":\"GET\",\"url\":\"/q\"},"
            + "\"response\":{\"status\":200,"
            + "\"body\":\"1234567890123456.mns.cn-shanghai.aliyuncs.com\"}}";

    StubMapping out = transformer.transform(StubMapping.buildFrom(json), null, null);

    String outJson = Json.write(out);
    assertFalse(outJson.contains("1234567890123456"), "first pair must be redacted");
    assertFalse(outJson.contains("cn-shanghai"), "second pair must be redacted");
    assertTrue(outJson.contains("account-id"), "first placeholder must be present");
    assertTrue(outJson.contains("REGION"), "second placeholder must be present");
  }

  @Test
  void keepsOnlyAllowlistedResponseHeaders() {
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.ALLOW_HEADERS_PROPERTY, "x-mns-version");
    String json =
        "{\"request\":{\"method\":\"GET\",\"url\":\"/q\"},"
            + "\"response\":{\"status\":200,\"headers\":{"
            + "\"x-mns-access-key\":\"secret\",\"x-mns-version\":\"2015-06-06\"}}}";

    StubMapping out = transformer.transform(StubMapping.buildFrom(json), null, null);

    String outJson = Json.write(out);
    assertFalse(outJson.contains("x-mns-access-key"), "header off the allowlist must be dropped");
    assertTrue(outJson.contains("x-mns-version"), "allowlisted header must be kept");
  }

  @Test
  void matchesAllowlistCaseInsensitively() {
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.ALLOW_HEADERS_PROPERTY, "x-mns-version");
    // The allowlist is lower-cased; a mixed-case allowlisted header is still kept, while a
    // mixed-case header that is not on the allowlist is still dropped.
    String json =
        "{\"request\":{\"method\":\"GET\",\"url\":\"/q\"},"
            + "\"response\":{\"status\":200,\"headers\":{"
            + "\"X-Mns-Access-Key\":\"secret\",\"X-Mns-Version\":\"2015-06-06\"}}}";

    StubMapping out = transformer.transform(StubMapping.buildFrom(json), null, null);

    String outJson = Json.write(out);
    assertFalse(outJson.contains("X-Mns-Access-Key"), "mixed-case sensitive header dropped");
    assertTrue(outJson.contains("X-Mns-Version"), "mixed-case allowlisted header must be kept");
  }

  @Test
  void noOpWhenNeitherPropertyIsSet() {
    String json =
        "{\"request\":{\"method\":\"GET\",\"url\":\"/q\"},"
            + "\"response\":{\"status\":200,\"body\":\"1234567890123456\"}}";

    StubMapping out = transformer.transform(StubMapping.buildFrom(json), null, null);

    assertTrue(Json.write(out).contains("1234567890123456"), "nothing redacted without config");
  }

  @Test
  void redactsValueContainingComma() {
    // A comma in the raw value is escaped in the property grammar so the pair still parses.
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, "foo\\,bar=REDACTED");
    StubMapping out =
        transformer.transform(StubMapping.buildFrom(stubWithBody("x foo,bar y")), null, null);
    // buildFrom throws on malformed JSON, so a successful reparse proves the output stays valid.
    String body = StubMapping.buildFrom(Json.write(out)).getResponse().getBody();
    assertFalse(body.contains("foo,bar"), "comma in value must be redacted");
    assertTrue(body.contains("REDACTED"), "placeholder must be present");
  }

  @Test
  void redactsValueContainingEquals() {
    // An equals in the raw value is escaped so it is not mistaken for the key/value separator.
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, "tok\\=en=REDACTED");
    StubMapping out =
        transformer.transform(StubMapping.buildFrom(stubWithBody("x tok=en y")), null, null);
    String body = StubMapping.buildFrom(Json.write(out)).getResponse().getBody();
    assertFalse(body.contains("tok=en"), "equals in value must be redacted");
    assertTrue(body.contains("REDACTED"), "placeholder must be present");
  }

  @Test
  void redactsValueContainingDoubleQuote() {
    // The raw value's JSON-escaped form (a\"b) is what appears in the serialized stub.
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, "a\"b=REDACTED");
    StubMapping out =
        transformer.transform(StubMapping.buildFrom(stubWithBody("x a\"b y")), null, null);
    String body = StubMapping.buildFrom(Json.write(out)).getResponse().getBody();
    assertFalse(body.contains("a\"b"), "double-quote value must be redacted");
    assertTrue(body.contains("REDACTED"), "placeholder must be present");
  }

  @Test
  void redactsValueContainingBackslash() {
    // Grammar-escape the backslash in the property; its JSON-escaped form (a\\b) matches the stub.
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, "a\\\\b=REDACTED");
    StubMapping out =
        transformer.transform(StubMapping.buildFrom(stubWithBody("x a\\b y")), null, null);
    String body = StubMapping.buildFrom(Json.write(out)).getResponse().getBody();
    assertFalse(body.contains("a\\b"), "backslash value must be redacted");
    assertTrue(body.contains("REDACTED"), "placeholder must be present");
  }

  @Test
  void redactsValueContainingControlChar() {
    // A control char (newline) is escaped as \n in the serialized stub; redaction must match that.
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, "a\nb=REDACTED");
    StubMapping out =
        transformer.transform(StubMapping.buildFrom(stubWithBody("x a\nb y")), null, null);
    String body = StubMapping.buildFrom(Json.write(out)).getResponse().getBody();
    assertFalse(body.contains("\n"), "control char in value must be redacted");
    assertTrue(body.contains("REDACTED"), "placeholder must be present");
  }

  @Test
  void keepsOutputValidJsonWhenPlaceholderHasSpecialChars() {
    // The placeholder itself carries a quote and a backslash; it must be JSON-escaped on the way in
    // so the redacted stub stays valid JSON (a successful reparse below proves validity).
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, "secret=a\"b\\\\c");
    StubMapping out =
        transformer.transform(StubMapping.buildFrom(stubWithBody("secret")), null, null);
    String body = StubMapping.buildFrom(Json.write(out)).getResponse().getBody();
    assertTrue(body.contains("a\"b\\c"), "placeholder must be substituted verbatim");
    assertFalse(body.contains("secret"), "raw value must be redacted");
  }

  private String stubWithBody(String body) {
    return "{\"request\":{\"method\":\"GET\",\"url\":\"/q\"},"
        + "\"response\":{\"status\":200,\"body\":"
        + Json.write(body)
        + "}}";
  }
}
