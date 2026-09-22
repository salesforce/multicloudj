package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class SmqTopicEnvelopeTest {

  /**
   * A probe-shaped JSON envelope carrying all eight fields a JSON-format SMQ topic subscription
   * delivers, with {@code Message} set to the given raw JSON token (a quoted string, or {@code
   * null}).
   */
  private static String envelope(String messageJsonToken) {
    return "{"
        + "\"TopicOwner\":\"1234567890123456\","
        + "\"Message\":" + messageJsonToken + ","
        + "\"Subscriber\":\"1234567890123456\","
        + "\"PublishTime\":1700000000000,"
        + "\"SubscriptionName\":\"my-subscription\","
        + "\"MessageMD5\":\"0CC175B9C0F1B6A831C399E269772661\","
        + "\"TopicName\":\"my-topic\","
        + "\"MessageId\":\"5F1BF2E7B0A1E2C3D4E5F6A7\""
        + "}";
  }

  /**
   * A probe-shaped envelope where the required field {@code overrideField} carries the raw JSON
   * token {@code overrideToken} (e.g. {@code "null"}, {@code "123"}, {@code "[\"x\"]"}) instead of
   * a string, so a non-Message required field's type can be exercised. Every other required field
   * stays a string.
   */
  private static String envelopeWith(String overrideField, String overrideToken) {
    return "{"
        + "\"TopicOwner\":" + tokenFor("TopicOwner", overrideField, overrideToken) + ","
        + "\"Message\":" + tokenFor("Message", overrideField, overrideToken) + ","
        + "\"Subscriber\":\"1234567890123456\","
        + "\"PublishTime\":1700000000000,"
        + "\"SubscriptionName\":" + tokenFor("SubscriptionName", overrideField, overrideToken) + ","
        + "\"MessageMD5\":\"0CC175B9C0F1B6A831C399E269772661\","
        + "\"TopicName\":" + tokenFor("TopicName", overrideField, overrideToken) + ","
        + "\"MessageId\":" + tokenFor("MessageId", overrideField, overrideToken)
        + "}";
  }

  /** The override token for {@code field} when it matches {@code overrideField}, else a string. */
  private static String tokenFor(String field, String overrideField, String overrideToken) {
    return field.equals(overrideField) ? overrideToken : "\"str-" + field + "\"";
  }

  @Test
  void extractsPlaintextMessageFromProbeShapedEnvelope() {
    assertEquals(
        "hello world", SmqTopicEnvelope.extractBodyIfEnvelope(envelope("\"hello world\"")));
  }

  @Test
  void returnsBase64MessageStringWithoutDecodingIt() {
    // The parser returns the literal inner value; base64-decoding is the subscription's job, not
    // the parser's, so a base64 Message comes back as the base64 text unchanged.
    String base64 = "aGVsbG8gd29ybGQ=";
    assertEquals(base64, SmqTopicEnvelope.extractBodyIfEnvelope(envelope("\"" + base64 + "\"")));
  }

  @Test
  void jsonNullMessageIsNotAnEnvelope() {
    // A JSON-null Message is not a string primitive, so the object is not classified as an
    // envelope. A legitimately empty body rides as an empty string instead (see below).
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelope("null")));
  }

  @Test
  void emptyStringMessageInEnvelopeYieldsEmptyString() {
    // A legitimately empty published body rides as an empty JSON string, which is a string
    // primitive, so it classifies as an envelope and yields the empty string.
    assertEquals("", SmqTopicEnvelope.extractBodyIfEnvelope(envelope("\"\"")));
  }

  @Test
  void numericOrBooleanMessageIsNotAnEnvelope() {
    // A numeric or boolean Message must not be coerced to a string body; an object with all five
    // required fields but a non-string Message is not an envelope.
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelope("42")));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelope("true")));
  }

  @Test
  void nonMessageRequiredFieldOfWrongTypeIsNotAnEnvelope() {
    // Every distinctive field must be a JSON string, not just Message. A required field other than
    // Message that is JSON-null, a number, an array, or an object disqualifies the object as an
    // envelope, even though Message itself is a valid string.
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelopeWith("TopicName", "null")));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelopeWith("MessageId", "123")));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelopeWith("SubscriptionName", "[\"x\"]")));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelopeWith("TopicOwner", "{\"a\":1}")));
  }

  @Test
  void missingNonMessageRequiredFieldIsNotAnEnvelope() {
    // Control for the wrong-type case: an object missing a required field other than Message (here
    // TopicName) is not an envelope, matching the missing-TopicOwner case above.
    String missingTopicName =
        "{"
            + "\"TopicOwner\":\"1234567890123456\","
            + "\"Message\":\"hi\","
            + "\"Subscriber\":\"1234567890123456\","
            + "\"PublishTime\":1700000000000,"
            + "\"SubscriptionName\":\"my-subscription\","
            + "\"MessageMD5\":\"ABC\","
            + "\"MessageId\":\"MID-1\""
            + "}";
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(missingTopicName));
  }

  @Test
  void nonJsonStringIsNotAnEnvelope() {
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope("just a plain body"));
  }

  @Test
  void jsonObjectMissingOneRequiredFieldIsNotAnEnvelope() {
    // Drop TopicOwner alone: the strong all-fields requirement disqualifies the object, so a body
    // that merely resembles an envelope is not misread as one.
    String missingOwner =
        "{"
            + "\"Message\":\"hi\","
            + "\"Subscriber\":\"1234567890123456\","
            + "\"PublishTime\":1700000000000,"
            + "\"SubscriptionName\":\"my-subscription\","
            + "\"MessageMD5\":\"ABC\","
            + "\"TopicName\":\"my-topic\","
            + "\"MessageId\":\"MID-1\""
            + "}";
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(missingOwner));
  }

  @Test
  void jsonArrayIsNotAnEnvelope() {
    assertNull(
        SmqTopicEnvelope.extractBodyIfEnvelope(
            "[\"Message\",\"TopicName\",\"MessageId\",\"SubscriptionName\",\"TopicOwner\"]"));
  }

  @Test
  void plainUserJsonObjectWithoutEnvelopeFieldsIsNotAnEnvelope() {
    // A legitimate user payload that happens to be a JSON object is a direct body, not an envelope.
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope("{\"user\":\"data\",\"id\":42}"));
  }

  @Test
  void malformedJsonReturnsNullAndDoesNotThrow() {
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope("{not valid json"));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope("{\"Message\":"));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope("{\"Message\":\"unterminated"));
  }

  @Test
  void nonStringMessageValueIsNotTreatedAsAnEnvelope() {
    // All required fields present but Message is a nested JSON object rather than a string: not a
    // valid envelope body, so it is treated as direct (returned null) rather than throwing.
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelope("{\"nested\":true}")));
  }

  @Test
  void nullOrEmptyInputIsNotAnEnvelope() {
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(null));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(""));
  }
}
