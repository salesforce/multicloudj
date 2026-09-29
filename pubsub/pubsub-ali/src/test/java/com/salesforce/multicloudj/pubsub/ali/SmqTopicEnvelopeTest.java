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
    // A numeric or boolean Message must not be coerced to a string body; a non-string "Message"
    // is not extractable, so the parser returns null.
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelope("42")));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelope("true")));
  }

  @Test
  void extractsMessageRegardlessOfOtherEnvelopeFields() {
    // Pure extractor: the reserved topic-originated marker (not the body shape) decides that a
    // message is a topic delivery, so extraction needs only a JSON object with a string "Message".
    // Other envelope fields being absent, or present with a non-string type, no longer matters —
    // the old 5-field shape-sniff detection is gone.
    assertEquals("hi", SmqTopicEnvelope.extractBodyIfEnvelope("{\"Message\":\"hi\"}"));
    assertEquals(
        "hi", SmqTopicEnvelope.extractBodyIfEnvelope("{\"Message\":\"hi\",\"TopicName\":\"t\"}"));
    assertEquals(
        "hi",
        SmqTopicEnvelope.extractBodyIfEnvelope(
            "{\"Message\":\"hi\",\"TopicName\":123,\"TopicOwner\":null}"));
  }

  @Test
  void nonJsonStringIsNotAnEnvelope() {
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope("just a plain body"));
  }

  @Test
  void objectWithoutStringMessageFieldIsNotAnEnvelope() {
    // No "Message" field at all -> not extractable -> null (caller fails closed).
    assertNull(
        SmqTopicEnvelope.extractBodyIfEnvelope("{\"TopicName\":\"t\",\"MessageId\":\"m\"}"));
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
    // "Message" is a nested JSON object rather than a string: not extractable, so the parser
    // returns null (never throwing) and the caller fails closed.
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(envelope("{\"nested\":true}")));
  }

  @Test
  void nullOrEmptyInputIsNotAnEnvelope() {
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(null));
    assertNull(SmqTopicEnvelope.extractBodyIfEnvelope(""));
  }
}
