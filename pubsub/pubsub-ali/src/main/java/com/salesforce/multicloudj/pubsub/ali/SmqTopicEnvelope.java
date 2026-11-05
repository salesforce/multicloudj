package com.salesforce.multicloudj.pubsub.ali;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extracts the publisher's body from the JSON envelope an SMQ (MNS) topic subscription wraps around
 * it when its backing queue is a JSON-format subscription endpoint.
 *
 * <p>A JSON-format topic delivery replaces the queue message's wire body with a JSON object whose
 * {@code "Message"} field holds the publisher's actual body; the surrounding fields are delivery
 * metadata added by the topic. Whether a given message IS a topic delivery is decided
 * authoritatively by the caller (via the reserved topic-originated marker user property), not by
 * this parser. This is therefore a pure extractor: given a body the caller already knows should be
 * an envelope, it pulls out the inner {@code "Message"} and returns {@code null} when the body is
 * not a well-formed envelope, so the caller can fail closed rather than guess at a raw body.
 */
final class SmqTopicEnvelope {

  private static final String MESSAGE_FIELD = "Message";

  // The repo's managed JSON library. A single shared ObjectMapper (thread-safe for read/parse).
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SmqTopicEnvelope() {}

  /**
   * Returns the value of the envelope's {@code "Message"} field, or {@code null} when
   * {@code rawWireBody} is not a well-formed envelope — that is, when it is not valid JSON, not a
   * JSON object, or has no string {@code "Message"} field. The returned value is the literal inner
   * body text (the empty string for an empty-string {@code "Message"}); whether it must be
   * base64-decoded is signalled separately by the native base64 flag, not by this parser. Never
   * throws: a parse failure yields {@code null} so the caller decides how to handle a non-envelope
   * body.
   */
  static String extractBodyIfEnvelope(String rawWireBody) {
    if (rawWireBody == null || rawWireBody.isEmpty()) {
      return null;
    }
    JsonNode object;
    try {
      object = MAPPER.readTree(rawWireBody);
    } catch (JsonProcessingException e) {
      // Not valid JSON: not a well-formed envelope.
      return null;
    }
    if (object == null || !object.isObject()) {
      return null;
    }
    JsonNode message = object.get(MESSAGE_FIELD);
    if (message == null || !message.isTextual()) {
      return null;
    }
    return message.asText();
  }
}
