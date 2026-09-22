package com.salesforce.multicloudj.pubsub.ali;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parses the JSON envelope an SMQ (MNS) topic subscription wraps around a published body when its
 * backing queue is a JSON-format subscription endpoint.
 *
 * <p>A JSON-format topic delivery replaces the queue message's wire body with a JSON object whose
 * {@code "Message"} field holds the publisher's actual body; the surrounding fields are delivery
 * metadata added by the topic. A message published directly to a queue carries no such envelope, so
 * the subscription must distinguish the two from the wire body alone.
 *
 * <p>Detection is a body-shape sniff: the wire body is treated as a topic envelope only when it
 * parses as a JSON object that carries all of the distinctive envelope fields
 * ({@code Message}, {@code TopicName}, {@code MessageId}, {@code SubscriptionName},
 * {@code TopicOwner}), each present as a JSON string primitive. Requiring the full set — and that
 * each be a string, as real deliveries always are — makes a direct body that coincidentally looks
 * like an envelope vanishingly unlikely to be misread. Parsing is defensive: any malformed input,
 * a non-object, a missing field, or a non-string required field is treated as "not an envelope"
 * (never throws), so a direct body is returned to the caller unchanged rather than silently
 * emptied.
 *
 * <p><b>Accepted limitation:</b> because detection is only a body-shape sniff, a message published
 * directly to the queue whose own body happens to be a JSON object carrying exactly this full set
 * of fields (with a string {@code Message}) would be unwrapped as if it were a topic delivery. A
 * stronger, unambiguous wire signal was considered and intentionally deferred. The required field
 * set is deliberately not widened further: a larger set would only enlarge the blast radius of a
 * false negative were the delivery schema ever to change.
 */
final class SmqTopicEnvelope {

  private static final String MESSAGE_FIELD = "Message";

  // The distinctive topic-envelope fields. All must be present for the wire body to be classified
  // as an envelope, so a direct body that happens to share one or two field names is not misread.
  private static final String[] REQUIRED_FIELDS = {
    MESSAGE_FIELD, "TopicName", "MessageId", "SubscriptionName", "TopicOwner"
  };

  // The repo's managed JSON library. A single shared ObjectMapper (thread-safe for read/parse).
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SmqTopicEnvelope() {}

  /**
   * Returns the value of the envelope's {@code "Message"} field when {@code rawWireBody} is a topic
   * envelope, or {@code null} when it is not (a direct body). The object is classified as an
   * envelope only when every distinctive field is present as a JSON string primitive; a missing
   * field, or any of them being a number, boolean, JSON-null, object, or array, is not an envelope.
   * The returned value is the literal inner body text (the empty string for an empty-string {@code
   * "Message"}); whether it must be base64-decoded is signalled separately by the native base64
   * flag, not by this parser.
   */
  static String extractBodyIfEnvelope(String rawWireBody) {
    if (rawWireBody == null || rawWireBody.isEmpty()) {
      return null;
    }
    JsonNode object;
    try {
      object = MAPPER.readTree(rawWireBody);
    } catch (JsonProcessingException e) {
      // Not valid JSON: treat as a direct (non-envelope) body.
      return null;
    }
    if (object == null || !object.isObject()) {
      return null;
    }
    // Every distinctive field must be present AND a JSON string primitive. Real JSON-format topic
    // deliveries always carry these as strings (probe-confirmed), so requiring the string type
    // shrinks the chance a direct body coincidentally matches, with no false-negative risk. A
    // missing field, or a null/number/boolean/object/array value, means "not an envelope" — the
    // wire body is treated as direct so its bytes are returned unchanged rather than emptied.
    for (String field : REQUIRED_FIELDS) {
      JsonNode value = object.get(field);
      if (value == null || !value.isTextual()) {
        return null;
      }
    }
    return object.get(MESSAGE_FIELD).asText();
  }
}
