package com.salesforce.multicloudj.pubsub.ali;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.model.MessagePropertyValue;
import com.aliyun.mns.model.PropertyType;
import com.aliyun.mns.model.serialize.queue.MessageListSerializer;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AliBaseTopicTest {

  @Test
  void encodedWireSizeDoesNotOverflowForNearMaxLength() {
    // A raw length near Integer.MAX_VALUE would overflow the 32-bit `4 * ((n + 2) / 3)` into a
    // negative, understated size that slips past the single-message size check. Computed in long,
    // encodedWireSize must return a correct positive size far above the per-request limit, so such
    // a body is rejected rather than silently accepted.
    long wireSize = AliBaseTopic.encodedWireSize(Integer.MAX_VALUE);

    long expected =
        4L * ((Integer.MAX_VALUE + 2L) / 3L) + AliBaseTopic.MESSAGE_ENVELOPE_OVERHEAD_BYTES;
    assertEquals(expected, wireSize);
    assertTrue(wireSize > 0, "wire size must not overflow to a negative value");
    assertTrue(
        wireSize > AliBaseTopic.MAX_REQUEST_BYTE_SIZE,
        "a near-2 GB body must measure well above the per-request limit");
  }

  @Test
  void estimatedRequestSizeIsAtLeastActualSerializedSize() throws Exception {
    // The byte guard estimates each batch's request size as FIXED_REQUEST_OVERHEAD_BYTES plus the
    // sum of measureWireSize(message) over the batch. This proves that estimate is a true upper
    // bound on the request the SMQ SDK actually serializes (MessageListSerializer,
    // exactly what batchPutMessage transmits) for representative batches: a tiny single message, a
    // single message near the per-message limit, and a full 16-message batch near the packing
    // boundary. Covers both body wire forms: AUTO carries these UTF-8 bodies raw, ALWAYS
    // base64-encodes them. Pure client-side serialization: no network, no credentials.
    try (AliSmqQueue rawTopic = new AliSmqQueue()) {
      assertEstimateIsUpperBound(rawTopic, uniform(1, 3));
      assertEstimateIsUpperBound(rawTopic, uniform(1, 48_000));
      assertEstimateIsUpperBound(rawTopic, uniform(16, 3_000));
    }
    try (AliSmqQueue base64Topic = alwaysBase64Topic()) {
      assertEstimateIsUpperBound(base64Topic, uniform(1, 3));
      assertEstimateIsUpperBound(base64Topic, uniform(1, 48_000));
      assertEstimateIsUpperBound(base64Topic, uniform(16, 3_000));
    }
  }

  @Test
  void everySubBatchFromSplitBySizeSerializesWithinTheLimit() throws Exception {
    // An over-limit logical batch is packed by splitBySize into sub-batches; each sub-batch, once
    // serialized by the SMQ SDK, must stay within the documented 64 KB per-request limit. Three
    // ~20 KB bodies (each ~26 KB base64 under ALWAYS) force a split into more than one sub-batch.
    try (AliSmqQueue topic = alwaysBase64Topic()) {
      List<Message> batch = uniform(3, 20_000);
      List<List<Message>> subBatches = topic.splitBySize(batch);
      assertTrue(
          subBatches.size() > 1, "the over-limit batch must split into multiple sub-batches");
      for (List<Message> subBatch : subBatches) {
        assertTrue(
            serializedLength(topic, subBatch) <= AliBaseTopic.MAX_REQUEST_BYTE_SIZE,
            "each serialized sub-batch must stay within the 64 KB per-request limit");
      }
    }
  }

  @Test
  void estimatedRequestSizeCoversUserProperties() throws Exception {
    // The metadata byte accounting must be a true upper bound on what the SMQ SDK actually
    // serializes, including XML escaping of values. Build a batch whose messages carry keys that
    // need hex-escaping and values with XML-special characters, serialize exactly as
    // batchPutMessage does, and confirm the estimate is at least the serialized size.
    try (AliSmqQueue topic = new AliSmqQueue()) {
      List<Message> batch =
          List.of(
              Message.builder()
                  .withBody("body-1".getBytes(UTF_8))
                  .withMetadata("trace.id", "a&b<c>d\"e")
                  .withMetadata("plainKey", "value")
                  .build(),
              Message.builder().withBody("body-2".getBytes(UTF_8)).withMetadata("k_2", "").build());
      long estimate = AliBaseTopic.FIXED_REQUEST_OVERHEAD_BYTES;
      for (Message message : batch) {
        estimate += topic.measureWireSize(message);
      }
      assertTrue(
          estimate >= serializedLength(topic, batch),
          "estimated request size must be an upper bound on the serialized size with properties");
    }
  }

  @Test
  void crHeavyRawBodyIsSizedAtLeastItsSerializedLength() throws Exception {
    // The SMQ SDK serializes carriage return (0x0D) in XML text as the numeric character reference
    // "&#13;", a multi-byte expansion, while line feed and tab stay one byte. A CR-heavy raw body
    // under AUTO must be sized with that expansion charged, or a body near the 64 KB cap could
    // under-count and slip past the guard. Prove the estimate is a true upper bound on the real
    // MessageListSerializer output for a body that is almost all CRs.
    StringBuilder crHeavy = new StringBuilder();
    for (int i = 0; i < 8_000; i++) {
      crHeavy.append("\r\n");
    }
    try (AliSmqQueue topic = new AliSmqQueue()) {
      List<Message> batch =
          List.of(Message.builder().withBody(crHeavy.toString().getBytes(UTF_8)).build());
      long estimate = AliBaseTopic.FIXED_REQUEST_OVERHEAD_BYTES;
      for (Message message : batch) {
        estimate += topic.measureWireSize(message);
      }
      assertTrue(
          estimate >= serializedLength(topic, batch),
          "a CR-heavy raw body must be sized at least its serialized (CR-expanded) length");
    }
  }

  @Test
  void estimatedRequestSizeCoversBinaryUserPropertyValue() throws Exception {
    // A metadata value carrying an XML-illegal control byte cannot ride as XML text, so it rides
    // as a BINARY property whose wire form is the base64 of its UTF-8 bytes (a 4/3 inflation, not
    // the up-to-5x XML-escape expansion a STRING value gets). The byte accounting must stay a true
    // upper bound on that base64 form, or a metadata-heavy message near the 64 KB cap could
    // under-count and slip past the guard. A several-hundred-byte value exercises the base64
    // inflation; confirm the estimate is at least the real MessageListSerializer output. A BINARY
    // value's wire form is the base64 of its default-charset bytes, which equal its UTF-8 bytes
    // only on a UTF-8-default JVM (the supported configuration).
    assumeTrue(
        Charset.defaultCharset().equals(StandardCharsets.UTF_8),
        "BINARY value sizing asserted only on a UTF-8-default JVM");
    try (AliSmqQueue topic = new AliSmqQueue()) {
      // A leading U+0001 (an XML-illegal C0 control byte) makes the whole value XML-unsafe.
      String binaryValue = (char) 1 + "v".repeat(400);
      List<Message> batch =
          List.of(
              Message.builder()
                  .withBody("body".getBytes(UTF_8))
                  .withMetadata("trace.id", binaryValue)
                  .build());
      // The XML-illegal control byte forces the value onto the BINARY (base64) property path.
      assertEquals(
          PropertyType.BINARY, propertyFor(topic, "trace.id", binaryValue).getDataType());
      long estimate = AliBaseTopic.FIXED_REQUEST_OVERHEAD_BYTES;
      for (Message message : batch) {
        estimate += topic.measureWireSize(message);
      }
      assertTrue(
          estimate >= serializedLength(topic, batch),
          "estimated request size must be an upper bound on the serialized size with a "
              + "BINARY-valued property");
    }
  }

  @Test
  void estimatedRequestSizeCoversBase64BodyWithMetadata() throws Exception {
    // Combined coverage: an ALWAYS message base64-encodes its body AND sets the reserved base64
    // flag as an extra user property, on top of the message's own metadata. The ALWAYS size
    // tests are body-only and the metadata size tests use raw bodies; this covers the
    // flag-plus-metadata path together and confirms the estimate stays an upper bound on the
    // real serialized size (a hex-escaped key and an XML-special value keep the form non-trivial).
    try (AliSmqQueue topic = alwaysBase64Topic()) {
      List<Message> batch =
          List.of(
              Message.builder()
                  .withBody("base64-body-with-metadata".getBytes(UTF_8))
                  .withMetadata("trace.id", "a&b<c>d\"e")
                  .withMetadata("with space", "spaced-value")
                  .withMetadata("plainKey", "value")
                  .build());
      // The reserved base64 flag and the user metadata ride together on the wire.
      com.aliyun.mns.model.Message wire = topic.toSmqMessage(batch.get(0));
      assertTrue(
          wire.getUserProperties().containsKey(AliBaseTopic.RESERVED_BASE64_FLAG_KEY),
          "ALWAYS must set the reserved base64 flag alongside the user metadata");
      assertTrue(
          wire.getUserProperties().containsKey(AliBaseTopic.encodeMetadataKey("with space")),
          "user metadata must be present on the wire together with the base64 flag");
      assertEstimateIsUpperBound(topic, batch);
    }
  }

  @Test
  void metadataAttributesCountTowardTheBatchByteLimit() throws Exception {
    try (AliSmqQueue topic = new AliSmqQueue()) {
      // Two messages with tiny bodies but large metadata (ten max-length-valued attributes each,
      // ~42 KB on the wire per message) exceed the 64 KB per-request cap together, so the byte
      // guard must split them into separate sub-batches even though their bodies alone would fit.
      Message heavy1 = messageWithLargeMetadata("m1");
      Message heavy2 = messageWithLargeMetadata("m2");
      // A single such message stays under the per-request cap (it is not rejected outright)...
      assertTrue(
          AliBaseTopic.FIXED_REQUEST_OVERHEAD_BYTES + topic.measureWireSize(heavy1)
              <= AliBaseTopic.MAX_REQUEST_BYTE_SIZE);
      // ...but the two together do not, so splitBySize separates them.
      assertEquals(2, topic.splitBySize(List.of(heavy1, heavy2)).size());
      // The split is driven by the metadata, not the body: the same bodies with no metadata pack
      // into a single sub-batch.
      List<List<Message>> bodyOnly =
          topic.splitBySize(
              List.of(
                  Message.builder().withBody("x").build(),
                  Message.builder().withBody("x").build()));
      assertEquals(1, bodyOnly.size());
    }
  }

  @Test
  void plainTextMetadataValuesAtMaxLengthAreNotOverRejected() throws Exception {
    // A STRING metadata value rides as XML text charged its exact XML-escaped byte length, so
    // ordinary escape-free text costs one wire byte per byte rather than a blanket worst-case
    // multiple. Four attributes each holding the documented 4096-character per-value maximum of
    // plain ASCII total ~17 KB on the wire, well under the 64 KB per-request cap, so such a message
    // must be accepted by the byte guard rather than falsely rejected as oversized.
    try (AliSmqQueue topic = new AliSmqQueue()) {
      Message.Builder builder = Message.builder().withBody("b".getBytes(UTF_8));
      for (int i = 0; i < 4; i++) {
        builder.withMetadata("key" + i, "a".repeat(AliBaseTopic.MAX_PROPERTY_VALUE_LENGTH));
      }
      Message message = builder.build();
      List<Message> batch = List.of(message);

      long requestSize = AliBaseTopic.FIXED_REQUEST_OVERHEAD_BYTES + topic.measureWireSize(message);
      assertTrue(
          requestSize < AliBaseTopic.MAX_REQUEST_BYTE_SIZE,
          "four max-length plain-text metadata values must estimate well under the 64 KB cap, not "
              + "over it; measured "
              + requestSize);
      // The byte guard must pack the message (not throw it out as oversized): splitBySize returns a
      // single sub-batch, and the estimate stays a true upper bound on the real serialized size.
      List<List<Message>> subBatches = topic.splitBySize(batch);
      assertEquals(1, subBatches.size());
      assertTrue(
          requestSize >= serializedLength(topic, batch),
          "the estimate must remain an upper bound on the serialized size");
    }
  }

  @Test
  void escapeHeavyMetadataValueIsSizedAtLeastItsSerializedLength() throws Exception {
    // A STRING metadata value is charged its exact XML-escaped byte length, so an escape-heavy
    // value (each '&' expands to "&amp;", each '<'/'>' to their entities) and a mixed plain+escape
    // value must both still estimate at least their real MessageListSerializer output. This proves
    // the per-byte accounting never under-counts the escaped wire form, which would slip an
    // oversized request past the 64 KB guard (a fail-open the exact sizing must not introduce).
    try (AliSmqQueue topic = new AliSmqQueue()) {
      List<Message> batch =
          List.of(
              Message.builder()
                  .withBody("b".getBytes(UTF_8))
                  .withMetadata("all-amp", "&".repeat(2000))
                  .withMetadata("mixed", "plain-" + "<&>".repeat(500) + "-tail")
                  .build());
      long estimate = AliBaseTopic.FIXED_REQUEST_OVERHEAD_BYTES;
      for (Message message : batch) {
        estimate += topic.measureWireSize(message);
      }
      assertTrue(
          estimate >= serializedLength(topic, batch),
          "escape-heavy STRING metadata must be sized at least its XML-escaped serialized length");
    }
  }

  @Test
  void metadataKeyCodecRoundTripsRepresentativeKeys() {
    String[] keys = {
      "simple",
      "with-hyphen",
      "MiXeD123",
      "dotted.key",
      "under_score",
      "space key",
      "colon:semi;comma,",
      "unicode-café-Ω",
      "__0x41__",
      "a..b",
      ".leading",
      "trailing."
    };
    for (String key : keys) {
      String encoded = AliBaseTopic.encodeMetadataKey(key);
      assertEquals(
          key,
          AliBaseTopic.decodeMetadataKey(encoded),
          "key must round-trip through encode/decode: " + key);
    }
  }

  @Test
  void encodedMetadataKeyStaysWithinSmqAttributeNameCharset() {
    // Encoded keys use only [A-Za-z0-9._-] and never a leading, trailing, or consecutive dot, so
    // they satisfy the SMQ attribute-name charset and its dot-position rules for any input.
    String[] keys = {
      "dotted.key", "a..b", ".x.", "space and & < >", "café", "trailing.", ".lead", "__0x41__"
    };
    for (String key : keys) {
      String encoded = AliBaseTopic.encodeMetadataKey(key);
      assertTrue(encoded.matches("[A-Za-z0-9._-]*"), "encoded key charset: " + encoded);
      assertFalse(encoded.startsWith("."), "encoded key must not start with a dot: " + encoded);
      assertFalse(encoded.endsWith("."), "encoded key must not end with a dot: " + encoded);
      assertFalse(encoded.contains(".."), "encoded key must not hold consecutive dots: " + encoded);
    }
  }

  @Test
  void encodeMetadataKeyPassesThroughConformingKeys() {
    // Alphanumerics, '-', '_', and a single interior '.' are all within the SMQ attribute-name
    // charset, so a conforming key rides the wire unescaped and decodes back identical.
    for (String key : new String[] {"abc-DEF-123", "under_score", "a_b_c", "trace.id", "a.b.c"}) {
      String encoded = AliBaseTopic.encodeMetadataKey(key);
      assertEquals(key, encoded, "conforming key must pass through raw: " + key);
      assertEquals(key, AliBaseTopic.decodeMetadataKey(encoded));
    }
  }

  @Test
  void encodeMetadataKeyEscapesNonConformingBytesAndDotBoundaries() {
    // A space (and any byte outside [A-Za-z0-9._-]) is hex-escaped; an underscore and an interior
    // isolated dot ride raw; a leading, trailing, or consecutive dot is escaped as __0x2E__.
    assertEquals("a__0x20__b", AliBaseTopic.encodeMetadataKey("a b"));
    assertEquals("a_b", AliBaseTopic.encodeMetadataKey("a_b"));
    assertEquals("a.b", AliBaseTopic.encodeMetadataKey("a.b"));
    assertEquals("__0x2E__a", AliBaseTopic.encodeMetadataKey(".a"));
    assertEquals("a__0x2E__", AliBaseTopic.encodeMetadataKey("a."));
    assertEquals("a__0x2E____0x2E__b", AliBaseTopic.encodeMetadataKey("a..b"));
  }

  @Test
  void encodeMetadataKeyHardensLiteralEscapeTokenSoItRoundTrips() {
    // A user key literally containing an "__0x..__" escape token must not be misread on decode as
    // the byte it looks like: encode disrupts the marker so the exact literal round-trips.
    String literal = "__0x41__";
    String encoded = AliBaseTopic.encodeMetadataKey(literal);
    assertNotEquals(literal, encoded, "the literal escape token must be disrupted on the wire");
    assertEquals(literal, AliBaseTopic.decodeMetadataKey(encoded));
    assertNotEquals("A", AliBaseTopic.decodeMetadataKey(encoded), "must not decode as byte 0x41");
  }

  @Test
  void encodeMetadataKeyRejectsEncodedKeyOverLengthLimit() {
    // Each '!' hex-escapes to 8 characters, so 33 of them encode to 264 characters, over the SMQ
    // 256-character attribute-name limit; the message must fail fast rather than be rejected by the
    // service. ('!' is a non-blank non-conforming byte, so this trips the length limit, not the
    // blank-key guard.) A conforming 256-character key stays within the limit.
    assertThrows(
        InvalidArgumentException.class,
        () -> AliBaseTopic.encodeMetadataKey("!".repeat(33)));
    assertEquals(256, AliBaseTopic.encodeMetadataKey("a".repeat(256)).length());
  }

  @Test
  void metadataValueRidesAsStringWhenXmlSafeAndBinaryWhenNot() throws Exception {
    try (AliSmqQueue topic = new AliSmqQueue()) {
      // An XML-safe value (incl. XML specials and multibyte) rides as a STRING property and reads
      // back exactly.
      MessagePropertyValue string = propertyFor(topic, "k", "a & b < c > 世界");
      assertEquals(PropertyType.STRING, string.getDataType());
      assertEquals("a & b < c > 世界", string.getStringValueByType());
      // An XML-unsafe value (an XML-illegal control byte) rides as a BINARY property and still
      // reads back exactly via getStringValueByType.
      MessagePropertyValue binary = propertyFor(topic, "k", "x\u0000y");
      assertEquals(PropertyType.BINARY, binary.getDataType());
      assertEquals("x\u0000y", binary.getStringValueByType());
      // An empty value rides as a present STRING "".
      MessagePropertyValue empty = propertyFor(topic, "k", "");
      assertEquals(PropertyType.STRING, empty.getDataType());
      assertEquals("", empty.getStringValueByType());
    }
  }

  @Test
  void binaryValueCharsetGuardAcceptsValuesOnUtf8DefaultJvm() {
    // The BINARY charset guard rejects a value only when the JVM default charset would not
    // reproduce its UTF-8 bytes. On a UTF-8-default JVM (the supported configuration) the two
    // encodings always agree, so no XML-unsafe value is spuriously rejected; the throw path is
    // exercised only where the default charset differs from UTF-8, which a unit test cannot force
    // in-process.
    assumeTrue(
        Charset.defaultCharset().equals(StandardCharsets.UTF_8),
        "guard behavior asserted only on a UTF-8-default JVM");
    for (String value : new String[] {"x\u0000y", "before\uFFFFafter", "世界", "", "plain"}) {
      assertFalse(
          AliBaseTopic.binaryValueCorruptsUnderDefaultCharset(value),
          "no value should be rejected on a UTF-8-default JVM: [" + value + "]");
    }
  }

  @Test
  void binaryValueLengthLimitIsMeasuredOnItsBase64WireForm() throws Exception {
    // An XML-unsafe value rides as a BINARY property whose wire form is the base64 of its UTF-8
    // bytes, and SMQ measures the 4096-character per-value limit on that base64 text. So the raw
    // value may hold at most 3072 UTF-8 bytes: base64 inflates a 3-byte group to 4 characters, so
    // 3072 bytes encode to exactly 4096 characters (accepted, rides as BINARY) while one more byte
    // encodes to 4100 characters (rejected fail-fast). A single-byte XML-illegal control character
    // (U+0001) makes the value XML-unsafe with its character count equal to its UTF-8 byte count. A
    // BINARY value's wire form is the base64 of its default-charset bytes, which equal its UTF-8
    // bytes only on a UTF-8-default JVM (the supported configuration).
    assumeTrue(
        Charset.defaultCharset().equals(StandardCharsets.UTF_8),
        "BINARY value length asserted only on a UTF-8-default JVM");
    try (AliSmqQueue topic = new AliSmqQueue()) {
      String atLimit = String.valueOf((char) 1).repeat(3072);
      MessagePropertyValue property = propertyFor(topic, "k", atLimit);
      assertEquals(
          PropertyType.BINARY,
          property.getDataType(),
          "a 3072-byte XML-unsafe value (base64 length 4096) must be accepted as BINARY");

      String overLimit = String.valueOf((char) 1).repeat(3073);
      Message message =
          Message.builder().withBody("b".getBytes(UTF_8)).withMetadata("k", overLimit).build();
      assertThrows(
          InvalidArgumentException.class,
          () -> topic.toSmqMessage(message),
          "a 3073-byte XML-unsafe value (base64 length 4100) must be rejected fail-fast");
    }
  }

  @Test
  void stringValueLengthLimitCountsLogicalCharacters() throws Exception {
    // An XML-safe value rides as a STRING property (XML text), and SMQ measures the 4096-character
    // per-value limit in logical characters: XML escaping and multibyte UTF-8 encoding do not count
    // toward it. A 4096-character value of '&' (which escapes to ~20 KB on the wire) and a
    // 4096-character multibyte value (~12 KB of UTF-8) are both accepted as STRING properties; only
    // a value over 4096 logical characters is rejected, regardless of how large it is in bytes.
    try (AliSmqQueue topic = new AliSmqQueue()) {
      String ampersands = "&".repeat(AliBaseTopic.MAX_PROPERTY_VALUE_LENGTH);
      assertEquals(
          PropertyType.STRING,
          propertyFor(topic, "k", ampersands).getDataType(),
          "a 4096-char value that escapes to ~20 KB on the wire must be accepted as STRING");

      String multibyte = "世".repeat(AliBaseTopic.MAX_PROPERTY_VALUE_LENGTH);
      assertEquals(
          PropertyType.STRING,
          propertyFor(topic, "k", multibyte).getDataType(),
          "a 4096-char multibyte value (~12 KB of UTF-8) must be accepted as STRING");

      String tooLong = "&".repeat(AliBaseTopic.MAX_PROPERTY_VALUE_LENGTH + 1);
      Message message =
          Message.builder().withBody("b".getBytes(UTF_8)).withMetadata("k", tooLong).build();
      assertThrows(
          InvalidArgumentException.class,
          () -> topic.toSmqMessage(message),
          "a value over 4096 logical characters must be rejected regardless of byte size");
    }
  }

  /** Builds the SMQ user property a publisher would set for {@code key -> value}. */
  private static MessagePropertyValue propertyFor(AliSmqQueue topic, String key, String value) {
    com.aliyun.mns.model.Message wire =
        topic.toSmqMessage(
            Message.builder().withBody("b".getBytes(UTF_8)).withMetadata(key, value).build());
    return wire.getUserProperties().get(AliBaseTopic.encodeMetadataKey(key));
  }

  @Test
  void encodeMetadataKeyRejectsNullOrEmptyKey() {
    // An empty key would encode to an empty wire name (<Name/>), which SMQ rejects with an opaque
    // error and the SDK silently drops on receive; both null and empty must fail fast at the codec.
    assertThrows(InvalidArgumentException.class, () -> AliBaseTopic.encodeMetadataKey(null));
    assertThrows(InvalidArgumentException.class, () -> AliBaseTopic.encodeMetadataKey(""));
  }

  @Test
  void toSmqMessageRejectsEmptyMetadataKey() throws Exception {
    // End-to-end: a message carrying an empty metadata key must be rejected before publish (as the
    // other per-message limits are), not shipped as an <Name/> the service rejects opaquely.
    try (AliSmqQueue topic = new AliSmqQueue()) {
      Message message =
          Message.builder().withBody("b".getBytes(UTF_8)).withMetadata("", "value").build();
      assertThrows(InvalidArgumentException.class, () -> topic.toSmqMessage(message));
    }
  }

  @Test
  void encodeMetadataKeyRejectsBlankKeys() {
    // A whitespace-only key is blank and is rejected fail-fast to match the cloud-agnostic
    // MessageUtils.validateMetadata contract (key.trim().isEmpty()), which the public send path
    // already enforces on every publish.
    assertThrows(InvalidArgumentException.class, () -> AliBaseTopic.encodeMetadataKey(" "));
    assertThrows(InvalidArgumentException.class, () -> AliBaseTopic.encodeMetadataKey("\t"));
  }

  @Test
  void decodeMetadataKeyTreatsMalformedTokenAsLiteral() {
    // A "__0x" sequence not completed by two hex digits and "__" is not a valid token, so it
    // decodes literally rather than throwing.
    assertEquals("__0xZZ__", AliBaseTopic.decodeMetadataKey("__0xZZ__"));
    assertEquals("__0x", AliBaseTopic.decodeMetadataKey("__0x"));
    assertEquals(null, AliBaseTopic.decodeMetadataKey(null));
  }

  @Test
  void isXmlSafeAcceptsAllowedWhitespaceAndRejectsOtherControlBytes() {
    // XML 1.0 permits tab, LF and CR among the C0 controls; every other control byte
    // (0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F) is illegal in XML text and must be reported unsafe.
    assertTrue(AliBaseTopic.isXmlSafe(new byte[0]), "empty body is XML-safe");
    assertTrue(AliBaseTopic.isXmlSafe(new byte[] {'\t'}), "tab (0x09) is XML-safe");
    assertTrue(AliBaseTopic.isXmlSafe(new byte[] {'\n'}), "LF (0x0A) is XML-safe");
    assertTrue(AliBaseTopic.isXmlSafe(new byte[] {'\r'}), "CR (0x0D) is XML-safe");
    assertTrue(
        AliBaseTopic.isXmlSafe("plain text <>&".getBytes(UTF_8)),
        "printable ASCII incl. XML specials is XML-safe");
    assertTrue(AliBaseTopic.isXmlSafe("é-Ω-😀".getBytes(UTF_8)), "multibyte UTF-8 is XML-safe");

    assertFalse(AliBaseTopic.isXmlSafe(new byte[] {0x00}), "NUL (0x00) is XML-illegal");
    assertFalse(AliBaseTopic.isXmlSafe(new byte[] {0x0B}), "vertical tab (0x0B) is XML-illegal");
    assertFalse(AliBaseTopic.isXmlSafe(new byte[] {0x0C}), "form feed (0x0C) is XML-illegal");
    assertFalse(AliBaseTopic.isXmlSafe(new byte[] {0x1F}), "unit separator (0x1F) is XML-illegal");
    assertFalse(
        AliBaseTopic.isXmlSafe(new byte[] {'o', 'k', 0x08, 'x'}),
        "an embedded backspace (0x08) makes the body XML-illegal");

    // U+FFFE and U+FFFF are valid UTF-8 (EF BF BE / EF BF BF) but XML 1.0 forbids them in text.
    assertFalse(
        AliBaseTopic.isXmlSafe(new byte[] {'o', 'k', (byte) 0xEF, (byte) 0xBF, (byte) 0xBE, 'x'}),
        "U+FFFE (EF BF BE) is XML-illegal even though it is valid UTF-8");
    assertFalse(
        AliBaseTopic.isXmlSafe(new byte[] {'o', 'k', (byte) 0xEF, (byte) 0xBF, (byte) 0xBF, 'x'}),
        "U+FFFF (EF BF BF) is XML-illegal even though it is valid UTF-8");
    // The noncharacter check must not misfire on ordinary multibyte UTF-8: U+FFFD (EF BF BD, one
    // byte below U+FFFE) and a 4-byte code point (U+1F600, F0 9F 98 80) stay XML-safe.
    byte[] replacementThenEmoji = {
      (byte) 0xEF, (byte) 0xBF, (byte) 0xBD, (byte) 0xF0, (byte) 0x9F, (byte) 0x98, (byte) 0x80
    };
    assertTrue(
        AliBaseTopic.isXmlSafe(replacementThenEmoji),
        "ordinary multibyte UTF-8 (incl. U+FFFD and 4-byte code points) stays XML-safe");
  }

  @Test
  void isXmlSafeDetectsNoncharacterAtEndOfBuffer() {
    // The noncharacter scan matches a 3-byte U+FFFE/U+FFFF sequence at any offset, including one
    // occupying the final three bytes. Verify the end-of-buffer boundary is not off-by-one: a body
    // whose last three bytes are U+FFFE (EF BF BE) or U+FFFF (EF BF BF) is reported XML-unsafe, as
    // an interior noncharacter is (covered in
    // isXmlSafeAcceptsAllowedWhitespaceAndRejectsOtherControlBytes).
    assertFalse(
        AliBaseTopic.isXmlSafe(new byte[] {'o', 'k', (byte) 0xEF, (byte) 0xBF, (byte) 0xBE}),
        "a body ending in U+FFFE (EF BF BE) is XML-illegal");
    assertFalse(
        AliBaseTopic.isXmlSafe(new byte[] {'o', 'k', (byte) 0xEF, (byte) 0xBF, (byte) 0xBF}),
        "a body ending in U+FFFF (EF BF BF) is XML-illegal");
    // The 3-byte sequence occupying the entire buffer is detected too.
    assertFalse(
        AliBaseTopic.isXmlSafe(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBE}),
        "a body that is only U+FFFE is XML-illegal");
  }

  @Test
  void autoBase64EncodesBodyEndingInNoncharacter() throws Exception {
    // Downstream of isXmlSafe: under AUTO, a valid-UTF-8 body ending in an XML-illegal noncharacter
    // cannot ride as raw XML text, so toSmqMessage base64-encodes it and sets the reserved flag so
    // the receiver decodes it. Confirms the trailing-noncharacter case drives the same base64
    // decision an interior noncharacter does.
    try (AliSmqQueue topic = new AliSmqQueue()) {
      byte[] endsWithFffe = {'d', 'a', 't', 'a', (byte) 0xEF, (byte) 0xBF, (byte) 0xBE};
      byte[] endsWithFfff = {'d', 'a', 't', 'a', (byte) 0xEF, (byte) 0xBF, (byte) 0xBF};
      assertTrue(
          bodyRidesAsBase64(topic, endsWithFffe),
          "a body ending in U+FFFE must be base64-encoded and flagged under AUTO");
      assertTrue(
          bodyRidesAsBase64(topic, endsWithFfff),
          "a body ending in U+FFFF must be base64-encoded and flagged under AUTO");
      // A control-free, XML-safe UTF-8 body still rides raw (no flag), so the flag tracks the
      // noncharacter itself, not merely the act of calling toSmqMessage.
      assertFalse(
          bodyRidesAsBase64(topic, "plain-text".getBytes(UTF_8)),
          "an XML-safe body must ride raw, without the base64 flag, under AUTO");
    }
  }

  @Test
  void autoBase64EncodesInvalidUtf8Body() throws Exception {
    // Downstream of isValidUtf8: under AUTO, a body that is not valid UTF-8 cannot ride as raw XML
    // text, so toSmqMessage base64-encodes it and sets the reserved flag so the receiver decodes
    // it. Exercises the isValidUtf8==false branch: 0xC3 is a 2-byte UTF-8 lead byte not followed by
    // a valid continuation byte (0x28 '('), so the sequence is malformed.
    try (AliSmqQueue topic = new AliSmqQueue()) {
      byte[] invalidUtf8 = {(byte) 0xC3, (byte) 0x28};
      assertTrue(
          bodyRidesAsBase64(topic, invalidUtf8),
          "an invalid-UTF-8 body must be base64-encoded and flagged under AUTO");
    }
  }

  /** True if AUTO placed {@code body} on the wire base64-encoded (the reserved flag is set). */
  private static boolean bodyRidesAsBase64(AliSmqQueue topic, byte[] body) {
    com.aliyun.mns.model.Message wire =
        topic.toSmqMessage(Message.builder().withBody(body).build());
    return wire.getUserProperties() != null
        && wire.getUserProperties().containsKey(AliBaseTopic.RESERVED_BASE64_FLAG_KEY);
  }

  private static Message messageWithLargeMetadata(String keyPrefix) {
    String value = "v".repeat(AliBaseTopic.MAX_PROPERTY_VALUE_LENGTH);
    Message.Builder builder = Message.builder().withBody("x");
    for (int i = 0; i < 10; i++) {
      builder.withMetadata(keyPrefix + "-" + i, value);
    }
    return builder.build();
  }

  /** Builds an AliSmqQueue pinned to ALWAYS-base64 so bodies are sized by their base64 form. */
  private static AliSmqQueue alwaysBase64Topic() {
    MNSClient client = mock(MNSClient.class);
    when(client.getQueueRef("size-test")).thenReturn(mock(CloudQueue.class));
    AliSmqQueue.Builder builder = new AliSmqQueue.Builder();
    builder.withSmqClient(client);
    builder.withTopicName("size-test");
    builder.withBodyEncodingStrategy(AliBaseTopic.Base64EncodingStrategy.ALWAYS);
    return builder.build();
  }

  private static void assertEstimateIsUpperBound(AliSmqQueue topic, List<Message> messages)
      throws Exception {
    long estimate = AliBaseTopic.FIXED_REQUEST_OVERHEAD_BYTES;
    for (Message message : messages) {
      estimate += topic.measureWireSize(message);
    }
    long actual = serializedLength(topic, messages);
    assertTrue(
        estimate >= actual,
        "estimated request size ("
            + estimate
            + ") must be an upper bound on the actual serialized size ("
            + actual
            + ") for a batch of "
            + messages.size()
            + " message(s)");
  }

  /** Serializes the batch exactly as batchPutMessage does and returns the byte length. */
  private static long serializedLength(AliSmqQueue topic, List<Message> messages)
      throws Exception {
    List<com.aliyun.mns.model.Message> smqMessages = new ArrayList<>();
    for (Message message : messages) {
      smqMessages.add(topic.toSmqMessage(message));
    }
    MessageListSerializer serializer = new MessageListSerializer();
    try (InputStream in = serializer.serialize(smqMessages, "UTF-8")) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int read;
      while ((read = in.read(chunk)) != -1) {
        buffer.write(chunk, 0, read);
      }
      return buffer.size();
    }
  }

  private static List<Message> uniform(int count, int rawLength) {
    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      messages.add(Message.builder().withBody(bodyOf(rawLength)).build());
    }
    return messages;
  }

  private static byte[] bodyOf(int rawLength) {
    byte[] body = new byte[rawLength];
    for (int i = 0; i < rawLength; i++) {
      body[i] = (byte) ('A' + (i % 26));
    }
    return body;
  }
}
