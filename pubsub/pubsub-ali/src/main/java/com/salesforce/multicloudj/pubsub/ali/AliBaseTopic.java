package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.model.Message.MessageBodyType;
import com.aliyun.mns.model.MessagePropertyValue;
import com.aliyun.mns.model.PropertyType;
import com.google.common.base.Utf8;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base for Alibaba SMQ (MNS) topic (publisher) implementations.
 *
 * <p>Holds the logic shared by every SMQ publisher: converting a multicloudj {@link Message} into
 * the SMQ SDK message ({@link #toSmqMessage}), the batch limits ({@link #createBatcherOptions}),
 * and error translation ({@link #mapException}). Concrete subclasses implement {@code doSendBatch}
 * with the queue- or topic-specific SMQ call.
 */
public abstract class AliBaseTopic<T extends AliBaseTopic<T>> extends AbstractTopic<T> {

  // SMQ BatchSendMessage accepts up to 16 messages per batch. Only this message-count cap is
  // enforced by the batcher; the byte-size cap is enforced separately in the publish path (see
  // splitBySize) because the driver Message the batcher accumulates reports no per-message byte
  // size, leaving the batcher's own byte cap structurally inert.
  protected static final int MAX_BATCH_HANDLERS = 100;
  protected static final int MIN_BATCH_SIZE = 1;
  protected static final int MAX_BATCH_SIZE = 16;

  // Alibaba SMQ BatchSendMessage documents a maximum total payload of 64 KB (65,536 bytes) per
  // batch request. The byte guard (see splitBySize) keeps a conservative upper bound on the actual
  // SDK-serialized request under this documented limit. The SDK serializes a batch as one XML
  // document, so its serialized size is a fixed per-request framing (the XML prolog and the
  // <Messages> root element), independent of message count, plus for each message its serialized
  // body, its user-property (metadata) block, and a small per-message XML envelope
  // (<Message><MessageBody>...). A base64-encoded body of N raw bytes occupies 4*ceil(N/3) wire
  // bytes; a raw body occupies its XML-escaped byte length (see measureWireSize).
  // FIXED_REQUEST_OVERHEAD_BYTES covers the per-request framing and MESSAGE_ENVELOPE_OVERHEAD_BYTES
  // covers the per-message framing; both reserve conservative headroom so the estimated size is
  // always at least the true serialized size and stays under the documented 64 KB limit.
  protected static final int MAX_BATCH_BYTE_SIZE = 64 * 1024;
  // Measured fixed per-request framing (XML prolog + <Messages> root) is ~114 bytes; rounded up to
  // 256 for buffer.
  protected static final int FIXED_REQUEST_OVERHEAD_BYTES = 256;
  protected static final int MESSAGE_ENVELOPE_OVERHEAD_BYTES = 64;

  // Message metadata is carried as SMQ message user properties. SMQ caps a message at 50 user
  // properties, each property name at 256 characters, and each property value at 4096 characters of
  // its serialized wire form: logical characters for a STRING value, base64 characters for a BINARY
  // value (see toPropertyValue). All are enforced fail-fast in toSmqMessage so an over-limit
  // message is rejected before any publish rather than by the service.
  protected static final int MAX_USER_PROPERTIES = 50;
  protected static final int MAX_PROPERTY_VALUE_LENGTH = 4096;
  protected static final int MAX_USER_PROPERTY_KEY_LENGTH = 256;

  // UserProperties framing for the 64 KB byte-size guard. The SMQ SDK serializes a message's
  // metadata as an <UserProperties> element wrapping one <PropertyValue> element per attribute:
  // <PropertyValue><Name>KEY</Name><Value>VALUE</Value><Type>STRING</Type></PropertyValue>. The
  // wrapper element is ~33 bytes and each attribute's fixed tag framing is ~78 bytes; both are
  // rounded up so the estimate stays a conservative upper bound on the serialized size.
  private static final int USER_PROPERTIES_WRAPPER_OVERHEAD_BYTES = 48;
  private static final int PROPERTY_FRAMING_OVERHEAD_BYTES = 96;

  // Non-conforming metadata-key bytes are hex-escaped as "__0xHH__". The escape token starts with
  // this marker; the two hex digits and the closing "__" complete it.
  private static final String KEY_ESCAPE_PREFIX = "__0x";
  private static final String KEY_ESCAPE_SUFFIX = "__";
  private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

  // Reserved user property recording that the body was base64-encoded, so the receiver knows to
  // base64-decode it. A user metadata key that would otherwise encode to this same wire name is
  // force-escaped by encodeMetadataKey (see its reserved-flag collision handling), so no user
  // attribute can ever masquerade as — or be stripped as — this flag.
  static final String RESERVED_BASE64_FLAG_KEY = "base64encoded";

  /**
   * How message bodies are placed on the SMQ wire, and the round-trip contract for readers.
   *
   * <p>SMQ carries a message body as XML text, so a body that is not XML-safe UTF-8 (or that a
   * caller wants encoded regardless) must be base64-encoded to survive the round trip; base64 costs
   * a ~33% size increase. When a body is base64-encoded, the reserved
   * {@link #RESERVED_BASE64_FLAG_KEY} user property is set so the subscription knows to decode it;
   * the subscription base64-decodes a received body ONLY when that flag is present.
   *
   * <p><b>Interop contract:</b> a body published and consumed through this SDK round-trips
   * transparently — the caller's exact bytes are returned. The flag is specific to this SDK, so
   * when the producer or consumer is NOT this SDK the caller owns the encoding contract:
   * <ul>
   *   <li>A non-SDK consumer reading a body this SDK base64-encoded must base64-decode it
   *       itself.</li>
   *   <li>A body from a non-SDK producer carries no flag, so the subscription returns it as the raw
   *       wire bytes unchanged; if that producer base64-encoded the body, the caller must decode
   *       it.</li>
   * </ul>
   * Use {@link #ALWAYS} to guarantee every body is base64 on the wire for a non-SDK consumer that
   * expects that encoding.
   */
  public enum Base64EncodingStrategy {
    /**
     * Base64-encode only bodies that are not XML-safe UTF-8; carry XML-safe UTF-8 bodies as-is. A
     * body is XML-safe UTF-8 when it decodes as valid UTF-8 and contains no XML-illegal control
     * byte (see {@link #isXmlSafe}).
     */
    AUTO,
    /** Always base64-encode the body. */
    ALWAYS
  }

  private final Base64EncodingStrategy bodyEncodingStrategy;

  protected AliBaseTopic(Builder<?, T> builder) {
    super(builder);
    this.bodyEncodingStrategy = builder.bodyEncodingStrategy;
  }

  /**
   * Overrides the default batcher options to align with SMQ service limits.
   *
   * <p>Only the 16-message count cap is set here. The byte-size cap is left disabled (0) because
   * the batcher cannot size the driver {@link Message} it accumulates; the aggregate byte limit is
   * instead enforced in the publish path by {@link #splitBySize}.
   */
  @Override
  protected Batcher.Options createBatcherOptions() {
    return new Batcher.Options()
        .setMaxHandlers(MAX_BATCH_HANDLERS)
        .setMinBatchSize(MIN_BATCH_SIZE)
        .setMaxBatchSize(MAX_BATCH_SIZE)
        .setMaxBatchByteSize(0);
  }

  /**
   * Returns a conservative upper bound on the SMQ wire size of {@code message}: the serialized body
   * (base64-encoded or raw XML text, per the configured {@link Base64EncodingStrategy}), a fixed
   * per-message XML envelope allowance, the serialized size of the message metadata carried as SMQ
   * user properties, and — when the body is base64-encoded — the reserved base64 flag property.
   *
   * <p>Isolated as a pure size function so it can later be handed to the shared batcher as a
   * per-provider sizer instead of being enforced here.
   */
  protected long measureWireSize(Message message) {
    byte[] body = message.getBody() == null ? new byte[0] : message.getBody();
    boolean base64 = shouldBase64EncodeBody(body);
    long size = base64 ? encodedWireSize(body.length) : rawBodyWireSize(body);
    Map<String, String> metadata = message.getMetadata();
    size += metadataWireSize(metadata);
    if (base64) {
      // The base64 flag rides as an extra user property; count it, and count the <UserProperties>
      // wrapper when the message has no other metadata that would already carry it.
      if (metadata == null || metadata.isEmpty()) {
        size += USER_PROPERTIES_WRAPPER_OVERHEAD_BYTES;
      }
      size += propertyWireSize(RESERVED_BASE64_FLAG_KEY, "true");
    }
    return size;
  }

  /**
   * Computes a conservative upper bound on the serialized wire size of {@code metadata} carried as
   * SMQ user properties: the {@code <UserProperties>} wrapper framing plus, per attribute, its
   * {@link #PROPERTY_FRAMING_OVERHEAD_BYTES fixed tag framing}, its encoded key length, and its
   * value's wire length (see {@link #valueWireSize}). Returns 0 for null or empty metadata (no
   * {@code <UserProperties>} element is serialized).
   */
  static long metadataWireSize(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return 0L;
    }
    long size = USER_PROPERTIES_WRAPPER_OVERHEAD_BYTES;
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String value = entry.getValue() == null ? "" : entry.getValue();
      size += propertyWireSize(encodeMetadataKey(entry.getKey()), value);
    }
    return size;
  }

  /**
   * Conservative upper bound on the serialized wire size of one SMQ user property: the fixed
   * per-attribute tag framing, the (ASCII) encoded key length, and the value's wire length (see
   * {@link #valueWireSize}).
   */
  static long propertyWireSize(String encodedKey, String value) {
    return PROPERTY_FRAMING_OVERHEAD_BYTES + encodedKey.length() + valueWireSize(value);
  }

  /**
   * Upper bound on the serialized wire byte length of a user-property value. An XML-safe value
   * rides as a {@link PropertyType#STRING} property (XML text), so it is bounded by its exact
   * XML-escaped byte length (see {@link #xmlEscapedByteLength}) — 1 byte per ordinary byte, more
   * only for the few bytes XML escaping expands. An XML-unsafe value rides as a
   * {@link PropertyType#BINARY} property, whose wire form is the base64 of its UTF-8 bytes (4
   * characters per 3-byte group, rounded up); the base64 alphabet is XML-safe, so no escaping
   * expansion applies.
   */
  static long valueWireSize(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (isXmlSafe(bytes)) {
      return xmlEscapedByteLength(bytes);
    }
    return base64Length(bytes.length);
  }

  /**
   * Number of characters the base64 encoding of {@code rawLength} bytes occupies: 4 characters per
   * 3-byte group with the final group padded, i.e. {@code 4 * ceil(rawLength / 3)}. Computed in
   * {@code long} so a near-2 GB length cannot overflow the multiplication into a negative or
   * understated size.
   */
  static long base64Length(int rawLength) {
    return 4L * ((rawLength + 2L) / 3L);
  }

  /**
   * Computes the SMQ wire size of a base64-encoded body of {@code rawLength} raw bytes: the base64
   * encoding (4 characters per 3-byte group, rounded up) plus the fixed per-message XML envelope
   * allowance. The base64 alphabet is XML-safe, so no escaping expansion applies.
   *
   * <p>Computed in {@code long} so a near-2 GB raw body cannot overflow the multiplication into a
   * negative or understated size that would slip past the single-message size check in
   * {@link #splitBySize}.
   */
  static long encodedWireSize(int rawLength) {
    return base64Length(rawLength) + MESSAGE_ENVELOPE_OVERHEAD_BYTES;
  }

  /**
   * Computes an upper bound on the SMQ wire size of a raw ({@link MessageBodyType#RAW_STRING})
   * body: its XML-escaped byte length plus the fixed per-message XML envelope allowance. A raw body
   * is serialized as XML text, so {@code '&'}, {@code '<'} and {@code '>'} expand to their entity
   * references and carriage return ({@code 0x0D}) to a numeric character reference; all other bytes
   * (including tab, line feed, and UTF-8 continuation bytes) serialize unchanged. The raw path only
   * ever carries XML-safe UTF-8 (AUTO base64-encodes anything that is not), so no XML-illegal
   * control byte ever reaches this sizer.
   */
  static long rawBodyWireSize(byte[] body) {
    return xmlEscapedByteLength(body) + MESSAGE_ENVELOPE_OVERHEAD_BYTES;
  }

  /**
   * Sum of the {@link #escapedByteLength XML-escaped byte length} of every byte in {@code bytes}: a
   * conservative upper bound on the byte count the SMQ SDK produces when it serializes those bytes
   * as XML text. Shared by the raw body sizer ({@link #rawBodyWireSize}) and the STRING
   * user-property value sizer ({@link #valueWireSize}), so both charge XML escaping per byte rather
   * than by a blanket worst-case multiplier.
   */
  static long xmlEscapedByteLength(byte[] bytes) {
    long escaped = 0L;
    for (byte raw : bytes) {
      escaped += escapedByteLength(raw & 0xFF);
    }
    return escaped;
  }

  /**
   * Upper bound on the serialized byte count of a single body byte under XML text escaping:
   * {@code '&'} becomes {@code "&amp;"}, {@code '<'}/{@code '>'} their entities, and carriage
   * return ({@code 0x0D}) the numeric character reference {@code "&#13;"} (so it survives XML
   * line-ending normalization); tab ({@code 0x09}), line feed ({@code 0x0A}) and every other byte
   * (including UTF-8 continuation bytes) serialize unchanged. 6 conservatively covers every escaped
   * form.
   */
  private static int escapedByteLength(int b) {
    if (b == '&' || b == '<' || b == '>' || b == '\r') {
      return 6;
    }
    return 1;
  }

  /**
   * Whether the body is base64-encoded on the wire for the configured strategy: always under
   * {@link Base64EncodingStrategy#ALWAYS}, and under {@link Base64EncodingStrategy#AUTO} only when
   * the body is not XML-safe UTF-8 — that is, when it is not valid UTF-8 or contains an XML-illegal
   * control byte, neither of which can be carried losslessly as raw XML text.
   */
  private boolean shouldBase64EncodeBody(byte[] body) {
    switch (bodyEncodingStrategy) {
      case ALWAYS:
        return true;
      case AUTO:
      default:
        return !(isValidUtf8(body) && isXmlSafe(body));
    }
  }

  /**
   * True if {@code body} is a valid UTF-8 byte sequence. Delegates to Guava's
   * {@link Utf8#isWellFormed(byte[])} well-formedness check rather than driving a
   * {@link java.nio.charset.CharsetDecoder} through exception-based control flow.
   */
  private static boolean isValidUtf8(byte[] body) {
    return Utf8.isWellFormed(body);
  }

  /**
   * True if {@code body} contains no XML 1.0-illegal content. XML text forbids the C0 control
   * characters except tab ({@code 0x09}), line feed ({@code 0x0A}), and carriage return
   * ({@code 0x0D}) — any byte in {@code 0x00}–{@code 0x08}, {@code 0x0B}, {@code 0x0C}, or
   * {@code 0x0E}–{@code 0x1F} — and also the noncharacter code points {@code U+FFFE} and
   * {@code U+FFFF} (the UTF-8 sequences {@code EF BF BE} / {@code EF BF BF}), which are valid UTF-8
   * but illegal in XML text and rejected by the service. None can be carried as raw XML text, so
   * such a body is base64-encoded and such a metadata value is carried as a BINARY property
   * instead. The C0 controls are single-byte and the noncharacters a fixed 3-byte sequence in valid
   * UTF-8, so a byte-level scan suffices both for the {@link #shouldBase64EncodeBody AUTO} body
   * predicate (paired with {@link #isValidUtf8}) and for the metadata value STRING/BINARY choice.
   */
  static boolean isXmlSafe(byte[] body) {
    for (int i = 0; i < body.length; i++) {
      int b = body[i] & 0xFF;
      if (b < 0x20 && b != '\t' && b != '\n' && b != '\r') {
        return false;
      }
      // U+FFFE and U+FFFF are valid UTF-8 (EF BF BE / EF BF BF) but XML 1.0 forbids them in text.
      // Detect the 3-byte sequence directly so no full decode is needed; EF is only ever a 3-byte
      // lead in valid UTF-8, so scanning for it at any offset cannot misfire on a continuation.
      if (b == 0xEF
          && i + 2 < body.length
          && (body[i + 1] & 0xFF) == 0xBF
          && ((body[i + 2] & 0xFF) == 0xBE || (body[i + 2] & 0xFF) == 0xBF)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Packs an already count-capped ({@code <= MAX_BATCH_SIZE}) list into sub-batches whose estimated
   * request size ({@link #FIXED_REQUEST_OVERHEAD_BYTES} plus the cumulative
   * {@link #measureWireSize wire size} of the messages) stays within {@link #MAX_BATCH_BYTE_SIZE},
   * the 64 KB (65,536 bytes) total payload per batch that SMQ BatchSendMessage documents.
   *
   * <p>Fails fast with {@link InvalidArgumentException} if any single message, together with the
   * fixed per-request overhead, exceeds the limit on its own, since such a message can never be
   * sent in any batch. The packing loop is kept self-contained so it could later be lifted into the
   * shared batcher alongside a per-provider sizer.
   */
  protected List<List<Message>> splitBySize(List<Message> messages) {
    List<List<Message>> batches = new ArrayList<>();
    List<Message> current = new ArrayList<>();
    // The accumulator carries the fixed per-request framing up front so each sub-batch's estimated
    // request size (fixed overhead + per-message wire sizes) is bounded by MAX_BATCH_BYTE_SIZE.
    long currentSize = FIXED_REQUEST_OVERHEAD_BYTES;
    for (Message message : messages) {
      long wireSize = measureWireSize(message);
      if (FIXED_REQUEST_OVERHEAD_BYTES + wireSize > MAX_BATCH_BYTE_SIZE) {
        throw new InvalidArgumentException(
            "message exceeds the Alibaba SMQ per-request size limit of "
                + MAX_BATCH_BYTE_SIZE
                + " bytes (fixed request overhead plus serialized body, metadata, and envelope);"
                + " measured "
                + (FIXED_REQUEST_OVERHEAD_BYTES + wireSize)
                + " bytes");
      }
      if (!current.isEmpty() && currentSize + wireSize > MAX_BATCH_BYTE_SIZE) {
        batches.add(current);
        current = new ArrayList<>();
        currentSize = FIXED_REQUEST_OVERHEAD_BYTES;
      }
      current.add(message);
      currentSize += wireSize;
    }
    if (!current.isEmpty()) {
      batches.add(current);
    }
    return batches;
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    return SmqExceptionMapper.map(t);
  }

  /**
   * Converts a multicloudj {@link Message} into an SMQ SDK message.
   *
   * <p>The body is placed on the wire per the configured {@link Base64EncodingStrategy}: as base64
   * ({@link MessageBodyType#BASE64}) or as raw XML text ({@link MessageBodyType#RAW_STRING}). When
   * base64 is applied, the reserved {@link #RESERVED_BASE64_FLAG_KEY} user property is set so the
   * receiver knows to base64-decode the body.
   *
   * <p>Message metadata is mapped onto SMQ user properties natively: each entry's key is
   * {@link #encodeMetadataKey escaped} to the SMQ attribute-name charset, and its value is carried
   * as a {@link PropertyType#STRING} property when XML-safe or a {@link PropertyType#BINARY}
   * property otherwise (a null value is first coalesced to the empty string). The per-message
   * limits (at most {@link #MAX_USER_PROPERTIES} attributes, each value at most
   * {@link #MAX_PROPERTY_VALUE_LENGTH} characters, each encoded key at most
   * {@link #MAX_USER_PROPERTY_KEY_LENGTH} characters) are enforced fail-fast here so an over-limit
   * message is rejected before publish; when the body is base64-encoded the reserved flag occupies
   * one of those attribute slots, so the effective metadata cap is one lower.
   */
  protected com.aliyun.mns.model.Message toSmqMessage(Message message) {
    byte[] body = message.getBody() == null ? new byte[0] : message.getBody();
    boolean base64 = shouldBase64EncodeBody(body);
    com.aliyun.mns.model.Message smqMessage = new com.aliyun.mns.model.Message();
    smqMessage.setMessageBody(
        body, base64 ? MessageBodyType.BASE64 : MessageBodyType.RAW_STRING);
    Map<String, MessagePropertyValue> userProperties =
        toUserProperties(message.getMetadata(), base64);
    if (base64) {
      if (userProperties == null) {
        userProperties = new HashMap<>();
      }
      userProperties.put(RESERVED_BASE64_FLAG_KEY, new MessagePropertyValue(true));
    }
    if (userProperties != null) {
      smqMessage.setUserProperties(userProperties);
    }
    return smqMessage;
  }

  /**
   * Builds the SMQ user-property map from message metadata, enforcing the per-message limits
   * fail-fast, or returns null when there is no metadata to carry. Each value is carried natively
   * and its per-value length limit enforced by {@link #toPropertyValue} against the form the value
   * takes on the wire.
   *
   * <p>When {@code base64Applies}, the caller adds the reserved base64 flag as an extra user
   * property, so it counts toward SMQ's {@link #MAX_USER_PROPERTIES} cap: the metadata may then
   * carry at most {@code MAX_USER_PROPERTIES - 1} attributes (the flag brings the wire total to
   * exactly the cap). Enforcing that effective cap here keeps an over-limit message from slipping
   * past this fail-fast check only to be rejected by the service once the flag is added.
   *
   * @param base64Applies whether the body will be base64-encoded, reserving one user-property slot
   *     for the base64 flag
   * @throws InvalidArgumentException if the metadata exceeds the effective attribute cap (
   *     {@link #MAX_USER_PROPERTIES}, or one fewer when {@code base64Applies}), or if any value
   *     exceeds the SMQ per-value length limit on its wire form (see {@link #toPropertyValue})
   */
  private static Map<String, MessagePropertyValue> toUserProperties(
      Map<String, String> metadata, boolean base64Applies) {
    if (metadata == null || metadata.isEmpty()) {
      return null;
    }
    int effectiveMaxProperties = base64Applies ? MAX_USER_PROPERTIES - 1 : MAX_USER_PROPERTIES;
    if (metadata.size() > effectiveMaxProperties) {
      throw new InvalidArgumentException(
          "message metadata has "
              + metadata.size()
              + " attributes, exceeding the Alibaba SMQ limit of "
              + MAX_USER_PROPERTIES
              + " user properties"
              + (base64Applies
                  ? " (the base64 body flag reserves one, leaving " + effectiveMaxProperties + ")"
                  : ""));
    }
    Map<String, MessagePropertyValue> userProperties = new HashMap<>();
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      // A null metadata value is coalesced to the empty string, which the 2-arg
      // MessagePropertyValue constructor requires (it rejects a null value).
      String value = entry.getValue() == null ? "" : entry.getValue();
      userProperties.put(encodeMetadataKey(entry.getKey()), toPropertyValue(entry.getKey(), value));
    }
    return userProperties;
  }

  /**
   * Builds the native SMQ user-property value for a metadata value, enforcing the SMQ per-value
   * length limit fail-fast on the form the value takes on the wire. An XML-safe value rides as a
   * {@link PropertyType#STRING} property (raw XML text): SMQ measures that limit in logical
   * characters, so it is capped at {@link #MAX_PROPERTY_VALUE_LENGTH} characters and neither XML
   * escaping nor multibyte encoding counts against it. An XML-unsafe value — one carrying an
   * XML-illegal control byte or noncharacter that cannot survive as XML text — rides as a
   * {@link PropertyType#BINARY} property, whose wire form is the base64 of its UTF-8 bytes: SMQ
   * measures that limit on the base64 text, so it is capped at {@link #MAX_PROPERTY_VALUE_LENGTH}
   * base64 characters, i.e. at most 3072 raw UTF-8 bytes (base64 inflates each 3-byte group to 4
   * characters). The two caps differ because SMQ enforces the length on the serialized wire form,
   * which differs by property type.
   *
   * <p>The SDK's BINARY serialize path base64-encodes the value's bytes under the JVM default
   * charset, while its deserialize path reads them back as UTF-8, so a BINARY value round-trips
   * losslessly only where the default charset reproduces the value's UTF-8 bytes. A fail-closed
   * guard rejects the value when it would not (see
   * {@link #binaryValueCorruptsUnderDefaultCharset}), so a BINARY value never ships where it would
   * be silently corrupted on receive.
   *
   * @throws InvalidArgumentException if the value's serialized wire form exceeds the SMQ per-value
   *     length limit, or (BINARY only) if it would be corrupted under a non-UTF-8 default charset
   */
  private static MessagePropertyValue toPropertyValue(String key, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (isXmlSafe(bytes)) {
      // A STRING value rides as XML text; SMQ caps it at MAX_PROPERTY_VALUE_LENGTH logical
      // characters (XML escaping and multibyte encoding do not count toward the limit).
      if (value.length() > MAX_PROPERTY_VALUE_LENGTH) {
        throw new InvalidArgumentException(
            "message metadata value for key '"
                + key
                + "' has length "
                + value.length()
                + ", exceeding the Alibaba SMQ per-value limit of "
                + MAX_PROPERTY_VALUE_LENGTH
                + " characters");
      }
      return new MessagePropertyValue(PropertyType.STRING, value);
    }
    // A BINARY value rides as the base64 of its UTF-8 bytes; SMQ caps that base64 text at
    // MAX_PROPERTY_VALUE_LENGTH characters, so the raw value may hold at most 3072 UTF-8 bytes.
    long base64WireLength = base64Length(bytes.length);
    if (base64WireLength > MAX_PROPERTY_VALUE_LENGTH) {
      throw new InvalidArgumentException(
          "message metadata value for key '"
              + key
              + "' is not XML-safe and rides as base64, whose length "
              + base64WireLength
              + " exceeds the Alibaba SMQ per-value limit of "
              + MAX_PROPERTY_VALUE_LENGTH
              + " characters ("
              + bytes.length
              + " raw UTF-8 bytes)");
    }
    if (binaryValueCorruptsUnderDefaultCharset(value)) {
      throw new InvalidArgumentException(
          "metadata value needs binary encoding, which requires a UTF-8-default JVM "
              + "(set -Dfile.encoding=UTF-8); key="
              + key);
    }
    return new MessagePropertyValue(PropertyType.BINARY, value);
  }

  /**
   * True if {@code value}'s bytes under the default charset differ from its UTF-8 bytes. The SMQ
   * SDK serializes a BINARY property value by base64-ing the string's default-charset bytes but
   * deserializes it as UTF-8, so a BINARY value round-trips losslessly only when the two encodings
   * agree — which they do on a UTF-8-default JVM (the guarded, supported configuration).
   */
  static boolean binaryValueCorruptsUnderDefaultCharset(String value) {
    return !Arrays.equals(value.getBytes(), value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Escapes a metadata key into the SMQ attribute-name charset. SMQ attribute names admit only
   * {@code [A-Za-z0-9._-]} (at most 256 characters, with no leading, trailing, or consecutive dot).
   * Alphanumerics, {@code '-'} and {@code '_'} pass through unchanged; an interior isolated
   * {@code '.'} passes through, while a leading, trailing, or consecutive dot is escaped as
   * {@code __0x2E__}; every other byte is escaped as {@code __0xHH__} over the key's UTF-8 bytes.
   * Two collisions are hardened so the wire form always decodes back to the original key: an
   * underscore that begins a literal {@code __0x} escape marker is escaped so decode cannot mistake
   * the user's text for an encoded byte, and a key that would encode to the reserved base64 flag
   * name has its first byte force-escaped so it can never masquerade as the flag.
   * {@link #decodeMetadataKey} reverses it.
   *
   * @throws InvalidArgumentException if the key is null or empty, or its encoded form exceeds
   *     {@link #MAX_USER_PROPERTY_KEY_LENGTH} characters
   */
  static String encodeMetadataKey(String key) {
    // An empty key encodes to an empty wire name, which SMQ serializes as <Name/> — the service
    // rejects it with an opaque error and the SDK silently drops it on receive, breaking the
    // round-trip guarantee. Reject it fail-fast here so both the emit path (toUserProperties) and
    // the sizing path (metadataWireSize) are guarded consistently. A whitespace-only key such as
    // " " hex-escapes to a valid, non-empty name (e.g. __0x20__) and is left to pass through.
    if (key == null || key.isEmpty()) {
      throw new InvalidArgumentException("message metadata key cannot be null or empty");
    }
    byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
    String encoded = encodeKeyBytes(bytes, false);
    // Reserved-flag-key collision: a user key whose wire form would equal the reserved flag name
    // must not masquerade as (or be stripped as) the flag. Force-escape its first byte so the wire
    // key differs from the reserved name while still decoding back to the user's key.
    if (encoded.equals(RESERVED_BASE64_FLAG_KEY)) {
      encoded = encodeKeyBytes(bytes, true);
    }
    // Defensive: a non-empty key never encodes to an empty form, but guard it explicitly so an
    // empty wire name can never reach SMQ regardless of how the key was constructed.
    if (encoded.isEmpty()) {
      throw new InvalidArgumentException("message metadata key cannot be null or empty");
    }
    if (encoded.length() > MAX_USER_PROPERTY_KEY_LENGTH) {
      throw new InvalidArgumentException(
          "message metadata key encodes to length "
              + encoded.length()
              + ", exceeding the Alibaba SMQ user-property name limit of "
              + MAX_USER_PROPERTY_KEY_LENGTH
              + " characters");
    }
    return encoded;
  }

  /**
   * Encodes a key's UTF-8 {@code bytes} into the SMQ attribute-name charset (see
   * {@link #encodeMetadataKey}). When {@code forceEscapeFirstByte} is set, the first byte is always
   * escaped even if it would otherwise pass through, which lets the caller break a collision with
   * the reserved flag name while preserving a lossless round trip.
   */
  private static String encodeKeyBytes(byte[] bytes, boolean forceEscapeFirstByte) {
    StringBuilder encoded = new StringBuilder(bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      int b = bytes[i] & 0xFF;
      if ((i == 0 && forceEscapeFirstByte) || !isRawKeyByte(bytes, i)) {
        encoded
            .append(KEY_ESCAPE_PREFIX)
            .append(HEX_DIGITS[(b >> 4) & 0xF])
            .append(HEX_DIGITS[b & 0xF])
            .append(KEY_ESCAPE_SUFFIX);
      } else {
        encoded.append((char) b);
      }
    }
    return encoded.toString();
  }

  /**
   * Reverses {@link #encodeMetadataKey}: replaces each {@code __0xHH__} token with the byte it
   * escapes and appends every other (pass-through ASCII) character as its own byte, then decodes
   * the accumulated bytes as UTF-8. Lenient: a malformed token (one not matching {@code __0xHH__})
   * is treated as literal text so decoding never throws.
   */
  static String decodeMetadataKey(String encoded) {
    if (encoded == null) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream(encoded.length());
    int i = 0;
    int n = encoded.length();
    while (i < n) {
      if (isEscapeTokenAt(encoded, i, n)) {
        int high = hexValue(encoded.charAt(i + 4));
        int low = hexValue(encoded.charAt(i + 5));
        out.write((high << 4) | low);
        i += 8;
      } else {
        appendCharAsUtf8(out, encoded.charAt(i));
        i++;
      }
    }
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  /**
   * True for a byte {@link #encodeMetadataKey} emits unescaped at index {@code i} of the key's
   * {@code bytes}: an alphanumeric, {@code '-'}, an underscore that does not begin a literal
   * {@code __0x} escape marker, or an interior isolated {@code '.'} (SMQ rejects a leading,
   * trailing, or consecutive dot in an attribute name, so those are escaped).
   */
  private static boolean isRawKeyByte(byte[] bytes, int i) {
    int b = bytes[i] & 0xFF;
    if (b == '.') {
      return !(i == 0
          || i == bytes.length - 1
          || (bytes[i - 1] & 0xFF) == '.'
          || (bytes[i + 1] & 0xFF) == '.');
    }
    if (b == '_') {
      return !startsEscapeMarker(bytes, i);
    }
    return isConformingKeyByte(b);
  }

  /**
   * True if a literal {@code __0x} escape-token marker begins at index {@code i} of {@code bytes}.
   */
  private static boolean startsEscapeMarker(byte[] bytes, int i) {
    return i + 3 < bytes.length
        && (bytes[i] & 0xFF) == '_'
        && (bytes[i + 1] & 0xFF) == '_'
        && (bytes[i + 2] & 0xFF) == '0'
        && (bytes[i + 3] & 0xFF) == 'x';
  }

  /** True for a byte that always passes through a key unescaped: {@code [A-Za-z0-9-]}. */
  private static boolean isConformingKeyByte(int b) {
    return (b >= 'A' && b <= 'Z')
        || (b >= 'a' && b <= 'z')
        || (b >= '0' && b <= '9')
        || b == '-';
  }

  /** True if a well-formed {@code __0xHH__} escape token begins at index {@code i} of {@code s}. */
  private static boolean isEscapeTokenAt(String s, int i, int n) {
    return i + 8 <= n
        && s.charAt(i) == '_'
        && s.charAt(i + 1) == '_'
        && s.charAt(i + 2) == '0'
        && s.charAt(i + 3) == 'x'
        && isHexDigit(s.charAt(i + 4))
        && isHexDigit(s.charAt(i + 5))
        && s.charAt(i + 6) == '_'
        && s.charAt(i + 7) == '_';
  }

  private static boolean isHexDigit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
  }

  private static int hexValue(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    return c - 'a' + 10;
  }

  /** Appends a literal (non-token) character to {@code out} as its UTF-8 bytes. */
  private static void appendCharAsUtf8(ByteArrayOutputStream out, char c) {
    if (c < 0x80) {
      out.write(c);
    } else {
      byte[] utf8 = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
      out.write(utf8, 0, utf8.length);
    }
  }

  /** Base builder shared by the SMQ publisher builders. */
  public abstract static class Builder<
          TBuilder extends Builder<TBuilder, TTopic>, TTopic extends AliBaseTopic<TTopic>>
      extends AbstractTopic.Builder<TTopic> {

    protected MNSClient smqClient;
    protected Base64EncodingStrategy bodyEncodingStrategy = Base64EncodingStrategy.AUTO;

    /**
     * Injects a pre-built {@link MNSClient}. Primarily a test hook; when unset the client
     * is built from the endpoint, credentials, and proxy in {@code build()}.
     */
    public TBuilder withSmqClient(MNSClient smqClient) {
      this.smqClient = smqClient;
      return self();
    }

    /**
     * Sets how message bodies are placed on the SMQ wire. Defaults to
     * {@link Base64EncodingStrategy#AUTO}. A null argument resets the default.
     */
    public TBuilder withBodyEncodingStrategy(Base64EncodingStrategy strategy) {
      this.bodyEncodingStrategy = strategy == null ? Base64EncodingStrategy.AUTO : strategy;
      return self();
    }

    protected abstract TBuilder self();
  }
}
