package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.model.Message.MessageBodyType;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract base for Alibaba SMQ (MNS) topic (publisher) implementations.
 *
 * <p>Holds the logic shared by every SMQ publisher: converting a multicloudj {@link Message} into
 * the SMQ SDK message ({@link #toMnsMessage}), the batch limits ({@link #createBatcherOptions}),
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
  // <Messages> root element), independent of message count, plus for each message its
  // base64-encoded body and a small per-message XML envelope (<Message><MessageBody>...).
  // toMnsMessage sends each body base64-encoded (MessageBodyType.BASE64), so a raw body of N bytes
  // occupies 4*ceil(N/3) wire bytes. FIXED_REQUEST_OVERHEAD_BYTES covers the per-request framing
  // and MESSAGE_ENVELOPE_OVERHEAD_BYTES covers the per-message framing; both reserve conservative
  // headroom so the estimated size (fixed overhead + per-message base64 body + framing allowance)
  // is always at least the true serialized size and stays under the documented 64 KB limit.
  protected static final int MAX_BATCH_BYTE_SIZE = 64 * 1024;
  // Measured fixed per-request framing (XML prolog + <Messages> root) is ~114 bytes; rounded up to
  // 256 for buffer.
  protected static final int FIXED_REQUEST_OVERHEAD_BYTES = 256;
  protected static final int MESSAGE_ENVELOPE_OVERHEAD_BYTES = 64;

  protected AliBaseTopic(Builder<?, T> builder) {
    super(builder);
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
   * Returns the SMQ wire size of {@code message}: the base64-encoded body length (bodies are sent
   * as {@link MessageBodyType#BASE64}) plus a fixed per-message XML envelope allowance.
   *
   * <p>Isolated as a pure size function so it can later be handed to the shared batcher as a
   * per-provider sizer instead of being enforced here.
   */
  protected long measureWireSize(Message message) {
    byte[] body = message.getBody();
    int rawLength = body == null ? 0 : body.length;
    return encodedWireSize(rawLength);
  }

  /**
   * Computes the SMQ wire size of a body of {@code rawLength} raw bytes: the base64 encoding
   * toMnsMessage uses (4 characters per 3-byte group, rounded up) plus the fixed per-message XML
   * envelope allowance.
   *
   * <p>Computed in {@code long} so a near-2 GB raw body cannot overflow the multiplication into a
   * negative or understated size that would slip past the single-message size check in
   * {@link #splitBySize}.
   */
  static long encodedWireSize(int rawLength) {
    long base64Length = 4L * ((rawLength + 2L) / 3L);
    return base64Length + MESSAGE_ENVELOPE_OVERHEAD_BYTES;
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
                + " bytes (fixed request overhead plus base64-encoded body and envelope); measured "
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
    return MnsExceptionMapper.map(t);
  }

  /**
   * Converts a multicloudj {@link Message} into an SMQ SDK message.
   *
   * <p>The body is carried as base64 on the wire ({@link MessageBodyType#BASE64}) so raw bytes
   * round-trip losslessly.
   *
   * <p>Message metadata is not yet supported by the SMQ provider: a non-empty metadata map is
   * rejected rather than silently dropped. Metadata encoding is added in a follow-up change, at
   * which point this method maps the metadata onto the SMQ message user properties.
   */
  protected com.aliyun.mns.model.Message toMnsMessage(Message message) {
    Map<String, String> metadata = message.getMetadata();
    if (metadata != null && !metadata.isEmpty()) {
      throw new UnSupportedOperationException(
          "message metadata is not yet supported by the Alibaba SMQ provider");
    }
    byte[] body = message.getBody();
    com.aliyun.mns.model.Message mnsMessage = new com.aliyun.mns.model.Message();
    mnsMessage.setMessageBody(body == null ? new byte[0] : body, MessageBodyType.BASE64);
    return mnsMessage;
  }

  /** Base builder shared by the SMQ publisher builders. */
  public abstract static class Builder<
          TBuilder extends Builder<TBuilder, TTopic>, TTopic extends AliBaseTopic<TTopic>>
      extends AbstractTopic.Builder<TTopic> {

    protected MNSClient mnsClient;

    /**
     * Injects a pre-built {@link MNSClient}. Primarily a test hook; when unset the client
     * is built from the endpoint, credentials, and proxy in {@code build()}.
     */
    public TBuilder withMnsClient(MNSClient mnsClient) {
      this.mnsClient = mnsClient;
      return self();
    }

    protected abstract TBuilder self();
  }
}
