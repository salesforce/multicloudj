package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.model.Message.MessageBodyType;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
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
  // enforced; the batcher's byte-size cap is left disabled (0) because the batched message type
  // reports no per-message byte size for the batcher to accumulate.
  protected static final int MAX_BATCH_HANDLERS = 100;
  protected static final int MIN_BATCH_SIZE = 1;
  protected static final int MAX_BATCH_SIZE = 16;

  protected AliBaseTopic(Builder<?, T> builder) {
    super(builder);
  }

  /** Overrides the default batcher options to align with SMQ service limits. */
  @Override
  protected Batcher.Options createBatcherOptions() {
    return new Batcher.Options()
        .setMaxHandlers(MAX_BATCH_HANDLERS)
        .setMinBatchSize(MIN_BATCH_SIZE)
        .setMaxBatchSize(MAX_BATCH_SIZE)
        .setMaxBatchByteSize(0);
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
