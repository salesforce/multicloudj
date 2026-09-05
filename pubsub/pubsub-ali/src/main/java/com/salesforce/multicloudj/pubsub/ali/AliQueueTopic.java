package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.BatchSendException;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.ErrorMessageResult;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.util.ArrayList;
import java.util.List;

/**
 * Alibaba SMQ (MNS) queue publisher.
 *
 * <p>Publishes messages to an SMQ queue via {@code batchPutMessage}. Registered under the {@code
 * alismqqueue} provider id.
 */
@AutoService(AbstractTopic.class)
public class AliQueueTopic extends AliBaseTopic<AliQueueTopic> {

  public static final String PROVIDER_ID = "alismqqueue";

  private final MNSClient mnsClient;
  private final CloudQueue queue;

  public AliQueueTopic() {
    this(new Builder());
  }

  AliQueueTopic(Builder builder) {
    super(builder);
    this.mnsClient = builder.mnsClient;
    this.queue = builder.queue;
  }

  @Override
  public String getProviderId() {
    return PROVIDER_ID;
  }

  /**
   * Publishes a batch of messages to the SMQ queue via {@code batchPutMessage}.
   *
   * <p>SMQ's {@code batchPutMessage} may accept some messages in a batch while rejecting others.
   * On any per-message failure this surfaces the first rejected entry and fails the entire batch,
   * so every message future in the batch completes exceptionally, including messages SMQ already
   * accepted. Under at-least-once delivery, a caller that retries the batch may therefore
   * re-publish the already-accepted messages, so consumers must tolerate duplicate delivery.
   */
  @Override
  protected void doSendBatch(List<Message> messages) {
    if (messages == null || messages.isEmpty()) {
      return;
    }
    List<com.aliyun.mns.model.Message> mnsMessages = new ArrayList<>(messages.size());
    for (Message message : messages) {
      mnsMessages.add(toMnsMessage(message));
    }
    try {
      queue.batchPutMessage(mnsMessages);
    } catch (BatchSendException e) {
      throw mapFailedEntry(e);
    } catch (ServiceException | ClientException e) {
      throw mapException(e);
    }
  }

  /**
   * SMQ throws {@link BatchSendException} when one or more messages in a batch fail to publish; the
   * exception carries the per-message results. Surface the first failed entry as the mapped
   * exception, falling back to the batch exception itself when no per-entry detail is present.
   */
  private SubstrateSdkException mapFailedEntry(BatchSendException e) {
    List<com.aliyun.mns.model.Message> results = e.getMessages();
    if (results != null) {
      for (com.aliyun.mns.model.Message result : results) {
        if (result != null && result.isErrorMessage()) {
          ErrorMessageResult error = result.getErrorMessageDetail();
          String code = error == null ? null : error.getErrorCode();
          String detail = error == null ? "" : error.getErrorMessage();
          return MnsExceptionMapper.mapErrorCode(
              code,
              new RuntimeException(
                  "SMQ batchPutMessage reported a failed entry: code="
                      + code
                      + ", message="
                      + detail,
                  e));
        }
      }
    }
    return mapException(e);
  }

  @Override
  public void close() throws Exception {
    try {
      super.close();
    } catch (Throwable primary) {
      // Keep the shutdown failure (flushing pending batches) as the primary exception, but still
      // close the MNS client so its HTTP resources are not leaked; a client-close failure is
      // attached as suppressed rather than replacing the primary.
      if (mnsClient != null) {
        try {
          mnsClient.close();
        } catch (Throwable clientCloseError) {
          primary.addSuppressed(clientCloseError);
        }
      }
      throw primary;
    }
    if (mnsClient != null) {
      mnsClient.close();
    }
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  /** Builder for {@link AliQueueTopic}. */
  public static class Builder extends AliBaseTopic.Builder<Builder, AliQueueTopic> {

    private CloudQueue queue;

    public Builder() {
      this.providerId = PROVIDER_ID;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public AliQueueTopic build() {
      if (topicName == null || topicName.trim().isEmpty()) {
        throw new InvalidArgumentException("Topic name cannot be null or empty");
      }
      if (mnsClient == null) {
        mnsClient = MnsClientUtil.buildMnsClient(endpoint, credentialsOverrider, proxyEndpoint);
      }
      queue = mnsClient.getQueueRef(topicName);
      return new AliQueueTopic(this);
    }
  }
}
