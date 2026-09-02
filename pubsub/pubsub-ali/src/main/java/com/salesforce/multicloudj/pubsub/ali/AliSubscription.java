package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.BatchDeleteException;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.common.ServiceHandlingRequiredException;
import com.aliyun.mns.model.ErrorMessageResult;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.AckInfo;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Alibaba SMQ (MNS) subscription.
 *
 * <p>Receives messages from an SMQ queue via {@code batchPopMessage}, acknowledges them via {@code
 * batchDeleteMessage}, and nacks them via {@code changeMessageVisibility}. Receipt
 * handles are modelled as {@link AliAckID}. Registered under the {@code ali} provider id.
 */
@AutoService(AbstractSubscription.class)
public class AliSubscription extends AbstractSubscription<AliSubscription> {

  public static final String PROVIDER_ID = "ali";

  // SMQ batch limits: BatchReceiveMessage / BatchDeleteMessage accept up to 16 messages.
  private static final int MAX_BATCH_HANDLERS = 100;
  private static final int MAX_BATCH_SIZE = 16;

  // SMQ long-poll waits at most 30s per receive call.
  private static final int MAX_WAIT_SECONDS = 30;

  // SMQ ChangeMessageVisibility accepts a 1..43200s (12h) timeout; 0 is rejected.
  private static final int MIN_VISIBILITY_TIMEOUT_SECONDS = 1;
  private static final int MAX_VISIBILITY_TIMEOUT_SECONDS = 43200;

  private static final long NO_MESSAGES_POLL_DURATION_MS = 250;

  // SMQ codes signalling the message a receipt handle referred to is already gone (benign under
  // at-least-once redelivery): the message was deleted, or the receipt handle expired.
  private static final String MESSAGE_NOT_EXIST = "MessageNotExist";
  private static final String RECEIPT_HANDLE_ERROR = "ReceiptHandleError";

  private final MNSClient mnsClient;
  private final CloudQueue queue;
  private final int waitSeconds;

  public AliSubscription() {
    this(new Builder());
  }

  AliSubscription(Builder builder) {
    super(builder);
    this.mnsClient = builder.mnsClient;
    this.queue = builder.queue;
    this.waitSeconds = (int) Math.min(Math.max(builder.waitSeconds, 0), MAX_WAIT_SECONDS);
  }

  @Override
  public String getProviderId() {
    return PROVIDER_ID;
  }

  @Override
  protected List<Message> doReceiveBatch(int batchSize) {
    int n = Math.min(batchSize, MAX_BATCH_SIZE);
    List<com.aliyun.mns.model.Message> raw;
    try {
      raw = waitSeconds > 0 ? queue.batchPopMessage(n, waitSeconds) : queue.batchPopMessage(n);
    } catch (ServiceException e) {
      // An empty queue surfaces as MessageNotExist; treat it as "no messages", not an error.
      if (queue.isMessageNotExist(e)) {
        return sleepThenEmpty();
      }
      throw mapException(e);
    } catch (ClientException | ServiceHandlingRequiredException e) {
      throw mapException(e);
    }

    if (raw == null || raw.isEmpty()) {
      return sleepThenEmpty();
    }

    List<Message> messages = new ArrayList<>(raw.size());
    for (com.aliyun.mns.model.Message mnsMessage : raw) {
      messages.add(toMessage(mnsMessage));
    }
    return messages;
  }

  private List<Message> sleepThenEmpty() {
    // Only back off when not long-polling; a long-poll receive already blocks server-side.
    if (waitSeconds <= 0) {
      try {
        Thread.sleep(NO_MESSAGES_POLL_DURATION_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SubstrateSdkException("Interrupted while waiting for messages", e);
      }
    }
    return new ArrayList<>();
  }

  /**
   * Converts an SMQ SDK message into a multicloudj {@link Message}.
   *
   * <p>The body is base64-decoded back to raw bytes. Message metadata (SMQ user properties) is not
   * yet decoded; a follow-up change populates it here.
   */
  private Message toMessage(com.aliyun.mns.model.Message mnsMessage) {
    byte[] body = mnsMessage.getMessageBodyAsBytes();
    return Message.builder()
        .withBody(body == null ? new byte[0] : body)
        .withAckID(new AliAckID(mnsMessage.getReceiptHandle()))
        .withLoggableID(mnsMessage.getMessageId())
        .build();
  }

  @Override
  protected void doSendAcks(List<AckID> ackIDs) {
    if (ackIDs == null || ackIDs.isEmpty()) {
      return;
    }
    List<String> receiptHandles = new ArrayList<>(ackIDs.size());
    for (AckID ackID : ackIDs) {
      receiptHandles.add(receiptHandleOf(ackID));
    }
    for (int i = 0; i < receiptHandles.size(); i += MAX_BATCH_SIZE) {
      int end = Math.min(i + MAX_BATCH_SIZE, receiptHandles.size());
      List<String> chunk = new ArrayList<>(receiptHandles.subList(i, end));
      try {
        queue.batchDeleteMessage(chunk);
      } catch (BatchDeleteException e) {
        // Defensive: batchDeleteMessage catches a per-handle BatchDeleteException internally and
        // re-throws it wrapped in a ServiceHandlingRequiredException (handled below), so this
        // branch is unreachable via that call today; kept in case a future path throws it directly.
        tolerateOrSurfaceBatchDelete(e.getErrorMessages(), e);
      } catch (ServiceHandlingRequiredException e) {
        // batchDeleteMessage re-throws the SDK's per-handle BatchDeleteException wrapped in a
        // ServiceHandlingRequiredException. That wrapper's errorCode reflects only one (arbitrary)
        // per-handle entry, so inspect every entry in the wrapped batch failure rather than trust
        // the single code, which could otherwise mask a non-benign per-handle failure.
        if (e.getCause() instanceof BatchDeleteException) {
          tolerateOrSurfaceBatchDelete(((BatchDeleteException) e.getCause()).getErrorMessages(), e);
        } else if (!isAlreadyGone(e.getErrorCode())) {
          // No per-handle detail: a benign already-gone handle is nothing to delete; surface rest.
          throw mapException(e);
        }
      } catch (ServiceException | ClientException e) {
        throw mapException(e);
      }
    }
  }

  /**
   * Inspects a batch-delete per-handle failure map, tolerating handles whose message is already
   * gone (benign redelivery-timing races) and surfacing the first non-benign failure. Fails closed
   * by surfacing {@code toSurface} when the map carries no per-handle detail, so a malformed batch
   * failure is never silently swallowed.
   */
  private void tolerateOrSurfaceBatchDelete(
      Map<String, ErrorMessageResult> failures, Throwable toSurface) {
    if (failures == null || failures.isEmpty()) {
      throw mapException(toSurface);
    }
    for (ErrorMessageResult error : failures.values()) {
      String code = error == null ? null : error.getErrorCode();
      if (!isAlreadyGone(code)) {
        throw MnsExceptionMapper.mapErrorCode(code, toSurface);
      }
    }
  }

  /**
   * True for SMQ error codes indicating the message a receipt handle referred to is already gone
   * (deleted or its handle expired). Both are benign under at-least-once redelivery and must not
   * fail an ack.
   */
  private static boolean isAlreadyGone(String errorCode) {
    return MESSAGE_NOT_EXIST.equals(errorCode) || RECEIPT_HANDLE_ERROR.equals(errorCode);
  }

  /**
   * Returns the SMQ receipt handle for an {@link AckID}, rejecting a wrong-type (non-{@link
   * AliAckID}) handle with an {@link InvalidArgumentException}. Such a handle would otherwise
   * stringify to a bogus receipt handle that SMQ treats as a benign already-gone handle, so the
   * ack/nack would silently no-op and the message would loop forever with no surfaced error.
   *
   * <p>This runs inside {@code doSendAcks}/{@code doSendNacks}, which the shared subscription
   * executes on its background ack/nack batcher after {@code sendAck}/{@code sendNack} have already
   * enqueued and returned. The rejection is therefore deferred rather than synchronous to the
   * caller: the {@link InvalidArgumentException} becomes the subscription's permanent-error state,
   * observable on a later {@code receive()} or when {@code close()} drains the batcher, consistent
   * with the shared asynchronous ack model.
   */
  private static String receiptHandleOf(AckID ackID) {
    if (!(ackID instanceof AliAckID)) {
      throw new InvalidArgumentException(
          "Unexpected AckID type for SMQ subscription: "
              + (ackID == null ? "null" : ackID.getClass().getName()));
    }
    return ackID.toString();
  }

  @Override
  protected void doSendNacks(List<AckInfo> nacks) {
    if (nacks == null || nacks.isEmpty()) {
      return;
    }
    Duration subscriptionDefault = getNackVisibilityTimeout();
    for (AckInfo nack : nacks) {
      Duration timeout =
          nack.getVisibilityTimeout() != null ? nack.getVisibilityTimeout() : subscriptionDefault;
      int seconds = clampVisibility(timeout);
      try {
        queue.changeMessageVisibility(receiptHandleOf(nack.getAckID()), seconds);
      } catch (ServiceException e) {
        // Message already deleted or the receipt handle expired: nothing to make visible.
        if (queue.isMessageNotExist(e) || isAlreadyGone(e.getErrorCode())) {
          continue;
        }
        throw mapException(e);
      } catch (ClientException e) {
        throw mapException(e);
      }
    }
  }

  /**
   * Clamps a nack visibility timeout into SMQ's valid {@code [1, 43200]} second range. SMQ rejects
   * a 0-second timeout, so the lower bound is 1: a nack that resolves to 0 (or a null default)
   * redelivers after 1s rather than immediately. The result is never 0.
   */
  private static int clampVisibility(Duration timeout) {
    if (timeout == null) {
      return MIN_VISIBILITY_TIMEOUT_SECONDS;
    }
    long seconds = Math.max(timeout.getSeconds(), MIN_VISIBILITY_TIMEOUT_SECONDS);
    return (int) Math.min(seconds, MAX_VISIBILITY_TIMEOUT_SECONDS);
  }

  @Override
  protected Batcher.Options createReceiveBatcherOptions() {
    return new Batcher.Options()
        .setMaxHandlers(MAX_BATCH_HANDLERS)
        .setMinBatchSize(1)
        .setMaxBatchSize(MAX_BATCH_SIZE)
        .setMaxBatchByteSize(0);
  }

  @Override
  protected Batcher.Options createAckBatcherOptions() {
    return new Batcher.Options()
        .setMaxHandlers(MAX_BATCH_HANDLERS)
        .setMinBatchSize(1)
        .setMaxBatchSize(MAX_BATCH_SIZE)
        .setMaxBatchByteSize(0);
  }

  @Override
  public boolean canNack() {
    return true;
  }

  @Override
  public boolean isRetryable(Throwable error) {
    // Delegate to the mapped exception's own retryability rather than treating every surfaced
    // error as terminal, which would permanently kill a subscription on a transient prefetch
    // failure. Honoring that flag follows the SDK-wide default-retryable convention: this treats
    // as retryable not only throttling (a ResourceExhaustedException) but also every
    // UnknownException-mapped error, which covers transport failures, InternalError, and any
    // unmapped code; non-retryable types (e.g. authorization) stay terminal.
    return error instanceof SubstrateSdkException && ((SubstrateSdkException) error).isRetryable();
  }

  @Override
  public GetAttributeResult getAttributes() {
    return new GetAttributeResult.Builder()
        .name(subscriptionName)
        .topic(queue.getQueueURL())
        .build();
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    return MnsExceptionMapper.map(t);
  }

  @Override
  public void close() throws Exception {
    try {
      super.close();
    } catch (Throwable primary) {
      // Keep the shutdown failure (draining pending acks or reporting an ack error) as the primary
      // exception, but still close the MNS client so its HTTP resources are not leaked; a
      // client-close failure is attached as suppressed rather than replacing the primary.
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

  /** SMQ receipt-handle-backed {@link AckID}. */
  public static class AliAckID implements AckID {
    private final String receiptHandle;

    public AliAckID(String receiptHandle) {
      if (receiptHandle == null || receiptHandle.trim().isEmpty()) {
        throw new IllegalArgumentException("Receipt handle cannot be null or empty");
      }
      this.receiptHandle = receiptHandle;
    }

    @Override
    public String toString() {
      return receiptHandle;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof AliAckID)) {
        return false;
      }
      return receiptHandle.equals(((AliAckID) other).receiptHandle);
    }

    @Override
    public int hashCode() {
      return receiptHandle.hashCode();
    }
  }

  /** Builder for {@link AliSubscription}. */
  public static class Builder extends AbstractSubscription.Builder<AliSubscription> {

    private MNSClient mnsClient;
    private CloudQueue queue;
    private long waitSeconds = 0;

    public Builder() {
      this.providerId = PROVIDER_ID;
    }

    /**
     * Injects a pre-built {@link MNSClient}. Primarily a test hook; when unset the client
     * is built from the endpoint, credentials, and proxy in {@code build()}.
     */
    public Builder withMnsClient(MNSClient mnsClient) {
      this.mnsClient = mnsClient;
      return this;
    }

    /**
     * Sets the SMQ long-poll wait, in seconds, applied to each receive. Negative values are
     * rejected; values above the SMQ maximum of 30s are clamped to 30s. 0 (default) means a short
     * poll.
     *
     * @throws InvalidArgumentException if {@code waitSeconds} is negative
     */
    public Builder withWaitTimeSeconds(long waitSeconds) {
      if (waitSeconds < 0) {
        throw new InvalidArgumentException("waitTimeSeconds cannot be negative");
      }
      this.waitSeconds = waitSeconds;
      return this;
    }

    @Override
    public AliSubscription build() {
      if (subscriptionName == null || subscriptionName.trim().isEmpty()) {
        throw new InvalidArgumentException("Subscription name cannot be null or empty");
      }
      if (mnsClient == null) {
        mnsClient = MnsClientUtil.buildMnsClient(endpoint, credentialsOverrider, proxyEndpoint);
      }
      queue = mnsClient.getQueueRef(subscriptionName);
      return new AliSubscription(this);
    }
  }
}
