package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * High-level client for receiving messages from a pubsub subscription.
 *
 * <p>SubscriptionClient provides a simplified interface for receiving messages from cloud pubsub
 * services in a provider-agnostic way. It handles batching, acknowledgments, and error conversion
 * automatically.
 */
public class SubscriptionClient implements AutoCloseable {

  protected AbstractSubscription<?> subscription;

  protected SubscriptionClient(AbstractSubscription<?> subscription) {
    this.subscription = subscription;
  }

  /**
   * Creates a new SubscriptionClientBuilder for the specified provider.
   *
   * @param providerId The cloud provider identifier (e.g., "aws", "gcp", "ali")
   * @return A new SubscriptionClientBuilder instance
   */
  public static SubscriptionClientBuilder builder(String providerId) {
    return new SubscriptionClientBuilder(providerId);
  }

  /**
   * Receives and returns the next message from the subscription. This method will block until a
   * message is available
   *
   * @return The next message from the subscription
   * @throws SubstrateSdkException If the receive operation fails
   */
  public Message receive() {
    try {
      return subscription.receive();
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
      return null; // Never reached due to exception propagation
    }
  }

  /**
   * Acknowledges a single message, indicating successful processing.
   *
   * <p>Acknowledged messages will not be redelivered. The acknowledgment is sent asynchronously in
   * the background.
   *
   * @param ackID The acknowledgment ID of the message to acknowledge
   * @throws SubstrateSdkException If the ack operation fails
   */
  public void sendAck(AckID ackID) {
    try {
      subscription.sendAck(ackID);
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
    }
  }

  /**
   * Acknowledges multiple messages in a batch.
   *
   * <p>This is more efficient than calling sendAck multiple times. Acknowledged messages will not
   * be redelivered.
   *
   * @param ackIDs The list of acknowledgment IDs to acknowledge
   * @return A CompletableFuture that completes when all acks are sent
   */
  public CompletableFuture<Void> sendAcks(List<AckID> ackIDs) {
    try {
      return subscription.sendAcks(ackIDs);
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
      return CompletableFuture.failedFuture(t);
    }
  }

  /**
   * Negatively acknowledges a single message, indicating processing failure.
   *
   * <p>Nacked messages will be redelivered for retry. Not all providers support nacking - check
   * canNack() first.
   *
   * @param ackID The acknowledgment ID of the message to nack
   * @throws SubstrateSdkException If the nack operation fails
   * @throws UnsupportedOperationException If the provider doesn't support nacking
   */
  public void sendNack(AckID ackID) {
    try {
      subscription.sendNack(ackID);
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
    }
  }

  /**
   * Negatively acknowledges a single message, overriding the subscription's default nack
   * visibility timeout for this call.
   *
   * <p>This allows callers to request a specific redelivery delay for individual messages without
   * reconfiguring the subscription. 
   *
   * @param ackID The acknowledgment ID of the message to nack
   * @param visibilityTimeout The visibility timeout to apply for this nack.
   * @throws SubstrateSdkException If the nack operation fails
   * @throws UnsupportedOperationException If the provider doesn't support nacking
   */
  public void sendNack(AckID ackID, Duration visibilityTimeout) {
    try {
      subscription.sendNack(ackID, visibilityTimeout);
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
    }
  }

  /**
   * Negatively acknowledges multiple messages in a batch.
   *
   * <p>This is more efficient than calling sendNack multiple times. Nacked messages will be
   * redelivered for retry.
   *
   * @param ackIDs The list of acknowledgment IDs to nack
   * @return A CompletableFuture that completes when all nacks are sent
   * @throws UnsupportedOperationException If the provider doesn't support nacking
   */
  public CompletableFuture<Void> sendNacks(List<AckID> ackIDs) {
    try {
      return subscription.sendNacks(ackIDs);
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
      return CompletableFuture.failedFuture(t);
    }
  }

  /**
   * Negatively acknowledges multiple messages in a batch, overriding the subscription's default
   * nack visibility timeout for the entire batch.
   *
   * @param ackIDs The list of acknowledgment IDs to nack
   * @param visibilityTimeout The visibility timeout to apply to every nack in this batch. When
   *     {@code null}, the subscription's default nack visibility timeout is used.
   * @return A CompletableFuture that completes when all nacks are sent
   * @throws UnsupportedOperationException If the provider doesn't support nacking
   */
  public CompletableFuture<Void> sendNacks(List<AckID> ackIDs, Duration visibilityTimeout) {
    try {
      return subscription.sendNacks(ackIDs, visibilityTimeout);
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
      return CompletableFuture.failedFuture(t);
    }
  }

  /**
   * Checks if this subscription supports negative acknowledgments (nacking).
   *
   * @return true if nacking is supported, false otherwise
   */
  public boolean canNack() {
    return subscription.canNack();
  }

  /**
   * Gets the attributes/metadata of this subscription.
   *
   * <p>This may include provider-specific configuration like delivery delay, message retention
   * period, etc.
   *
   * @return A GetAttributeResult containing subscription attributes
   * @throws SubstrateSdkException If the operation fails
   */
  public GetAttributeResult getAttributes() {
    try {
      return subscription.getAttributes();
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
      return null; // Never reached due to exception propagation
    }
  }

  /**
   * Determines if an error is retryable.
   *
   * <p>This can be used by application code to determine whether to retry a failed operation or
   * consider it a permanent failure.
   *
   * @param error The error to check
   * @return true if the error is retryable, false otherwise
   */
  public boolean isRetryable(Throwable error) {
    return subscription.isRetryable(error);
  }

  /* Shuts down the subscription client and releases all resources.
   *
   * After calling this method, no more messages can be received through this client.
   * Any pending acknowledgments will be flushed before shutdown completes.
   * It's safe to call this method multiple times.
   *
   * @throws Exception If the shutdown operation fails
   */
  @Override
  public void close() throws Exception {
    try {
      subscription.close();
    } catch (Throwable t) {
      Class<? extends SubstrateSdkException> exception = subscription.getException(t);
      ExceptionHandler.handleAndPropagate(exception, t);
    }
  }

  /** Builder for creating SubscriptionClient instances. */
  public static class SubscriptionClientBuilder {

    private final AbstractSubscription.Builder<?> subscriptionBuilder;

    public SubscriptionClientBuilder(String providerId) {
      this.subscriptionBuilder = ProviderSupplier.findSubscriptionProviderBuilder(providerId);
    }

    /**
     * Sets the subscription name or ARN.
     *
     * @param subscriptionName The subscription name or ARN
     * @return This builder instance
     */
    public SubscriptionClientBuilder withSubscriptionName(String subscriptionName) {
      this.subscriptionBuilder.withSubscriptionName(subscriptionName);
      return this;
    }

    /**
     * Sets the region for the subscription.
     *
     * @param region The region
     * @return This builder instance
     */
    public SubscriptionClientBuilder withRegion(String region) {
      this.subscriptionBuilder.withRegion(region);
      return this;
    }

    /**
     * Sets a custom endpoint override.
     *
     * @param endpoint The endpoint URI
     * @return This builder instance
     */
    public SubscriptionClientBuilder withEndpoint(URI endpoint) {
      this.subscriptionBuilder.withEndpoint(endpoint);
      return this;
    }

    /**
     * Sets a proxy endpoint.
     *
     * @param proxyEndpoint The proxy endpoint URI
     * @return This builder instance
     */
    public SubscriptionClientBuilder withProxyEndpoint(URI proxyEndpoint) {
      this.subscriptionBuilder.withProxyEndpoint(proxyEndpoint);
      return this;
    }

    /**
     * Sets credentials overrider for custom authentication.
     *
     * @param credentialsOverrider The credentials overrider
     * @return This builder instance
     */
    public SubscriptionClientBuilder withCredentialsOverrider(
        CredentialsOverrider credentialsOverrider) {
      this.subscriptionBuilder.withCredentialsOverrider(credentialsOverrider);
      return this;
    }

    /**
     * Sets the visibility timeout applied when a message is negatively acknowledged (nacked).
     *
     * <p>When set to {@link Duration#ZERO} (the default), nacked messages are made immediately
     * available for redelivery. A positive duration delays redelivery by that amount, giving
     * downstream consumers time before the provider re-queues the message. A negative or null
     * value is treated as zero (immediate redelivery).
     *
     * <p>This option is ignored by providers that do not expose a configurable nack visibility
     * timeout. Per-call timeouts passed to {@link SubscriptionClient#sendNack(AckID, Duration)}
     * or {@link SubscriptionClient#sendNacks(List, Duration)} take precedence over this default.
     *
     * @param nackVisibilityTimeout the visibility timeout to apply on nack
     * @return This builder instance
     */
    public SubscriptionClientBuilder withNackVisibilityTimeout(Duration nackVisibilityTimeout) {
      this.subscriptionBuilder.withNackVisibilityTimeout(nackVisibilityTimeout);
      return this;
    }

    /**
     * Builds and returns a new SubscriptionClient instance.
     *
     * @return A new SubscriptionClient
     */
    public SubscriptionClient build() {
      return new SubscriptionClient(subscriptionBuilder.build());
    }
  }
}
