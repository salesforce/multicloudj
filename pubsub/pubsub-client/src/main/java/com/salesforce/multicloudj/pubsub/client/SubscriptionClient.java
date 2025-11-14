package com.salesforce.multicloudj.pubsub.client;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

/**
 * High-level client for receiving messages from a pubsub subscription.
 * 
 * SubscriptionClient provides a simplified interface for receiving messages from cloud
 * pubsub services in a provider-agnostic way. It handles batching, acknowledgments, and
 * error conversion automatically.
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
     * Receives and returns the next message from the subscription.
     * This method will block until a message is available
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
     * Acknowledged messages will not be redelivered. The acknowledgment
     * is sent asynchronously in the background.
     *
     * @param ackID The acknowledgment ID of the message to acknowledge
     * @throws SubstrateSdkException If the ack operation fails
     */
    public void sendAck(String ackID) {
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
     * This is more efficient than calling sendAck multiple times.
     * Acknowledged messages will not be redelivered.
     *
     * @param ackIDs The list of acknowledgment IDs to acknowledge
     * @return A CompletableFuture that completes when all acks are sent
     */
    public CompletableFuture<Void> sendAcks(List<String> ackIDs) {
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
     * Nacked messages will be redelivered for retry. Not all providers
     * support nacking - check canNack() first.
     *
     * @param ackID The acknowledgment ID of the message to nack
     * @throws SubstrateSdkException If the nack operation fails
     * @throws UnsupportedOperationException If the provider doesn't support nacking
     */
    public void sendNack(String ackID) {
        try {
            subscription.sendNack(ackID);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = subscription.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Negatively acknowledges multiple messages in a batch.
     * 
     * This is more efficient than calling sendNack multiple times.
     * Nacked messages will be redelivered for retry.
     *
     * @param ackIDs The list of acknowledgment IDs to nack
     * @return A CompletableFuture that completes when all nacks are sent
     * @throws UnsupportedOperationException If the provider doesn't support nacking
     */
    public CompletableFuture<Void> sendNacks(List<String> ackIDs) {
        try {
            return subscription.sendNacks(ackIDs);
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
     * This may include provider-specific configuration like delivery delay,
     * message retention period, etc.
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
     * This can be used by application code to determine whether to retry
     * a failed operation or consider it a permanent failure.
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

    /**
     * Builder for creating SubscriptionClient instances.
     */
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
        public SubscriptionClientBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.subscriptionBuilder.withCredentialsOverrider(credentialsOverrider);
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