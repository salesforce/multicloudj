package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * High-level client for publishing messages to a pubsub topic.
 * 
 * TopicClient provides a simplified interface for sending messages to cloud
 * pubsub queue/topic in a provider-agnostic way. It handles batching, retries, and
 * error conversion automatically.
 */
public class TopicClient implements AutoCloseable {

    protected AbstractTopic<?> topic;

    protected TopicClient(AbstractTopic<?> topic) {
        this.topic = topic;
    }

    /**
     * Creates a new TopicClientBuilder for the specified provider.
     *
     * @param providerId The cloud provider identifier (e.g., "aws", "gcp", "ali")
     * @return A new TopicClientBuilder instance
     */
    public static TopicClientBuilder builder(String providerId) {
        return new TopicClientBuilder(providerId);
    }

    /**
     * Sends a single message to the topic.
     * 
     * <p>This method will block until the message is successfully sent or an error occurs.
     * Messages are automatically batched by the underlying driver for efficiency.
     *
     * @param message The message to send
     * @throws SubstrateSdkException If the send operation fails
     */
    public void send(Message message) {
        try {
            topic.send(message);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = topic.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Implements AutoCloseable interface for try-with-resources support.
     * 
     * Shuts down the topic client and releases all resources.
     * After calling this method, no more messages can be sent through this client.
     * Any pending messages will be flushed before shutdown completes.
     * It's safe to call this method multiple times.
     * 
     * @throws SubstrateSdkException If the shutdown operation fails
     */
    @Override
    public void close() throws Exception {
        try {
            topic.close();
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = topic.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Builder for creating TopicClient instances.
     */
    public static class TopicClientBuilder {

        private final AbstractTopic.Builder<?> topicBuilder;

        public TopicClientBuilder(String providerId) {
            this.topicBuilder = ProviderSupplier.findTopicProviderBuilder(providerId);
        }

        /**
         * Sets the topic name or ARN.
         *
         * @param topicName The topic name or ARN
         * @return This builder instance
         */
        public TopicClientBuilder withTopicName(String topicName) {
            this.topicBuilder.withTopicName(topicName);
            return this;
        }

        /**
         * Sets the region for the topic.
         *
         * @param region The region
         * @return This builder instance
         */
        public TopicClientBuilder withRegion(String region) {
            this.topicBuilder.withRegion(region);
            return this;
        }

        /**
         * Sets a custom endpoint override.
         *
         * @param endpoint The endpoint URI
         * @return This builder instance
         */
        public TopicClientBuilder withEndpoint(URI endpoint) {
            this.topicBuilder.withEndpoint(endpoint);
            return this;
        }

        /**
         * Sets a proxy endpoint.
         *
         * @param proxyEndpoint The proxy endpoint URI
         * @return This builder instance
         */
        public TopicClientBuilder withProxyEndpoint(URI proxyEndpoint) {
            this.topicBuilder.withProxyEndpoint(proxyEndpoint);
            return this;
        }

        /**
         * Sets credentials overrider for custom authentication.
         *
         * @param credentialsOverrider The credentials overrider
         * @return This builder instance
         */
        public TopicClientBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.topicBuilder.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        /**
         * Builds and returns a new TopicClient instance.
         *
         * @return A new TopicClient
         */
        public TopicClient build() {
            return new TopicClient(topicBuilder.build());
        }
    }
} 