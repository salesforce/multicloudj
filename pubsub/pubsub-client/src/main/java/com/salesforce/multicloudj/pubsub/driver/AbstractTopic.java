package com.salesforce.multicloudj.pubsub.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.utils.MessageUtils;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Abstract base class for topic implementations.
 *
 * All message sending operations use the Batcher infrastructure.
 * 
 * Features provided:
 * - Batching using batcher infrastructure and default batcher configuration
 * - Message validation and preprocessing
 * - Exception propagation (cloud providers handle their own exception mapping)
 * - Before/after send hooks for extensibility
 * - Implementation of send and sendBatch methods
 * 
 * Drivers need to provide:
 * - doSendBatch(List) - Provider-specific synchronous batch sending
 * - Override batching configuration methods for provider-specific settings
 * - Override hook methods for custom pre/post-send behavior
 */
public abstract class AbstractTopic<T extends AbstractTopic<T>> implements AutoCloseable {

    protected final String providerId;
    protected final String topicName;
    protected final String region;
    protected final URI endpoint;
    protected final URI proxyEndpoint;
    protected final CredentialsOverrider credentialsOverrider;
    protected final Batcher<Message> batcher;

    protected AbstractTopic(String providerId, String topicName, String region, URI endpoint, URI proxyEndpoint, CredentialsOverrider credentialsOverrider) {
        this.providerId = providerId;
        this.topicName = topicName;
        this.region = region;
        this.endpoint = endpoint;
        this.proxyEndpoint = proxyEndpoint;
        this.credentialsOverrider = credentialsOverrider;
        this.batcher = new Batcher<>(createBatcherOptions(), this::handleBatch);
    }
    
    protected AbstractTopic(Builder<T> builder) {
        this.providerId = builder.providerId;
        this.topicName = builder.topicName;
        this.region = builder.region;
        this.endpoint = builder.endpoint;
        this.proxyEndpoint = builder.proxyEndpoint;
        this.credentialsOverrider = builder.credentialsOverrider;
        this.batcher = new Batcher<>(createBatcherOptions(), this::handleBatch);
    }
    
    public String getProviderId() {
        return providerId;
    }

    /**
     * Creates batcher options for this topic. Provides defaults that
     * cloud providers can override to provide provider-specific batching configuration
     * that aligns with their service limits and performance characteristics.
     */
    protected Batcher.Options createBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(1)                    // Maximum concurrent handlers
            .setMinBatchSize(1)                   // Minimum batch size
            .setMaxBatchSize(0)                   // No limit
            .setMaxBatchByteSize(0);              // No limit
    }
    
    /**
     * Handler method called by Batcher when a batch is ready to be processed.
     */
    private Void handleBatch(List<Message> messages) {
        executeBeforeSendBatchHooks(messages);
        try {
            doSendBatch(messages);
        } finally {
            executeAfterSendBatchHooks(messages);
        }
        return null;
    }
    
    /**
     * Sends a single message synchronously with error handling.
     * This method blocks until the message is successfully sent or an error occurs.
     */
    public final void send(Message message) {
        MessageUtils.validateMessage(message);
        sendBatch(List.of(message));
    }
    
    /**
     * Sends a batch of messages synchronously with full validation and error handling.
     * This method blocks until all messages are successfully sent or an error occurs.
     */
    private final void sendBatch(List<Message> messages) {
        MessageUtils.validateMessageBatch(messages);
        if (messages.isEmpty()) {
            return;
        }
        
        try {
            // Add all messages to batcher and wait for completion to ensure synchronous behavior
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Message message : messages) {
                futures.add(batcher.addNoWait(message));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() != null) {
                throw new SubstrateSdkException("Error sending batch of messages", e.getCause());
            }
            throw new SubstrateSdkException("Error sending batch of messages", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted while waiting for pubsub batch send to complete", e);
        }
    }
    
    /**
     * Provider-specific implementation for sending a batch of messages synchronously.
     */
    protected abstract void doSendBatch(List<Message> messages);
    
    /**
     * Gets the SubstrateSdkException class for a given throwable.
     * Used for exception type mapping.
     */
    public abstract Class<? extends SubstrateSdkException> getException(Throwable t);
    
    /**
     * Called before sending a batch of messages. Override for custom pre-send logic.
     */
    protected void executeBeforeSendBatchHooks(List<Message> messages) {
        // Default implementation does nothing
    }
    
    /**
     * Called after successfully sending a batch of messages. Override for custom post-send logic.
     */
    protected void executeAfterSendBatchHooks(List<Message> messages) {
        // Default implementation does nothing
    }
    
    /**
     * Implements AutoCloseable interface for try-with-resources support.
     * 
     * This method should:
     * - Flush any pending outbound messages
     * - Close network connections
     * - Stop background threads
     * - Release any other resources
     * 
     * After calling this method, the topic should not accept new messages.
     * It's safe to call this method multiple times.
     */
    @Override
    public void close() throws Exception {
        if (batcher != null) {
            batcher.shutdownAndDrain();
        }
    }
    
    public abstract static class Builder<T extends AbstractTopic<T>> {
        protected String providerId;
        protected String topicName;
        protected String region;
        protected URI endpoint;
        protected URI proxyEndpoint;
        protected CredentialsOverrider credentialsOverrider;
                
        public Builder<T> withTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }
        
        public Builder<T> withRegion(String region) {
            this.region = region;
            return this;
        }
        
        public Builder<T> withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            return this;
        }
        
        public Builder<T> withProxyEndpoint(URI proxyEndpoint) {
            this.proxyEndpoint = proxyEndpoint;
            return this;
        }
        
        public Builder<T> withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.credentialsOverrider = credentialsOverrider;
            return this;
        }

        public abstract T build();
    }
} 