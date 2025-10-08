package com.salesforce.multicloudj.pubsub.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.httpjson.InstantiatingHttpJsonChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.salesforce.multicloudj.blob.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class GcpSubscription extends AbstractSubscription<GcpSubscription> {

    private volatile Subscriber subscriber;
    private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    private Batcher.Options receiveBatcherOptions;
    private volatile SubscriberStub subscriberStub;
    private final boolean nackLazy;

    private static final RetrySettings RETRY_SETTINGS = RetrySettings.newBuilder()
            .setMaxAttempts(0)                               // Unlimited attempts, controlled by timeout
            .setInitialRetryDelay(Duration.ofMillis(100))
            .setRetryDelayMultiplier(1.3)
            .setMaxRetryDelay(Duration.ofSeconds(60))
            .setTotalTimeout(Duration.ofMinutes(5))          // context timeout
            .build();
    
    private static final int DEFAULT_HTTPS_PORT = 443;
    private static final int DEFAULT_HTTP_PORT = 80;
    
    public GcpSubscription() {
        this(new Builder());
    }
    
    public GcpSubscription(Builder builder) {
        super(builder);
        this.nackLazy = builder.nackLazy;
        validateSubscriptionName(this.subscriptionName);
    }
    
    /**
     * Gets or creates the SubscriberStub lazily.
     * @return the SubscriberStub instance
     * @throws SubstrateSdkException if stub creation fails
     */
    private SubscriberStub getOrCreateSubscriberStub() {
        if (subscriberStub == null) {
            synchronized (this) {
                if (subscriberStub == null) {
                    try {
                        subscriberStub = createStubWithRetry();
                    } catch (IOException e) {
                        throw new SubstrateSdkException("Failed to create subscriber stub", e);
                    }
                }
            }
        }
        return subscriberStub;
    }
    
    /**
     * Creates a SubscriberStub configured with GAX retry settings.      
     * @return a configured SubscriberStub with retry settings applied
     * @throws IOException if stub creation fails
     */
    private SubscriberStub createStubWithRetry() throws IOException {
        SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();
        
        // This applies our RETRY_SETTINGS to all acknowledge calls made through this stub
        builder.acknowledgeSettings()
            .setRetrySettings(RETRY_SETTINGS);
            
        builder.modifyAckDeadlineSettings()
            .setRetrySettings(RETRY_SETTINGS);
            
        // Use same credentials as Subscriber 
        if (credentialsOverrider != null) {
            Credentials credentials = GcpCredentialsProvider.getCredentials(credentialsOverrider);
            if (credentials != null) {
                builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            }
        }
        
        // Use same endpoint and proxy configuration as Subscriber 
        if (this.endpoint != null) {
            // Extract host and port from URI for GAX compatibility
            String host = this.endpoint.getHost();
            int port = this.endpoint.getPort();
            if (port == -1) {
                // Use default port based on scheme
                port = "https".equals(this.endpoint.getScheme()) ? DEFAULT_HTTPS_PORT : DEFAULT_HTTP_PORT;
            }
            builder.setEndpoint(host + ":" + port);
        }
        
        // SubscriberStubSettings doesn't support channel provider configuration
        // Proxy configuration is handled at the Subscriber level only
        
        return GrpcSubscriberStub.create(builder.build());
    }

    /**
     * Initializes the GCP Subscriber with built-in retries for message consumption.
     */
    private synchronized Subscriber getOrCreateSubscriber() {
        if (subscriber == null) {
            try {
                // Validate and parse the subscription name
                validateSubscriptionName(subscriptionName);
                ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.parse(subscriptionName);
                
                MessageReceiver receiver = (message, consumer) -> {
                    Message convertedMessage = convertToMessage(message, consumer);
                    messageQueue.offer(convertedMessage);
                };
                
                Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(projectSubscriptionName, receiver);
                
                if (credentialsOverrider != null) {
                    Credentials credentials = GcpCredentialsProvider.getCredentials(credentialsOverrider);
                    if (credentials != null) {
                        subscriberBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
                    }
                }
                
                if (this.endpoint != null) {
                    subscriberBuilder.setEndpoint(this.endpoint.toString());
                }
                
                if (this.proxyEndpoint != null) {
                    HttpTransport httpTransport = new NetHttpTransport.Builder()
                        .setProxy(new Proxy(Proxy.Type.HTTP,
                            new InetSocketAddress(this.proxyEndpoint.getHost(), this.proxyEndpoint.getPort())))
                        .build();
                    TransportChannelProvider channelProvider = InstantiatingHttpJsonChannelProvider.newBuilder()
                        .setHttpTransport(httpTransport)
                        .build();
                    subscriberBuilder.setChannelProvider(channelProvider);
                }
                
                subscriber = subscriberBuilder.build();
                subscriber.startAsync();
                
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException("Invalid subscription name or configuration: " + subscriptionName, e);
            } catch (RuntimeException e) {
                throw new SubstrateSdkException("Failed to create GCP Subscriber for subscription: " + subscriptionName, e);
            }
        }
        return subscriber;
    }

    @Override
    protected void doSendAcks(List<AckID> ackIDs) {
        if (isShutdown.get()) {
            throw new IllegalStateException("Subscription is closed, cannot send ack");
        }
        
        try {
            List<String> ackIds = new ArrayList<>();
            for (AckID ackID : ackIDs) {
                ackIds.add(ackID.toString());
            }
            
            AcknowledgeRequest request = AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAllAckIds(ackIds)
                .build();
            
            getOrCreateSubscriberStub().acknowledgeCallable().call(request);
            
        } catch (Exception e) {
            if (!isRetryable(e)) {
                permanentError.set(e);
            }
            unreportedAckErr.set(e);
            throw new SubstrateSdkException("Failed to send acks", e);
        }
    }
    
    @Override
    protected void doSendNacks(List<AckID> ackIDs) {
        if (isShutdown.get()) {
            throw new IllegalStateException("Subscription is closed, cannot send nack");
        }
        
        try {
            // NackLazy mode: bypass ModifyAckDeadline call
            // Messages will be redelivered after existing ack deadline expires
            if (nackLazy) {
                return;
            }
            
            // Normal mode: immediate redelivery by setting deadline to 0
            List<String> ackIds = new ArrayList<>();
            for (AckID ackID : ackIDs) {
                ackIds.add(ackID.toString());
            }
            
            if (ackIds.isEmpty()) {
                return;
            }
            
            ModifyAckDeadlineRequest request = ModifyAckDeadlineRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAllAckIds(ackIds)
                .setAckDeadlineSeconds(0) // 0 means nack (immediate redelivery)
                .build();
            
            getOrCreateSubscriberStub().modifyAckDeadlineCallable().call(request);
            
        } catch (Exception e) {
            if (!isRetryable(e)) {
                permanentError.set(e);
            }
            unreportedAckErr.set(e);
            throw new SubstrateSdkException("Failed to send nacks", e);
        }
    }
    
    @Override
    protected String getMessageId(AckID ackID) {
        return ackID != null ? ackID.toString() : null;
    }
    
    @Override
    protected void validateAckIDType(AckID ackID) {
        // For GCP, we validate that the AckID is not null and is a GcpAckID
        if (ackID == null) {
            throw new InvalidArgumentException("AckID cannot be null");
        }
        
        if (!(ackID instanceof GcpAckID)) {
            throw new InvalidArgumentException("Expected GcpAckID, got: " + ackID.getClass().getSimpleName());
        }
    }
    

    @Override
    public boolean canNack() {
        return true;
    }

    @Override
    public boolean isRetryable(Throwable error) {
        if (error == null) {
            return false;
        }

        Throwable t = error;
        if (t instanceof java.util.concurrent.ExecutionException && t.getCause() != null) {
            t = t.getCause();
        }

        if (t instanceof ApiException) {
            com.google.api.gax.rpc.StatusCode.Code code = ((ApiException) t).getStatusCode().getCode();
            // Same as Go CDK, only retry DeadlineExceeded errors
            return code == com.google.api.gax.rpc.StatusCode.Code.DEADLINE_EXCEEDED;
        }
        return false;
    }

    @Override
    public Map<String, String> getAttributes() {
        // TODO: Implement subscription attributes retrieval
        return Collections.emptyMap();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof ApiException) {
            ApiException apiException = (ApiException) t;
            return CommonErrorCodeMapping.getException(apiException.getStatusCode().getCode());
        }

        // For any other exceptions, return UnknownException as the fallback
        return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
    }

    @Override
    protected Batcher.Options createReceiveBatcherOptions() {
        if (receiveBatcherOptions == null) {
            this.receiveBatcherOptions = new Batcher.Options()
                .setMaxHandlers(10)
                .setMinBatchSize(1)
                .setMaxBatchSize(1000)   // GCP Pub/Sub returns at most 1000 messages per RPC
                .setMaxBatchByteSize(0); // No limit
        }
        return receiveBatcherOptions;
    }
    
    @Override
    protected List<Message> doReceiveBatch(int batchSize) {
        try {
            Subscriber sub = getOrCreateSubscriber();
            if (sub == null) {
                throw new SubstrateSdkException("Failed to initialize GCP Subscriber");
            }
            
            List<Message> messages = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                // Use 0 timeout to make it behave like synchronous calls
                Message message = messageQueue.poll(0, TimeUnit.SECONDS);
                if (message == null) {
                    break; 
                }
                messages.add(message);
            }
            
            return messages;
                    
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /**
     * Closes the GCP subscription and releases all resources.
     * 
     * This method shuts down any GCP resources, then calls the
     * parent close method to handle common cleanup.
     */
    @Override
    public void close() throws Exception {
        if (subscriber != null) {
            subscriber.stopAsync().awaitTerminated();
            subscriber = null;
        }
        
        if (subscriberStub != null) {
            subscriberStub.close();
            subscriberStub = null;
        }
        
        super.close();
    }

    /**
     * Validates that the subscription name is in the correct GCP Pub/Sub format.
     * Expected format: "projects/{project-id}/subscriptions/{subscription-id}"
     * 
     * @param subscriptionName the subscription name to validate
     * @throws InvalidArgumentException if the subscription name is invalid
     */
    private void validateSubscriptionName(String subscriptionName) {
        if (subscriptionName == null || subscriptionName.trim().isEmpty()) {
            throw new InvalidArgumentException("Subscription name cannot be null or empty");
        }
        if (!subscriptionName.matches("projects/[^/]+/subscriptions/[^/]+")) {
            throw new InvalidArgumentException(
                    "Subscription name must be in format: projects/{projectId}/subscriptions/{subscriptionId}, got: " + subscriptionName);
        }
    }

    Message convertToMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
        String ackId = extractAckId(ackReplyConsumer, pubsubMessage.getMessageId());
        
        AckID ackID = new GcpAckID(ackId);
        
        return Message.builder()
                .withBody(pubsubMessage.getData().toByteArray())
                .withMetadata(pubsubMessage.getAttributesMap())
                .withAckID(ackID)
                .withLoggableID(pubsubMessage.getMessageId())
                .build();
    }
    
    /**
     * Extracts the ackId from AckReplyConsumer using reflection.
     * This is necessary because GCP Pub/Sub Java client doesn't expose ackId directly.
     */
    private String extractAckId(AckReplyConsumer ackReplyConsumer, String messageId) {
        try {
            java.lang.reflect.Field ackIdField = ackReplyConsumer.getClass().getDeclaredField("ackId");
            ackIdField.setAccessible(true);
            return (String) ackIdField.get(ackReplyConsumer);
        } catch (Exception e) {
            return messageId;
        }
    }
    
    /**
     * GCP-specific implementation of AckID that wraps the acknowledgment ID string.
     */
    public static class GcpAckID implements AckID {
        private final String ackId;
        
        public GcpAckID(String ackId) {
            if (ackId == null || ackId.trim().isEmpty()) {
                throw new IllegalArgumentException("AckID string cannot be null or empty");
            }
            this.ackId = ackId;
        }
        
        @Override
        public String toString() {
            return ackId;
        }
    }

    
    public static class Builder extends AbstractSubscription.Builder<GcpSubscription> {
        private boolean nackLazy = false;
        
        public Builder() {
            this.providerId = GcpConstants.PROVIDER_ID;
        }
        
        @Override
        public GcpSubscription.Builder withSubscriptionName(String subscriptionName) {
            super.withSubscriptionName(subscriptionName);
            return this;
        }
        
        @Override
        public GcpSubscription.Builder withRegion(String region) {
            super.withRegion(region);
            return this;
        }
        
        @Override
        public GcpSubscription.Builder withEndpoint(URI endpoint) {
            super.withEndpoint(endpoint);
            return this;
        }
        
        @Override
        public GcpSubscription.Builder withProxyEndpoint(URI proxyEndpoint) {
            super.withProxyEndpoint(proxyEndpoint);
            return this;
        }
        
        @Override
        public GcpSubscription.Builder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            super.withCredentialsOverrider(credentialsOverrider);
            return this;
        }
        
        /**
         * Sets the nackLazy mode for negative acknowledgments.
         *
         * By default (false), Nack uses ModifyAckDeadline to set the ack deadline
         * for the nacked message to 0, so that it will be redelivered immediately.
         *
         * Set to true to bypass this behavior; Nack will do nothing, and the message
         * will be redelivered after the existing ack deadline expires.
         *
         * This is useful when you don't want immediate retry but prefer to wait for
         * the natural timeout before reprocessing the message.
         * 
         * @param nackLazy true to enable lazy NACK mode, false for immediate redelivery
         * @return this builder for method chaining
         */
        public GcpSubscription.Builder withNackLazy(boolean nackLazy) {
            this.nackLazy = nackLazy;
            return this;
        }
        
        @Override
        public GcpSubscription build() {
            return new GcpSubscription(this);
        }
    }

}