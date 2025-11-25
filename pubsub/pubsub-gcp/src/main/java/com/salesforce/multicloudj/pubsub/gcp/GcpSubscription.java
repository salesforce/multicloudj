package com.salesforce.multicloudj.pubsub.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.httpjson.InstantiatingHttpJsonChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.google.pubsub.v1.Subscription;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;

import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;
import com.google.auto.service.AutoService;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("rawtypes")
@AutoService(AbstractSubscription.class)
public class GcpSubscription extends AbstractSubscription<GcpSubscription> {

    private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    private Batcher.Options receiveBatcherOptions;
    private volatile SubscriptionAdminClient subscriptionAdminClient;
    private final boolean nackLazy;

    public GcpSubscription() {
        this(new Builder());
    }
    
    public GcpSubscription(Builder builder) {
        super(builder);
        this.nackLazy = builder.nackLazy;
    }
    
    public GcpSubscription(Builder builder, SubscriptionAdminClient subscriptionAdminClient) {
        super(builder);
        this.nackLazy = builder.nackLazy;
        this.subscriptionAdminClient = subscriptionAdminClient;
    }

    /**
     * Gets or creates the SubscriptionAdminClient lazily.
     * @return the SubscriptionAdminClient instance
     * @throws SubstrateSdkException if client creation fails
     */
    private SubscriptionAdminClient getOrCreateSubscriptionAdminClient() {
        if (subscriptionAdminClient == null) {
            synchronized (this) {
                if (subscriptionAdminClient == null) {
            try {
                        SubscriptionAdminSettings.Builder settingsBuilder = SubscriptionAdminSettings.newHttpJsonBuilder();
                
                        // Configure credentials if available
                if (credentialsOverrider != null) {
                    Credentials credentials = GcpCredentialsProvider.getCredentials(credentialsOverrider);
                    if (credentials != null) {
                                settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
                    }
                }
                
                        // Configure endpoint if specified
                if (this.endpoint != null) {
                            settingsBuilder.setEndpoint(this.endpoint.toString());
                }
                
                        // Configure proxy if specified
                        InstantiatingHttpJsonChannelProvider.Builder httpJson = InstantiatingHttpJsonChannelProvider.newBuilder();
                if (this.proxyEndpoint != null) {
                    HttpTransport httpTransport = new NetHttpTransport.Builder()
                        .setProxy(new Proxy(Proxy.Type.HTTP,
                            new InetSocketAddress(this.proxyEndpoint.getHost(), this.proxyEndpoint.getPort())))
                        .build();
                            httpJson.setHttpTransport(httpTransport);
                }
                        TransportChannelProvider channelProvider = httpJson.build();
                        settingsBuilder.setTransportChannelProvider(channelProvider);
                
                        subscriptionAdminClient = SubscriptionAdminClient.create(settingsBuilder.build());
                    } catch (IOException e) {
                        throw new SubstrateSdkException("Failed to create subscription admin client", e);
            }
        }
            }
        }
        return subscriptionAdminClient;
    }

    @Override
    protected void doSendAcks(List<AckID> ackIDs) {
            List<String> ackIds = new ArrayList<>();
            for (AckID ackID : ackIDs) {
                ackIds.add(ackID.toString());
            }
            
            AcknowledgeRequest request = AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAllAckIds(ackIds)
                .build();
            
        getOrCreateSubscriptionAdminClient().acknowledgeCallable().call(request);
    }
    
    @Override
    protected void doSendNacks(List<AckID> ackIDs) {
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
            
        getOrCreateSubscriptionAdminClient().modifyAckDeadlineCallable().call(request);
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
        if (error instanceof ApiException) {
            ApiException apiException = (ApiException) error;
            StatusCode.Code code = apiException.getStatusCode().getCode();
            return code == StatusCode.Code.DEADLINE_EXCEEDED;
        }

        return false;
    }

    @Override
    public GetAttributeResult getAttributes() {
        try {
            Subscription sub = getOrCreateSubscriptionAdminClient().getSubscription(subscriptionName);

            return new GetAttributeResult.Builder()
                    .name(sub.getName())
                    .topic(sub.getTopic())
                    .build();
        } catch (ApiException e) {
            throw new SubstrateSdkException("Failed to retrieve subscription attributes", e);
        }
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
    protected Batcher.Options createAckBatcherOptions() {
        return new Batcher.Options()
                .setMaxHandlers(2)
                .setMinBatchSize(1)
                .setMaxBatchSize(1000)
                .setMaxBatchByteSize(0);
            }
            
    @Override
    protected List<Message> doReceiveBatch(int batchSize) {
        PullRequest req = PullRequest.newBuilder()
                .setSubscription(subscriptionName)
                .setMaxMessages(Math.max(1, batchSize))
                .setReturnImmediately(true)
                .build();
            
        PullResponse resp = getOrCreateSubscriptionAdminClient().pullCallable().call(req);
                    
        List<Message> receivedMessages = new ArrayList<>();
        for (ReceivedMessage rm : resp.getReceivedMessagesList()) {
            Message m = convertToMessage(rm);
            receivedMessages.add(m);
        }
        return receivedMessages;
    }

    /**
     * Closes the GCP subscription and releases all resources.
     */
    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            if (subscriptionAdminClient != null) {
                subscriptionAdminClient.close();
                subscriptionAdminClient = null;
        }
        }
    }

    /**
     * Validates that the subscription name is in the correct GCP Pub/Sub format.
     * Expected format: "projects/{project-id}/subscriptions/{subscription-id}"
     * 
     * @param subscriptionName the subscription name to validate
     * @throws InvalidArgumentException if the subscription name is invalid
     */
    static void validateSubscriptionName(String subscriptionName) {
        if (subscriptionName == null || subscriptionName.trim().isEmpty()) {
            throw new InvalidArgumentException("Subscription name cannot be null or empty");
        }
        if (!subscriptionName.matches("projects/[^/]+/subscriptions/[^/]+")) {
            throw new InvalidArgumentException(
                    "Subscription name must be in format: projects/{projectId}/subscriptions/{subscriptionId}, got: " + subscriptionName);
        }
    }

    Message convertToMessage(ReceivedMessage receivedMessage) {
        PubsubMessage pubsubMessage = receivedMessage.getMessage();
        String ackId = receivedMessage.getAckId();
        
        AckID ackID = new GcpAckID(ackId);
        
        return Message.builder()
                .withBody(pubsubMessage.getData().toByteArray())
                .withMetadata(pubsubMessage.getAttributesMap())
                .withAckID(ackID)
                .withLoggableID(pubsubMessage.getMessageId())
                .build();
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

    @Override
    public Builder builder() {
        return new Builder();
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
        
        @Override
        public GcpSubscription.Builder withReceiveTimeoutSeconds(long receiveTimeoutSeconds) {
            super.withReceiveTimeoutSeconds(receiveTimeoutSeconds);
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
            validateSubscriptionName(this.subscriptionName);
            return new GcpSubscription(this);
        }
    }
}