package com.salesforce.multicloudj.pubsub.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.httpjson.InstantiatingHttpJsonChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;

@SuppressWarnings("rawtypes")
@AutoService(AbstractTopic.class)
public class GcpTopic extends AbstractTopic<GcpTopic> {

    private volatile TopicAdminClient topicAdminClient;
    public GcpTopic() {
        this(new Builder());
    }
    public GcpTopic(Builder builder) {
        super(builder);
        validateTopicName(topicName);
    }
    
    public GcpTopic(Builder builder, TopicAdminClient topicAdminClient) {
        super(builder);
        validateTopicName(topicName);
        this.topicAdminClient = topicAdminClient;
    }
    /**
     * Override batcher options to align with GCP Pub/Sub limits.
     * GCP supports up to 1000 messages or 10MB per batch.
     */
    @Override
    protected Batcher.Options createBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(2)
            .setMinBatchSize(1)
            .setMaxBatchSize(1000)
            .setMaxBatchByteSize(9 * 1024 * 1024);
    }

    @Override
    protected void doSendBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        TopicAdminClient topicAdminClient = getOrCreateTopicAdminClient();
        if (topicAdminClient == null) {
            throw new SubstrateSdkException("Failed to initialize Pub/Sub TopicAdminClient");
        }

        // Build PubsubMessage list
        List<PubsubMessage> pubsubMessages = messages.stream()
                .map(m -> {
                    PubsubMessage.Builder b = PubsubMessage.newBuilder()
                            .setData(ByteString.copyFrom(m.getBody()));
                    Map<String, String> metadata = m.getMetadata();
                if (metadata != null && !metadata.isEmpty()) {
                        b.putAllAttributes(metadata);
                }
                    return b.build();
            })
            .collect(Collectors.toList());

        PublishRequest req = PublishRequest.newBuilder()
                .setTopic(this.topicName)
                .addAllMessages(pubsubMessages)
                .build();

        try {
            PublishResponse resp = topicAdminClient.publishCallable().futureCall(req).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted while waiting for messages to publish.", e);
        } catch (ExecutionException e) {
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            throw new SubstrateSdkException("Publish failed.", cause);
        }
    }

    /**
     * Closes the GCP topic and releases all resources.
    */
    @Override
    public void close() throws Exception {
        super.close();
        if (topicAdminClient != null) {
            topicAdminClient.close();
        }
    }

    /**
     * Creates a new builder for GcpTopic.
     */
    public static Builder builder() {
        return new Builder();
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

    /**
     * Called before sending a batch of messages to GCP Pub/Sub.
     */
    @Override
    protected void executeBeforeSendBatchHooks(List<Message> messages) {
    }

    /**
     * Called after successfully sending a batch of messages to GCP Pub/Sub.
     */
    @Override
    protected void executeAfterSendBatchHooks(List<Message> messages) {
    }

    /**
     * Validates that the topic name is in the correct GCP format.
     *
     * @param topicName The topic name to validate
     * @throws InvalidArgumentException if the topic name is invalid
     */
    private static void validateTopicName(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new InvalidArgumentException("Topic name cannot be null or empty");
        }

        // topicName is expected to be the full resource name: projects/{projectId}/topics/{topicId}
        if (!topicName.matches("projects/[^/]+/topics/[^/]+")) {
            throw new InvalidArgumentException(
                    "Topic name must be in format: projects/{projectId}/topics/{topicId}, got: " + topicName);
        }
    }

    /**
     * Initializes the GCP TopicAdminClient with built-in retries for direct batch publishing.
     */
    private synchronized TopicAdminClient getOrCreateTopicAdminClient() {
        if (topicAdminClient != null) return topicAdminClient;

            try {
            TopicAdminSettings.Builder settingsBuilder = TopicAdminSettings.newHttpJsonBuilder();

            if (this.endpoint != null) {
                settingsBuilder.setEndpoint(this.endpoint.toString());
            }

                if (credentialsOverrider != null) {
                    Credentials credentials = GcpCredentialsProvider.getCredentials(credentialsOverrider);
                    if (credentials != null) {
                    settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
                    }
                }

            InstantiatingHttpJsonChannelProvider.Builder httpJson = InstantiatingHttpJsonChannelProvider.newBuilder();
                if (this.proxyEndpoint != null) {
                    HttpTransport httpTransport = new NetHttpTransport.Builder()
                        .setProxy(new Proxy(
                                Proxy.Type.HTTP,
                                new InetSocketAddress(this.proxyEndpoint.getHost(), this.proxyEndpoint.getPort())
                        ))
                        .build();
                httpJson.setHttpTransport(httpTransport);
                }
            TransportChannelProvider channelProvider = httpJson.build();
            settingsBuilder.setTransportChannelProvider(channelProvider);

            topicAdminClient = TopicAdminClient.create(settingsBuilder.build());
            return topicAdminClient;

            } catch (IOException e) {
            throw new SubstrateSdkException(
                    "Failed to create topicAdminClient for topic: " + this.topicName, e);
            }
        }

    /**
     * Builder for creating GcpTopic instances.
     */
    public static class Builder extends AbstractTopic.Builder<GcpTopic> {
        
        public Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

        public Builder providerId(String providerId) {
            this.providerId = providerId;
            return this;
        }
        
        @Override
        public GcpTopic build() {
            return new GcpTopic(this);
        }
    }
} 