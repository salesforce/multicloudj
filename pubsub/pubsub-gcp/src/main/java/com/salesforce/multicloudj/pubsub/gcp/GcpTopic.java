package com.salesforce.multicloudj.pubsub.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.core.ApiFuture;
import com.google.api.gax.httpjson.InstantiatingHttpJsonChannelProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.auto.service.AutoService;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.blob.gcp.GcpCredentialsProvider;
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

@SuppressWarnings("rawtypes")
@AutoService(AbstractTopic.class)
public class GcpTopic extends AbstractTopic<GcpTopic> {

    private volatile Publisher publisher;
    
    public GcpTopic() {
        this(new Builder());
    }
    
    public GcpTopic(Builder builder) {
        super(builder);
        validateTopicName(topicName);
        // Use injected Publisher if provided (for testing)
        if (builder.publisher != null) {
            this.publisher = builder.publisher;
        }
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

        Publisher pub = getOrCreatePublisher();
        if (pub == null) {
            throw new SubstrateSdkException("Failed to initialize GCP Publisher");
        }

        List<ApiFuture<String>> futures = messages.stream()
            .map(message -> {
                PubsubMessage.Builder pubsubMessageBuilder = PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(message.getBody()));
                Map<String, String> metadata = message.getMetadata();
                if (metadata != null && !metadata.isEmpty()) {
                    pubsubMessageBuilder.putAllAttributes(metadata);
                }
                return pubsubMessageBuilder.build();
            })
            .map(pub::publish)
            .collect(Collectors.toList());

        try {
            for (ApiFuture<String> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted while waiting for messages to publish.", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() != null) {
                throw new SubstrateSdkException("One or more messages failed to publish.", e.getCause());
            } else {
                throw new SubstrateSdkException("One or more messages failed to publish.", e);
            }
        }
    }

    /** Closes the GCP topic and releases all resources.
    *
    * This method shuts down any GCP resources, then calls the
    * parent close method to handle common cleanup.
    */
    @Override
    public void close() throws Exception {
        super.close();
        if (publisher != null) {
            publisher.shutdown();
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
     * Initializes the GCP PublisherStub with built-in retries for direct batch publishing.
     */
    private synchronized Publisher getOrCreatePublisher() {
        if (publisher == null) {
            try {
                TopicName topicNameObj = TopicName.parse(this.topicName);
                Publisher.Builder publisherBuilder = Publisher.newBuilder(topicNameObj);

                if (credentialsOverrider != null) {
                    Credentials credentials = GcpCredentialsProvider.getCredentials(credentialsOverrider);
                    if (credentials != null) {
                        publisherBuilder.setCredentialsProvider(() -> credentials);
                    }
                }

                if (this.endpoint != null) {
                    publisherBuilder.setEndpoint(this.endpoint.toString());
                }

                if (this.proxyEndpoint != null) {
                    HttpTransport httpTransport = new NetHttpTransport.Builder()
                        .setProxy(new Proxy(Proxy.Type.HTTP,
                            new InetSocketAddress(this.proxyEndpoint.getHost(), this.proxyEndpoint.getPort())))
                        .build();
                    TransportChannelProvider channelProvider = InstantiatingHttpJsonChannelProvider.newBuilder()
                        .setHttpTransport(httpTransport)
                        .build();
                    publisherBuilder.setChannelProvider(channelProvider);
                }

                publisher = publisherBuilder.build();

            } catch (IOException e) {
                throw new SubstrateSdkException("Failed to create GCP Publisher for topic: " + this.topicName, e);
            }
        }
        return publisher;
    }

    /**
     * Builder for creating GcpTopic instances.
     */
    public static class Builder extends AbstractTopic.Builder<GcpTopic> {
        
        private Publisher publisher; // For dependency injection in tests

        public Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

        public Builder providerId(String providerId) {
            this.providerId = providerId;
            return this;
        }
        
        /**
         * Inject a Publisher for testing purposes.
         * @param publisher The Publisher to use instead of creating one
         * @return this builder
         */
        public Builder withPublisher(Publisher publisher) {
            this.publisher = publisher;
            return this;
        }

        @Override
        public GcpTopic build() {
            return new GcpTopic(this);
        }
    }
} 