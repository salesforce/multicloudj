package com.salesforce.multicloudj.pubsub.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

/**
 * AWS SQS implementation of AbstractTopic.
 * Handles sending messages to SQS queues.
 */
@AutoService(AbstractTopic.class)
public class AwsSqsTopic extends AwsBaseTopic<AwsSqsTopic> {

    public static final String PROVIDER_ID = "awssqs";

    private static final String SQS_QUEUE_URL_PREFIX = "https://sqs.";

    private static final int MAX_SQS_ATTRIBUTES = 10;

    private final SqsClient sqsClient;
    private final String queueUrl;

    public AwsSqsTopic(Builder builder) {
        super(builder);
        this.sqsClient = builder.sqsClient;
        this.queueUrl = builder.queueUrl;
    }

    @Override
    public String getProviderId() {
        return PROVIDER_ID;
    }


    @Override
    protected void doSendBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        sendToSqs(messages);
    }

    /**
     * Send messages to SQS queue.
     */
    private void sendToSqs(List<Message> messages) {
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            
            // Encode metadata as message attributes
            Map<String, MessageAttributeValue> attributes = 
                convertMetadataToSqsAttributes(message.getMetadata());
            
            // Handle base64 encoding based on topic options
            BodyEncodingResult encodingResult = encodeMessageBody(message);
            String messageBody = encodingResult.getBody();
            if (encodingResult.isBase64Encoded()) {
                // Add base64 encoding flag
                attributes.put(MetadataKeys.BASE64_ENCODED, 
                    MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue("true")
                        .build());
            }
            
            SendMessageBatchRequestEntry.Builder entryBuilder = SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody(messageBody)
                .messageAttributes(attributes);
            
            reviseSqsEntryAttributes(message, entryBuilder);
            
            entries.add(entryBuilder.build());
        }

        SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
            .queueUrl(queueUrl)
            .entries(entries)
            .build();

        SendMessageBatchResponse batchResponse = sqsClient.sendMessageBatch(batchRequest);
        
        // Check for failed messages
        if (!batchResponse.failed().isEmpty()) {
            var firstFailure = batchResponse.failed().get(0);
            throw new SubstrateSdkException(
                String.format("SQS SendMessageBatch failed for %d message(s): %s, %s", 
                    batchResponse.failed().size(),
                    firstFailure.code(),
                    firstFailure.message()));
        }
    }

    /**
     * Sets SQS-specific attributes on a SendMessageBatchRequestEntry based on message metadata.
     */
    private void reviseSqsEntryAttributes(Message message, SendMessageBatchRequestEntry.Builder entryBuilder) {
        Map<String, String> metadata = message.getMetadata();
        if (metadata != null) {
            String dedupId = metadata.get(MetadataKeys.DEDUPLICATION_ID);
            if (dedupId != null) {
                entryBuilder.messageDeduplicationId(dedupId);
            }
            String groupId = metadata.get(MetadataKeys.MESSAGE_GROUP_ID);
            if (groupId != null) {
                entryBuilder.messageGroupId(groupId);
            }
        }
    }

    /**
     * Converts message metadata to SQS message attributes.
     */
    private Map<String, MessageAttributeValue> convertMetadataToSqsAttributes(Map<String, String> metadata) {
        Map<String, MessageAttributeValue> attributes = new java.util.HashMap<>();
        
        if (metadata != null && !metadata.isEmpty()) {
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                String key = entry.getKey();
                if (MetadataKeys.DEDUPLICATION_ID.equals(key) || MetadataKeys.MESSAGE_GROUP_ID.equals(key)) {
                    continue;
                }
                
                if (attributes.size() >= MAX_SQS_ATTRIBUTES) {
                    break;
                }
                
                String encodedKey = encodeMetadataKey(key);
                String encodedValue = encodeMetadataValue(entry.getValue());
                
                attributes.put(encodedKey, 
                    MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(encodedValue)
                        .build());
            }
        }
        return attributes;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (sqsClient != null) {
            sqsClient.close();
        }
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    /**
     * Gets the queue URL from the queue name.
     */
    static String getQueueUrl(String queueName, SqsClient sqsClient) 
            throws software.amazon.awssdk.awscore.exception.AwsServiceException, SdkClientException {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
            .queueName(queueName)
            .build();
        
        GetQueueUrlResponse response = sqsClient.getQueueUrl(request);
        return response.queueUrl();
    }

    public static class Builder extends AwsBaseTopic.Builder<Builder, AwsSqsTopic> {
        private SqsClient sqsClient;
        private String queueUrl;
        
        public Builder() {
            this.providerId = PROVIDER_ID;
        }

        @Override
        protected Builder self() {
            return this;
        }
        
        /**
         * Sets the topic name or queue URL.
         */
        @Override
        public Builder withTopicName(String topicName) {
            super.withTopicName(topicName);
            
            // Auto-detect URL format
            if (topicName != null && topicName.startsWith(SQS_QUEUE_URL_PREFIX)) {
                // SQS queue URL, in case user provides full URL
                // Will be validated for existence in build() method
                this.queueUrl = topicName;
            }
            // Otherwise, treat as queue name (will be resolved to URL via GetQueueUrl)
            
            return this;
        }
        
        public Builder withSqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }
        
        /**
         * Directly set the topic URL to avoid calling GetQueueUrl again.
         * Used when the queue URL has already been resolved.
         */
        Builder withTopicUrl(String topicUrl) {
            this.queueUrl = topicUrl;
            return this;
        }
        
        private static SqsClient buildSqsClient(Builder builder) {
            return SqsClientUtil.buildSqsClient(
                builder.region,
                builder.endpoint,
                builder.credentialsOverrider);
        }
        
        @Override
        public AwsSqsTopic build() {
            // Basic null/empty check for topicName
            if (this.topicName == null || this.topicName.trim().isEmpty()) {
                throw new InvalidArgumentException("Topic name/ARN cannot be null or empty");
            }
            
            if (sqsClient == null) {
                sqsClient = buildSqsClient(this);
            }
            
            // get the full queue URL from the queue name 
            if (this.queueUrl == null) {
                // If queueUrl is not set, resolve it from topicName via GetQueueUrl
                // This will validate that the queue exists and is accessible
                this.queueUrl = getQueueUrl(this.topicName, sqsClient);
            } else {
                // If user provided a URL, validate that the queue actually exists
                validateQueueUrlExists(this.queueUrl, sqsClient);
            }
            
            return new AwsSqsTopic(this);
        }
        
        /**
         * Validates that the provided queue URL points to an existing queue.
         * Throws an exception if the queue does not exist or is inaccessible.
         */
        private static void validateQueueUrlExists(String queueUrl, SqsClient sqsClient) {
            // Use GetQueueAttributes to verify the queue exists
            // All exceptions will be handled by getException() which maps them to appropriate SubstrateSdkException types
            GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN) 
                .build();
            sqsClient.getQueueAttributes(request);
        }
    }
}
