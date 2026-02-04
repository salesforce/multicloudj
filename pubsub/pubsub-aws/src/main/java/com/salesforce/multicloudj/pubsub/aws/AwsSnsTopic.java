package com.salesforce.multicloudj.pubsub.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesRequest;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;

/**
 * AWS SNS implementation of AbstractTopic.
 * Handles sending messages to SNS topics.
 */
@AutoService(AbstractTopic.class)
public class AwsSnsTopic extends AwsBaseTopic<AwsSnsTopic> {

    public static final String PROVIDER_ID = "awssns";

    private static final int MAX_SNS_ATTRIBUTES = 10;

    private final SnsClient snsClient;
    private final String topicArn;

    public AwsSnsTopic(Builder builder) {
        super(builder);
        this.snsClient = builder.snsClient;
        this.topicArn = builder.topicArn;
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
        sendToSns(messages);
    }

    /**
     * Send messages to SNS topic.
     */
    private void sendToSns(List<Message> messages) {
        List<PublishBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            
            // Encode metadata as message attributes
            Map<String, MessageAttributeValue> attributes = 
                convertMetadataToSnsAttributes(message.getMetadata());
            
            // Handle base64 encoding based on topic options
            EncodingResult encodingResult = encodeMessageBody(message);
            String messageBody = encodingResult.getBody();
            if (encodingResult.isBase64Encoded()) {
                // Add base64 encoding flag
                attributes.put(MetadataKeys.BASE64_ENCODED,
                    MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue("true")
                        .build());
            }
            
            PublishBatchRequestEntry.Builder entryBuilder = PublishBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .message(messageBody)
                .messageAttributes(attributes);
            
            // Set SNS-specific attributes directly from metadata
            reviseSnsEntryAttributes(message, entryBuilder);
            
            entries.add(entryBuilder.build());
        }

        PublishBatchRequest batchRequest = PublishBatchRequest.builder()
            .topicArn(topicArn)
            .publishBatchRequestEntries(entries)
            .build();

        PublishBatchResponse batchResponse = snsClient.publishBatch(batchRequest);
        
        // Check for failed messages
        if (!batchResponse.failed().isEmpty()) {
            var firstFailure = batchResponse.failed().get(0);
            throw new SubstrateSdkException(
                String.format("SNS PublishBatch failed for %d message(s): %s, %s", 
                    batchResponse.failed().size(),
                    firstFailure.code(),
                    firstFailure.message()));
        }
    }

    /**
     * Sets attributes on a PublishBatchRequestEntry based on message metadata.
     */
    private void reviseSnsEntryAttributes(Message message, PublishBatchRequestEntry.Builder entryBuilder) {
        Map<String, String> metadata = message.getMetadata();
        if (metadata != null) {
            // Set subject if provided
            String subject = metadata.get(MetadataKeys.SUBJECT);
            if (subject != null) {
                entryBuilder.subject(subject);
            }
            
            // Set MessageDeduplicationId for FIFO topics
            String dedupId = metadata.get(MetadataKeys.DEDUPLICATION_ID);
            if (dedupId != null) {
                entryBuilder.messageDeduplicationId(dedupId);
            }
            
            // Set MessageGroupId for FIFO topics
            String groupId = metadata.get(MetadataKeys.MESSAGE_GROUP_ID);
            if (groupId != null) {
                entryBuilder.messageGroupId(groupId);
            }
        }
    }

    /**
     * Converts message metadata to SNS message attributes.
     */
    private Map<String, MessageAttributeValue> convertMetadataToSnsAttributes(Map<String, String> metadata) {
        Map<String, MessageAttributeValue> attributes = new java.util.HashMap<>();
        
        if (metadata != null && !metadata.isEmpty()) {
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                String key = entry.getKey();
                // Skip keys that are handled as direct SNS message properties
                if (MetadataKeys.SUBJECT.equals(key) || 
                    MetadataKeys.DEDUPLICATION_ID.equals(key) || 
                    MetadataKeys.MESSAGE_GROUP_ID.equals(key)) {
                    continue;
                }
                
                if (attributes.size() >= MAX_SNS_ATTRIBUTES) {
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
        if (snsClient != null) {
            snsClient.close();
        }
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    public static class Builder extends AwsBaseTopic.Builder<Builder, AwsSnsTopic> {
        private SnsClient snsClient;
        private String topicArn;
        
        public Builder() {
            this.providerId = PROVIDER_ID;
        }

        @Override
        protected Builder self() {
            return this;
        }
        
        @Override
        public Builder withTopicName(String topicName) {
            super.withTopicName(topicName);
            this.topicArn = topicName;
            return this;
        }
        
        public Builder withSnsClient(SnsClient snsClient) {
            this.snsClient = snsClient;
            return this;
        }
        
        private static SnsClient buildSnsClient(Builder builder) {
            return SnsClientUtil.buildSnsClient(
                builder.region,
                builder.endpoint,
                builder.credentialsOverrider);
        }
        
        @Override
        public AwsSnsTopic build() {
            if (this.topicArn == null || this.topicArn.trim().isEmpty()) {
                throw new InvalidArgumentException("Topic ARN cannot be null or empty");
            }
            
            if (snsClient == null) {
                snsClient = buildSnsClient(this);
            }
            
            // Validate that the topic actually exists
            validateTopicArnExists(this.topicArn, snsClient);
            
            return new AwsSnsTopic(this);
        }

        /**
         * Validates that the provided topic ARN points to an existing topic.
         * Throws an exception if the topic does not exist or is inaccessible.
         */
        private static void validateTopicArnExists(String topicArn, SnsClient snsClient) {
            // Use GetTopicAttributes to verify the topic exists
            // All exceptions will be handled by getException() which maps them to appropriate SubstrateSdkException types
            GetTopicAttributesRequest request = GetTopicAttributesRequest.builder()
                .topicArn(topicArn)
                .build();
            snsClient.getTopicAttributes(request);
        }
    }
}
