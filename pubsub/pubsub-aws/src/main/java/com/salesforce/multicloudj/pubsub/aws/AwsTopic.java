package com.salesforce.multicloudj.pubsub.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

class MetadataKeys {
    public static final String DEDUPLICATION_ID = "DeduplicationId";
    public static final String MESSAGE_GROUP_ID = "MessageGroupId";
    public static final String BASE64_ENCODED = "base64encoded";
}

@SuppressWarnings("rawtypes")
@AutoService(AbstractTopic.class)
public class AwsTopic extends AbstractTopic<AwsTopic> {

    private static final int MAX_SQS_ATTRIBUTES = 10;
    private final SqsClient sqsClient;

    public AwsTopic() {
        this(new Builder());
    }
    
    public AwsTopic(Builder builder) {
        super(builder);
        validateTopicName(topicName);
        this.sqsClient = builder.sqsClient;
    }

    /**
     * Override batcher options to align with AWS SQS limits.
     * AWS SQS supports up to 10 messages per batch.
     */
    @Override
    protected Batcher.Options createBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(2)
            .setMinBatchSize(1)
            .setMaxBatchSize(10)
            .setMaxBatchByteSize(256 * 1024); // 256KB per message limit
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
            
            // Handle base64 encoding for non-UTF8 content
            String rawBody = new String(message.getBody(), StandardCharsets.UTF_8);
            String messageBody = maybeEncodeBody(message.getBody());
            if (!messageBody.equals(rawBody)) {
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
            
            // Set SQS-specific attributes directly from metadata
            setSqsEntryAttributes(message, entryBuilder);
            
            entries.add(entryBuilder.build());
        }

        SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
            .queueUrl(topicName)
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

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException && !t.getClass().equals(SubstrateSdkException.class)) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        }
        if (t instanceof AwsServiceException) {
            AwsServiceException serviceException = (AwsServiceException) t;
            if (serviceException.awsErrorDetails() != null) {
                String errorCode = serviceException.awsErrorDetails().errorCode();
                return ErrorCodeMapping.getException(errorCode);
            }
            return UnknownException.class;
        }
        if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (sqsClient != null) {
            sqsClient.close();
        }
    }

    /**
     * Executes hooks before sending messages to SQS queue.
     * Override this method to add custom pre-send logic.
     */
    @Override
    protected void executeBeforeSendBatchHooks(List<Message> messages) {
        // Default implementation - no pre-send hooks
    }

    /**
     * Executes hooks after successfully sending messages to SQS queue.
     * Override this method to add custom post-send logic.
     */
    @Override
    protected void executeAfterSendBatchHooks(List<Message> messages) {
        // Default implementation - no post-send hooks
    }

    /**
     * Validates that the topic name is in the correct AWS SQS URL format.
     */
    private static void validateTopicName(String topicName) {
        if (topicName == null) {
            throw new InvalidArgumentException("SQS topic name cannot be null");
        }
        if (topicName.trim().isEmpty()) {
            throw new InvalidArgumentException("SQS topic name cannot be empty");
        }

        // Validate SQS URL format: https://sqs.region.amazonaws.com/account/queue-name
        String sqsUrlPattern = "https://sqs\\.[^/]+\\.amazonaws\\.com/[^/]+/.+";
        if (!topicName.matches(sqsUrlPattern)) {
            throw new InvalidArgumentException(
                    "SQS topic name must be in format: https://sqs.region.amazonaws.com/account/queue-name, got: " + topicName);
        }
    }

    /**
     * Sets SQS-specific attributes on a SendMessageBatchRequestEntry based on message metadata.
     */
    private void setSqsEntryAttributes(Message message, SendMessageBatchRequestEntry.Builder entryBuilder) {
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
                
                // SQS has a limit of 10 message attributes per message
                if (attributes.size() >= MAX_SQS_ATTRIBUTES) {
                    // stop adding to avoid SQS rejection
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

    /**
     * Encodes metadata keys that contain special characters.
     * 
     * AWS SQS message attribute names can only contain alphanumeric characters,
     * underscores (_), hyphens (-), and periods (.). Special characters like
     * spaces, slashes, etc. are not allowed.
     * 
     * To work around this limitation, we use a custom encoding scheme:
     * special characters are encoded as "__0xHH__" where HH is the hex value
     * of the character.
     * 
     * Examples:
     * - "file/path" → "file__0x2F__path" (0x2F = '/')
     * - "user name" → "user__0x20__name" (0x20 = ' ')
     * - "type:message" → "type__0x3A__message" (0x3A = ':')
     */
    private String encodeMetadataKey(String key) {
        if (key == null) return "";
        
        // AWS MessageAttributeName only allows [a-zA-Z0-9_.-]
        // reference: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html
        // Encode special characters using hex pattern "__0xHH__"
        StringBuilder encoded = new StringBuilder();
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if ((c >= 'a' && c <= 'z') || 
                (c >= 'A' && c <= 'Z') || 
                (c >= '0' && c <= '9') || 
                c == '_' || c == '-' || c == '.') {
                // Valid character, append directly
                encoded.append(c);
            } else {
                // Invalid character, encode as "__0xHH__"
                String hexValue = Integer.toHexString(c).toUpperCase();
                encoded.append("__0x").append(hexValue).append("__");
            }
        }
        return encoded.toString();
    }

    /**
     * Encodes metadata value using URL encoding.
     */
    private String encodeMetadataValue(String value) {
        if (value == null) return "";
        return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    /**
     * Handles base64 encoding for non-UTF8 content.
     */
    private String maybeEncodeBody(byte[] body) {
        if (body == null) return "";
        
        if (isValidUtf8(body)) {
            return new String(body, StandardCharsets.UTF_8);
        }
        
        return java.util.Base64.getEncoder().encodeToString(body);
    }

    /**
     * Checks if byte array contains valid UTF-8.
     */
    private boolean isValidUtf8(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return true;
        }
        
        try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
            decoder.decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder extends AbstractTopic.Builder<AwsTopic> {
        private SqsClient sqsClient;
        
        public Builder() {
            this.providerId = AwsConstants.PROVIDER_ID;
        }
        
        public Builder withSqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }
        
        private static SqsClient buildSqsClient(Builder builder) {
            return SqsClientUtil.buildSqsClient(
                builder.region,
                builder.endpoint,
                builder.credentialsOverrider);
        }
        
        @Override
        public AwsTopic build() {
            if (sqsClient == null) {
                sqsClient = buildSqsClient(this);
            }
            return new AwsTopic(this);
        }
    }
} 