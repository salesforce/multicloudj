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
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;

/**
 * Metadata keys used for setting message attributes on SNS and SQS messages.
 */
class MetadataKeys {
    /** 
     * DeduplicationId is used for FIFO queues/topics to ensure message deduplication.
     * Supported by both SQS (FIFO queues) and SNS (FIFO topics).
     */
    public static final String DEDUPLICATION_ID = "DeduplicationId";
    
    /** 
     * MessageGroupId is used for FIFO queues/topics to ensure message ordering.
     * Supported by both SQS (FIFO queues) and SNS (FIFO topics).
     */
    public static final String MESSAGE_GROUP_ID = "MessageGroupId";
    
    /** 
     * Subject is used for SNS topics to set the message subject.
     * Only supported by SNS
     */
    public static final String SUBJECT = "Subject";
    
    /** 
     * Base64Encoded is a flag indicating that the message body is base64 encoded.
     * Used when message body contains non-UTF8 content.
     */
    public static final String BASE64_ENCODED = "base64encoded";
}

@SuppressWarnings("rawtypes")
@AutoService(AbstractTopic.class)
public class AwsTopic extends AbstractTopic<AwsTopic> {

    private static final int MAX_SQS_ATTRIBUTES = 10;
    private static final int MAX_SNS_ATTRIBUTES = 10;
    
    /**
     * Used to specify whether to use SQS or SNS for message publishing.
     */
    public enum ServiceType {
        SQS,
        SNS
    }
    
    /**
     * BodyBase64Encoding is an enum of strategies for when to base64 message bodies.
     */
    public enum BodyBase64Encoding {
        /**
         * NonUTF8Only means that message bodies that are valid UTF-8 encodings are
         * sent as-is. Invalid UTF-8 message bodies are base64 encoded, and a
         * MessageAttribute with key "base64encoded" is added to the message.
         */
        NON_UTF8_ONLY,
        /**
         * Always means that all message bodies are base64 encoded.
         * A MessageAttribute with key "base64encoded" is added to the message.
         */
        ALWAYS,
        /**
         * Never means that message bodies are never base64 encoded. Non-UTF-8
         * bytes in message bodies may be modified by SNS/SQS.
         */
        NEVER
    }
    
    /**
     * TopicOptions contains configuration options for topics.
     */
    public static class TopicOptions {
        /**
         * BodyBase64Encoding determines when message bodies are base64 encoded.
         * The default is NON_UTF8_ONLY.
         */
        private BodyBase64Encoding bodyBase64Encoding = BodyBase64Encoding.NON_UTF8_ONLY;
        
        public BodyBase64Encoding getBodyBase64Encoding() {
            return bodyBase64Encoding;
        }
        
        public TopicOptions withBodyBase64Encoding(BodyBase64Encoding encoding) {
            this.bodyBase64Encoding = encoding;
            return this;
        }
    }
    
    private final ServiceType serviceType;
    private final SqsClient sqsClient;
    private final SnsClient snsClient;
    private final String topicUrl;  // For SQS
    private final String topicArn;   // For SNS
    private final TopicOptions topicOptions;

    public AwsTopic() {
        this(new Builder());
    }
    
    public AwsTopic(Builder builder) {
        super(builder);
        this.serviceType = builder.serviceType;
        this.sqsClient = builder.sqsClient;
        this.snsClient = builder.snsClient;
        this.topicUrl = builder.topicUrl;
        this.topicArn = builder.topicArn;
        this.topicOptions = builder.topicOptions != null ? builder.topicOptions : new TopicOptions();
    }

    /**
     * Override batcher options to align with AWS SQS/SNS limits.
     */
    @Override
    protected Batcher.Options createBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(100)  // max concurrency for sends 
            .setMinBatchSize(1)
            .setMaxBatchSize(10)  // SQS/SNS SendBatch supports 10 messages at a time
            .setMaxBatchByteSize(256 * 1024); // 256KB per message limit
    }

    @Override
    protected void doSendBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (serviceType == ServiceType.SNS) {
            sendToSns(messages);
        } else {
            sendToSqs(messages);
        }
    }

    /**
     * Send messages to SQS queue.
     */
    private void sendToSqs(List<Message> messages) {
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            
            // Encode metadata as message attributes
            Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> attributes = 
                convertMetadataToSqsAttributes(message.getMetadata());
            
            // Handle base64 encoding based on topic options
            String rawBody = new String(message.getBody(), StandardCharsets.UTF_8);
            String messageBody;
            boolean didEncode = maybeEncodeBody(message.getBody(), topicOptions.getBodyBase64Encoding());
            if (didEncode) {
                messageBody = java.util.Base64.getEncoder().encodeToString(message.getBody());
                // Add base64 encoding flag
                attributes.put(MetadataKeys.BASE64_ENCODED, 
                    software.amazon.awssdk.services.sqs.model.MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue("true")
                        .build());
            } else {
                messageBody = rawBody;
            }
            
            SendMessageBatchRequestEntry.Builder entryBuilder = SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody(messageBody)
                .messageAttributes(attributes);
            
            // Set SQS-specific attributes directly from metadata
            reviseSqsEntryAttributes(message, entryBuilder);
            
            entries.add(entryBuilder.build());
        }

        SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
            .queueUrl(topicUrl)
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
     * Send messages to SNS topic.
     */
    private void sendToSns(List<Message> messages) {
        List<PublishBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            
            // Encode metadata as message attributes
            Map<String, software.amazon.awssdk.services.sns.model.MessageAttributeValue> attributes = 
                convertMetadataToSnsAttributes(message.getMetadata());
            
            // Handle base64 encoding based on topic options
            String rawBody = new String(message.getBody(), StandardCharsets.UTF_8);
            String messageBody;
            boolean didEncode = maybeEncodeBody(message.getBody(), topicOptions.getBodyBase64Encoding());
            if (didEncode) {
                messageBody = java.util.Base64.getEncoder().encodeToString(message.getBody());
                // Add base64 encoding flag
                attributes.put(MetadataKeys.BASE64_ENCODED, 
                    software.amazon.awssdk.services.sns.model.MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue("true")
                        .build());
            } else {
                messageBody = rawBody;
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
        if (snsClient != null) {
            snsClient.close();
        }
    }

    /**
     * Executes hooks before sending messages to SQS queue or SNS topic.
     * Override this method to add custom pre-send logic.
     */
    @Override
    protected void executeBeforeSendBatchHooks(List<Message> messages) {
        // Default implementation - no pre-send hooks
    }

    /**
     * Executes hooks after successfully sending messages to SQS queue or SNS topic.
     * Override this method to add custom post-send logic.
     */
    @Override
    protected void executeAfterSendBatchHooks(List<Message> messages) {
        // Default implementation - no post-send hooks
    }

    /**
     * Validates that the topic name/ARN is valid
     */
    static void validateTopicName(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new InvalidArgumentException("Topic name/ARN cannot be null or empty");
        }
    }
    
    static String getQueueUrl(String queueName, SqsClient sqsClient) 
            throws AwsServiceException, SdkClientException {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
            .queueName(queueName)
            .build();
        
        GetQueueUrlResponse response = sqsClient.getQueueUrl(request);
        return response.queueUrl();
    }

    /**
     * Sets SQS-specific attributes on a SendMessageBatchRequestEntry based on message metadata.
     */
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
     * Sets attributes on a PublishBatchRequestEntry based on message metadata.
     */
    private void reviseSnsEntryAttributes(Message message, PublishBatchRequestEntry.Builder entryBuilder) {
        Map<String, String> metadata = message.getMetadata();
        if (metadata != null) {
            // Set subject if provided (SNS-specific)
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
     * Converts message metadata to SQS message attributes.
     */
    private Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> convertMetadataToSqsAttributes(Map<String, String> metadata) {
        Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> attributes = new java.util.HashMap<>();
        
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
                    software.amazon.awssdk.services.sqs.model.MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(encodedValue)
                        .build());
            }
        }
        return attributes;
    }

    /**
     * Converts message metadata to SNS message attributes.
     */
    private Map<String, software.amazon.awssdk.services.sns.model.MessageAttributeValue> convertMetadataToSnsAttributes(Map<String, String> metadata) {
        Map<String, software.amazon.awssdk.services.sns.model.MessageAttributeValue> attributes = new java.util.HashMap<>();
        
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
                    software.amazon.awssdk.services.sns.model.MessageAttributeValue.builder()
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
     * AWS SQS/SNS message attribute names can only contain alphanumeric characters,
     * underscores (_), hyphens (-), and periods (.). Special characters like
     * spaces, slashes, etc. are not allowed. Additionally, periods cannot be at
     * the start of the key or follow another period.
     * 
     * To work around this limitation, we use a custom encoding scheme:
     * special characters are encoded as "__0xHH__" where HH is the hex value
     * of the character.
     * 
     * Examples:
     * - "file/path" → "file__0x2F__path" (0x2F = '/')
     * - "user name" → "user__0x20__name" (0x20 = ' ')
     * - "type:message" → "type__0x3A__message" (0x3A = ':')
     * - ".key" → "__0x2E__key" (period at start)
     * - "key..name" → "key__0x2E____0x2E__name" (consecutive periods)
     */
    private String encodeMetadataKey(String key) {
        if (key == null) return "";
        
        // AWS MessageAttributeName only allows [a-zA-Z0-9_.-]
        // reference: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html
        // reference: https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
        // Encode special characters using hex pattern "__0xHH__"
        // Periods at start or consecutive periods must be encoded
        StringBuilder encoded = new StringBuilder();
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            boolean isValid = false;
            
            if ((c >= 'a' && c <= 'z') || 
                (c >= 'A' && c <= 'Z') || 
                (c >= '0' && c <= '9') || 
                c == '_' || c == '-') {
                // Valid character, append directly
                isValid = true;
            } else if (c == '.') {
                // Period is valid only if not at start and previous char is not a period
                if (i != 0 && key.charAt(i - 1) != '.') {
                    isValid = true;
                }
            }
            
            if (isValid) {
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
    /**
     * Decides whether body should be base64-encoded based on encoding option.
     * Returns true if encoding occurred, false otherwise.
     */
    private boolean maybeEncodeBody(byte[] body, BodyBase64Encoding encoding) {
        if (body == null) return false;
        
        switch (encoding) {
            case ALWAYS:
                return true;
            case NEVER:
                return false;
            case NON_UTF8_ONLY:
            default:
                return !isValidUtf8(body);
        }
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

    @Override
    public Builder builder() {
        return new Builder();
    }
    
    public static class Builder extends AbstractTopic.Builder<AwsTopic> {
        private ServiceType serviceType; 
        private SqsClient sqsClient;
        private SnsClient snsClient;
        private String topicUrl;  // For SQS
        private String topicArn;   // For SNS
        private TopicOptions topicOptions;
        
        public Builder() {
            this.providerId = AwsConstants.PROVIDER_ID;
        }
        
        /**
         * Sets the topic name, ARN, or URL.
         */
        @Override
        public Builder withTopicName(String topicName) {
            super.withTopicName(topicName);
            
            // Auto-detect ARN or URL format
            if (topicName != null) {
                if (topicName.startsWith("arn:aws:sns:")) {
                    // SNS topic ARN - user provided ARN format
                    this.topicArn = topicName;
                } else if (topicName.startsWith("https://sqs.")) {
                    // SQS queue URL, in case user provides full URL
                    // This avoids treating the URL as a queue name and calling GetQueueUrl with invalid input
                    this.topicUrl = topicName;
                }
                // Otherwise, treat as name (will be resolved to URL via GetQueueUrl)
            }
            
            return this;
        }
        
        Builder withServiceType(ServiceType serviceType) {
            this.serviceType = serviceType;
            return this;
        }
        
        public Builder withSqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }
        
        public Builder withSnsClient(SnsClient snsClient) {
            this.snsClient = snsClient;
            return this;
        }
        
        /**
         * Directly set the topic URL to avoid calling GetQueueUrl again.
         * Used when the queue URL has already been resolved (for SQS).
         */
        Builder withTopicUrl(String topicUrl) {
            this.topicUrl = topicUrl;
            return this;
        }
        
        public Builder withTopicOptions(TopicOptions topicOptions) {
            this.topicOptions = topicOptions;
            return this;
        }
        
        private static SqsClient buildSqsClient(Builder builder) {
            return SqsClientUtil.buildSqsClient(
                builder.region,
                builder.endpoint,
                builder.credentialsOverrider);
        }
        
        private static SnsClient buildSnsClient(Builder builder) {
            return SnsClientUtil.buildSnsClient(
                builder.region,
                builder.endpoint,
                builder.credentialsOverrider);
        }
        
        @Override
        public AwsTopic build() {
            validateTopicName(this.topicName);
            
            // Auto-detect service type based on provided parameters
            if (serviceType == null) {
                if (this.topicArn != null || this.snsClient != null) {
                    serviceType = ServiceType.SNS;
                } else if (this.topicUrl != null || this.sqsClient != null) {
                    serviceType = ServiceType.SQS;
                } else {
                    // Default to SQS if only topicName is provided
                    serviceType = ServiceType.SQS;
                }
            }
            
            if (serviceType == ServiceType.SNS) {
                // SNS mode
                if (snsClient == null) {
                    snsClient = buildSnsClient(this);
                }
                
                // SNS requires topicArn to be set
                // Note: topicArn might be null if user provided snsClient or set serviceType explicitly
                // but didn't provide an ARN-format topicName (e.g., provided regular name instead)
                if (this.topicArn == null) {
                    throw new InvalidArgumentException(
                        "Topic ARN must be set when using SNS. Use withTopicName() with an ARN format (e.g., 'arn:aws:sns:region:account:topic-name').");
                }
            } else {
                // SQS mode 
                if (sqsClient == null) {
                    sqsClient = buildSqsClient(this);
                }
                
                // get the full queue URL from the queue name 
                if (this.topicUrl == null) {
                    this.topicUrl = getQueueUrl(this.topicName, sqsClient);
                }
            }
            
            return new AwsTopic(this);
        }
    }
} 