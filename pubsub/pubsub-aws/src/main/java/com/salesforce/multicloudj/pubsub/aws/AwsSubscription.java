package com.salesforce.multicloudj.pubsub.aws;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.Base64;
import java.nio.charset.StandardCharsets;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

@SuppressWarnings("rawtypes")
@AutoService(AbstractSubscription.class)
public class AwsSubscription extends AbstractSubscription<AwsSubscription> {

    private static final String BASE64_ENCODED_KEY = "base64encoded";
    private static final long NO_MESSAGES_POLL_DURATION_MS = 250;
    
    private final SqsClient sqsClient;
    private final boolean nackLazy;
    private final long waitTimeSeconds;
    
    public AwsSubscription() {
        this(new Builder());
    }
    
    public AwsSubscription(Builder builder) {
        super(builder);
        this.nackLazy = builder.nackLazy;
        this.waitTimeSeconds = builder.waitTimeSeconds;
        this.sqsClient = builder.sqsClient;
    }

    @Override
    public void sendAck(AckID ackID) {
    }

    @Override
    public CompletableFuture<Void> sendAcks(List<AckID> ackIDs) {
        return null;
    }

    @Override
    protected void doSendAcks(List<AckID> ackIDs) {
        // TODO: Implement AWS SQS acknowledgment
    }

    @Override
    protected void doSendNacks(List<AckID> ackIDs) {
        // TODO: Implement AWS SQS negative acknowledgment
    }

    @Override
    public void sendNack(AckID ackID) {
    }

    @Override
    public CompletableFuture<Void> sendNacks(List<AckID> ackIDs) {
        return null;
    }

    @Override
    protected List<Message> doReceiveBatch(int batchSize) {
        ReceiveMessageRequest.Builder requestBuilder = ReceiveMessageRequest.builder()
            .queueUrl(subscriptionName)
            .maxNumberOfMessages(Math.min(batchSize, 10)) // SQS supports max 10 messages
            .messageAttributeNames("All")
            .attributeNames(QueueAttributeName.ALL);

        if (waitTimeSeconds > 0) {
            requestBuilder.waitTimeSeconds((int) waitTimeSeconds);
        }

        ReceiveMessageResponse response = sqsClient.receiveMessage(requestBuilder.build());
        List<Message> messages = new ArrayList<>();

        for (var sqsMessage : response.messages()) {
            Message message = convertToMessage(sqsMessage);
            messages.add(message);
        }

        if (messages.isEmpty()) {
            try {
                Thread.sleep(NO_MESSAGES_POLL_DURATION_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SubstrateSdkException("Interrupted while waiting for messages", e);
            }
        }
        return messages;
    }

    /**
     * Converts SQS message to internal Message format.
     */
    protected Message convertToMessage(software.amazon.awssdk.services.sqs.model.Message sqsMessage) {
        String bodyStr = sqsMessage.body();
        Map<String, String> rawAttrs = new HashMap<>();
        
        // Extract message attributes
        for (Map.Entry<String, MessageAttributeValue> entry : sqsMessage.messageAttributes().entrySet()) {
            rawAttrs.put(entry.getKey(), entry.getValue().stringValue());
        }
        
        // Decode metadata attributes
        Map<String, String> attrs = new HashMap<>();
        boolean decodeBody = false;
        
        for (Map.Entry<String, String> entry : rawAttrs.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            if (BASE64_ENCODED_KEY.equals(key)) {
                decodeBody = true;
                continue;
            }
            
            attrs.put(decodeMetadataKey(key), decodeMetadataValue(value));
        }
        
        byte[] bodyBytes;
        if (decodeBody) {
            try {
                bodyBytes = Base64.getDecoder().decode(bodyStr);
            } catch (IllegalArgumentException e) {
                // Fall back to using the raw message
                bodyBytes = bodyStr.getBytes(StandardCharsets.UTF_8);
            }
        } else {
            bodyBytes = bodyStr.getBytes(StandardCharsets.UTF_8);
        }
        
        AckID ackID = new AwsAckID(sqsMessage.receiptHandle());
        
        return Message.builder()
            .withBody(bodyBytes)
            .withMetadata(attrs)
            .withAckID(ackID)
            .withLoggableID(sqsMessage.messageId())
            .build();
    }
    
    /**
     * Decodes metadata keys that contain hex-encoded special characters.
     * 
     * AWS SQS message attribute names can only contain alphanumeric characters,
     * underscores (_), hyphens (-), and periods (.). Special characters like
     * spaces, slashes, etc. are not allowed.
     * reference: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html
     * 
     * To work around this limitation, we use a custom encoding scheme:
     * special characters are encoded as "__0xHH__" where HH is the hex value
     * of the character.
     * 
     * Examples:
     * - "file__0x2F__path" → "file/path" (0x2F = '/')
     * - "user__0x20__name" → "user name" (0x20 = ' ')
     * - "type__0x3A__message" → "type:message" (0x3A = ':')
     */

    protected String decodeMetadataKey(String key) {
        if (key == null) return "";
        
        StringBuilder decoded = new StringBuilder();
        int i = 0;
        while (i < key.length()) {
            // Check if we have enough characters for a potential hex pattern (__0xHH__)
            // Minimum pattern length is 6: "__0x" (4) + at least 1 hex digit + "__" (2)
            if (i < key.length() - 6 && key.substring(i, i + 4).equals("__0x")) {
                // Found the start of a hex encoding pattern "__0x"
                // Now find the closing "__" to extract the hex value
                int endIdx = key.indexOf("__", i + 4);
                
                if (endIdx != -1) {
                    try {
                        // Extract hex string between "__0x" and "__"
                        // For example: from "__0x2F__", extract "2F"
                        String hexStr = key.substring(i + 4, endIdx);
                        
                        // Convert hex to decimal (e.g., "2F" → 47)
                        int charCode = Integer.parseInt(hexStr, 16);
                        
                        // Cast to char and append (e.g., 47 → '/')
                        decoded.append((char) charCode);
                        
                        // Move index past the entire pattern "__0xHH__"
                        i = endIdx + 2;  // +2 to skip the closing "__"
                        
                    } catch (NumberFormatException e) {
                        // Invalid hex format, treat as regular character
                        // This handles malformed patterns like "__0xGG__"
                        decoded.append(key.charAt(i));
                        i++;
                    }
                } else {
                    // No closing "__" found, treat as regular character
                    // This handles incomplete patterns like "__0x2F" at the end
                    decoded.append(key.charAt(i));
                    i++;
                }
            } else {
                // Regular character, not part of hex encoding pattern
                decoded.append(key.charAt(i));
                i++;
            }
        }
        
        return decoded.toString();
    }

    protected String decodeMetadataValue(String value) {
        if (value == null) return "";
        try {
            return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return value;
        }
    }

    private static void validateSubscriptionName(String subscriptionName) {
        if (subscriptionName == null || subscriptionName.trim().isEmpty()) {
            throw new InvalidArgumentException("Subscription name cannot be null or empty");
        }
        if (!subscriptionName.startsWith("https://sqs.") || !subscriptionName.contains(".amazonaws.com/")) {
            throw new InvalidArgumentException(
                "Subscription name must be in format: https://sqs.region.amazonaws.com/account/queue-name, got: " + subscriptionName);
        }
    }

    @Override
    protected Batcher.Options createReceiveBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(100)
            .setMinBatchSize(1)
            .setMaxBatchSize(10) // SQS supports max 10 messages per receive
            .setMaxBatchByteSize(0); // No limit
    }

    @Override
    protected Batcher.Options createAckBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(100)
            .setMinBatchSize(1)
            .setMaxBatchSize(10) // SQS supports max 10 messages per batch operation
            .setMaxBatchByteSize(0); // No limit
    }

    @Override
    public boolean canNack() {
        return true;
    }

    @Override
    public boolean isRetryable(Throwable error) {
        // The AWS client handles retries
        return false;
    }

    @Override
    public GetAttributeResult getAttributes() {
        return new GetAttributeResult.Builder()
                .name("aws-subscription")
                .topic("aws-topic")
                .build();
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
    
    public Builder builder() {
        return new Builder();
    }
    
    /**
     * AWS-specific implementation of AckID. 
     */
    public static class AwsAckID implements AckID {
        private final String receiptHandle;
        
        public AwsAckID(String receiptHandle) {
            if (receiptHandle == null || receiptHandle.trim().isEmpty()) {
                throw new IllegalArgumentException("Receipt handle cannot be null or empty");
            }
            this.receiptHandle = receiptHandle;
        }
        
        public String getReceiptHandle() {
            return receiptHandle;
        }
        
        @Override
        public String toString() {
            return receiptHandle;
        }
    }

    public static class Builder extends AbstractSubscription.Builder<AwsSubscription> {
        private boolean nackLazy = false;
        private long waitTimeSeconds = 0;
        private SqsClient sqsClient;
        
        public Builder() {
            this.providerId = AwsConstants.PROVIDER_ID;
        }
        
        public Builder withNackLazy(boolean nackLazy) {
            this.nackLazy = nackLazy;
            return this;
        }
        
        public Builder withWaitTimeSeconds(long waitTimeSeconds) {
            this.waitTimeSeconds = waitTimeSeconds;
            return this;
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
        public AwsSubscription build() {
            validateSubscriptionName(subscriptionName);
            
            if (sqsClient == null) {
                sqsClient = buildSqsClient(this);
            }
            return new AwsSubscription(this);
        }
    }
} 