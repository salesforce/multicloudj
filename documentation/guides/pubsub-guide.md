---
layout: default
title: How to PubSub
nav_order: 4
parent: Usage Guides
---
# PubSub

The `multicloudj` library provides a cloud-agnostic publish/subscribe interface through two clients: `TopicClient` for publishing messages and `SubscriptionClient` for consuming them. Supported providers include AWS SQS, AWS SNS, and GCP Pub/Sub.

Internally, each provider is implemented via drivers extending `AbstractTopic` and `AbstractSubscription`.

---

## Feature Support Across Providers

### TopicClient Features

| Feature Name | GCP Pub/Sub | AWS SQS | AWS SNS | Comments |
|--------------|-------------|---------|---------|----------|
| **Publish Message** | ✅ Supported | ✅ Supported | ✅ Supported | Send a single message to a topic/queue |
| **Message Metadata** | ✅ Supported | ✅ Supported | ✅ Supported | Arbitrary key/value string attributes |
| **Loggable ID** | ✅ Supported | ✅ Supported | ✅ Supported | Optional identifier for tracing |
| **Automatic Batching** | ✅ Supported | ✅ Supported | ✅ Supported | Messages are batched automatically by the driver |
| **FIFO / Deduplication** | ❌ Not Supported | ✅ Supported | ✅ Supported | DeduplicationId and MessageGroupId via metadata |

### SubscriptionClient Features

| Feature Name | GCP Pub/Sub | AWS SQS | AWS SNS | Comments |
|--------------|-------------|---------|---------|----------|
| **Receive Message** | ✅ Supported | ✅ Supported | ❌ Not Applicable | SNS is push-only and has no subscription client |
| **Acknowledge (single)** | ✅ Supported | ✅ Supported | ❌ Not Applicable | Mark message as successfully processed |
| **Acknowledge (batch)** | ✅ Supported | ✅ Supported | ❌ Not Applicable | Batch ack for efficiency |
| **Nack (single)** | ✅ Supported | ✅ Supported | ❌ Not Applicable | Request redelivery of a failed message |
| **Nack (with timeout)** | ✅ Supported | ✅ Supported | ❌ Not Applicable | Override redelivery delay per call |
| **Nack (batch)** | ✅ Supported | ✅ Supported | ❌ Not Applicable | Batch nack for efficiency |
| **Get Attributes** | ✅ Supported | ✅ Supported | ❌ Not Applicable | Retrieve subscription name and topic |
| **Retryable Check** | ⚠️ Partial | ❌ Not Supported | ❌ Not Applicable | GCP returns true for DEADLINE_EXCEEDED only; AWS handles retries internally |

### Configuration Options

| Configuration | GCP Pub/Sub | AWS SQS | AWS SNS | Comments |
|---------------|-------------|---------|---------|---------|
| **Region Support** | ✅ Supported | ✅ Supported | ✅ Supported | Region-specific operations |
| **Endpoint Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom endpoint configuration |
| **Proxy Support** | ✅ Supported | ✅ Supported | ✅ Supported | HTTP proxy configuration |
| **Credentials Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom credential providers via STS |
| **Nack Visibility Timeout** | ✅ Supported | ✅ Supported | ❌ Not Applicable | Default redelivery delay for nacked messages |

### Provider-Specific Notes

**AWS SQS (`awssqs`)**
- The queue URL is used as the topic/subscription name.
- Batch operations are limited to **10 messages per request**.
- Maximum visibility timeout (nack delay) is **43200 seconds (12 hours)**.
- When consuming from an SQS queue that is subscribed to an SNS topic, the driver automatically unwraps the SNS JSON envelope.
- For FIFO queues, use metadata keys `DeduplicationId` and `MessageGroupId`.

**AWS SNS (`awssns`)**
- The topic ARN is used as the topic name.
- SNS is publish-only — there is no `SubscriptionClient` for SNS.
- For FIFO topics, use metadata keys `DeduplicationId`, `MessageGroupId`, and optionally `Subject`.

**GCP Pub/Sub (`gcp`)**
- Topic and subscription names must use the full resource path format:
  - Topic: `projects/{projectId}/topics/{topicId}`
  - Subscription: `projects/{projectId}/subscriptions/{subscriptionId}`
- Batch operations support up to **1000 messages or 10 MB per batch**.
- Maximum ack deadline (nack timeout) is **600 seconds (10 minutes)**.

---

## TopicClient

### Creating a TopicClient

```java
TopicClient topicClient = TopicClient.builder("awssqs")
    .withTopicName("https://sqs.us-west-2.amazonaws.com/123456789012/my-queue")
    .withRegion("us-west-2")
    .build();
```

For AWS SNS:

```java
TopicClient topicClient = TopicClient.builder("awssns")
    .withTopicName("arn:aws:sns:us-west-2:123456789012:my-topic")
    .withRegion("us-west-2")
    .build();
```

For GCP Pub/Sub:

```java
TopicClient topicClient = TopicClient.builder("gcp")
    .withTopicName("projects/my-project/topics/my-topic")
    .withRegion("us-central1")
    .build();
```

With optional configuration:

```java
TopicClient topicClient = TopicClient.builder("awssqs")
    .withTopicName("https://sqs.us-west-2.amazonaws.com/123456789012/my-queue")
    .withRegion("us-west-2")
    .withEndpoint(URI.create("https://custom-endpoint.com"))
    .withProxyEndpoint(URI.create("https://proxy.example.com"))
    .withCredentialsOverrider(credentialsOverrider)
    .build();
```

### Building a Message

```java
Message message = Message.builder()
    .withBody("Hello, world!")           // String body (UTF-8 encoded)
    .withMetadata("env", "prod")         // Arbitrary key/value attributes
    .withMetadata("version", "2")
    .withLoggableID("req-abc-123")       // Optional trace ID for logging
    .build();
```

You can also supply a `byte[]` body directly:

```java
Message message = Message.builder()
    .withBody(new byte[]{1, 2, 3})
    .build();
```

### Sending a Message

```java
topicClient.send(message);
```

`send` blocks until the message is successfully delivered or an exception is thrown. The underlying driver batches messages automatically — calling `send` in a tight loop is efficient.

### FIFO / Deduplication (AWS only)

For AWS FIFO queues or topics, pass deduplication and group identifiers as metadata:

```java
Message message = Message.builder()
    .withBody("order event")
    .withMetadata("DeduplicationId", "order-789")
    .withMetadata("MessageGroupId", "orders")
    .build();

topicClient.send(message);
```

### Closing the TopicClient

```java
topicClient.close();
```

`close()` flushes any pending messages before shutting down. Use try-with-resources for guaranteed cleanup:

```java
try (TopicClient topicClient = TopicClient.builder("gcp")
        .withTopicName("projects/my-project/topics/my-topic")
        .withRegion("us-central1")
        .build()) {
    topicClient.send(message);
}
```

---

## SubscriptionClient

### Creating a SubscriptionClient

```java
SubscriptionClient subscriptionClient = SubscriptionClient.builder("awssqs")
    .withSubscriptionName("https://sqs.us-west-2.amazonaws.com/123456789012/my-queue")
    .withRegion("us-west-2")
    .build();
```

For GCP Pub/Sub:

```java
SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
    .withSubscriptionName("projects/my-project/subscriptions/my-subscription")
    .withRegion("us-central1")
    .build();
```

With a default nack visibility timeout:

```java
SubscriptionClient subscriptionClient = SubscriptionClient.builder("awssqs")
    .withSubscriptionName("https://sqs.us-west-2.amazonaws.com/123456789012/my-queue")
    .withRegion("us-west-2")
    .withNackVisibilityTimeout(Duration.ofSeconds(30))  // delay redelivery by 30s on nack
    .build();
```

### Receiving Messages

`receive()` blocks until a message is available. The driver handles background prefetching and flow control automatically.

```java
Message message = subscriptionClient.receive();
byte[] body = message.getBody();
Map<String, String> metadata = message.getMetadata();
AckID ackID = message.getAckID();
```

A typical consume loop:

```java
while (true) {
    Message message = subscriptionClient.receive();
    try {
        process(message);
        subscriptionClient.sendAck(message.getAckID());
    } catch (Exception e) {
        subscriptionClient.sendNack(message.getAckID());
    }
}
```

### Acknowledging Messages

Acknowledge a single message after successful processing:

```java
subscriptionClient.sendAck(message.getAckID());
```

Acknowledge multiple messages at once for better throughput:

```java
List<AckID> ackIDs = messages.stream()
    .map(Message::getAckID)
    .collect(Collectors.toList());

CompletableFuture<Void> future = subscriptionClient.sendAcks(ackIDs);
future.join(); // wait for completion if needed
```

### Negative Acknowledgment (Nack)

Nacking a message signals that processing failed and the message should be redelivered. Before nacking, check whether the provider supports it:

```java
if (subscriptionClient.canNack()) {
    subscriptionClient.sendNack(message.getAckID());
}
```

To override the redelivery delay for a specific message:

```java
// Delay redelivery by 60 seconds for this message only
subscriptionClient.sendNack(message.getAckID(), Duration.ofSeconds(60));
```

Batch nack multiple messages:

```java
CompletableFuture<Void> future = subscriptionClient.sendNacks(ackIDs);

// Or with a per-batch visibility timeout override
CompletableFuture<Void> future = subscriptionClient.sendNacks(ackIDs, Duration.ofSeconds(30));
```

**Provider nack behavior:**
- **AWS SQS**: Nack calls `ChangeMessageVisibility`. Maximum timeout is 43200 seconds (12 hours). `Duration.ZERO` causes immediate redelivery.
- **GCP Pub/Sub**: Nack calls `ModifyAckDeadline`. Maximum timeout is 600 seconds (10 minutes). `Duration.ZERO` causes immediate redelivery.

### Subscription Attributes

```java
GetAttributeResult attrs = subscriptionClient.getAttributes();
System.out.println("Subscription: " + attrs.getName());
System.out.println("Topic: " + attrs.getTopic());
```

### Retryable Error Check

```java
try {
    process(message);
    subscriptionClient.sendAck(message.getAckID());
} catch (Exception e) {
    if (subscriptionClient.isRetryable(e)) {
        // transient error — nack to retry
        subscriptionClient.sendNack(message.getAckID());
    } else {
        // permanent error — send to DLQ or log
        subscriptionClient.sendAck(message.getAckID());
    }
}
```

### Closing the SubscriptionClient

```java
subscriptionClient.close();
```

`close()` flushes any pending acknowledgments before shutting down. Use try-with-resources for guaranteed cleanup:

```java
try (SubscriptionClient subscriptionClient = SubscriptionClient.builder("awssqs")
        .withSubscriptionName("https://sqs.us-west-2.amazonaws.com/123456789012/my-queue")
        .withRegion("us-west-2")
        .build()) {
    Message message = subscriptionClient.receive();
    process(message);
    subscriptionClient.sendAck(message.getAckID());
}
```

---

## Error Handling

All operations may throw `SubstrateSdkException`:

```java
try {
    topicClient.send(message);
} catch (SubstrateSdkException e) {
    // Handle access denied, timeout, etc.
    e.printStackTrace();
}
```
