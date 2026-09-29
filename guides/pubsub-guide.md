---
layout: default
title: How to Pub/Sub
nav_order: 4
parent: Usage Guides
---
# Pub/Sub

The `TopicClient` and `SubscriptionClient` classes in the `multicloudj` library provide a comprehensive, cloud-agnostic interface to interact with publish/subscribe messaging services like Google Cloud Pub/Sub, AWS SNS/SQS, and Alibaba Cloud Message Service.

These clients enable sending messages to topics, receiving messages from subscriptions, and managing message acknowledgment across multiple cloud providers with a consistent API.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Send Messages** | ✅ Supported | ✅ Supported | ✅ Supported | Send messages to topics |
| **Receive Messages** | ✅ Supported | ✅ Supported | ✅ Supported | Pull messages from subscriptions |
| **Acknowledge Messages** | ✅ Supported | ✅ Supported | ✅ Supported | Confirm message processing |
| **Batch Acknowledgment** | ✅ Supported | ✅ Supported | ✅ Supported | Acknowledge multiple messages at once |
| **Negative Acknowledgment** | ✅ Supported | ✅ Supported | ✅ Supported | Reject messages for redelivery |
| **Subscription Attributes** | ✅ Supported | ✅ Supported | ✅ Supported | Retrieve subscription name and topic |

### Advanced Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Async Batch Acknowledgment** | ✅ Supported | ✅ Supported | ✅ Supported | CompletableFuture-based async ack/nack |
| **Double Acknowledgment Safety** | ✅ Supported | ✅ Supported | ✅ Supported | Safe to ack same message multiple times |
| **Nack Visibility Timeout** | ✅ Supported | ✅ Supported | ✅ Supported | Control redelivery delay on nack (default and per-call) |
| **Retryable Error Detection** | ✅ Supported | ✅ Supported | ✅ Supported | `isRetryable()` classifies errors as transient or permanent |

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Region** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Target region for the topic/subscription |
| **Endpoint Override** | ✅ Supported | ✅ Supported | ✅ Supported | Required for Ali — account-scoped `https://<accountId>.mns.<region>.aliyuncs.com` |
| **Proxy Support** | ✅ Supported | ✅ Supported | ✅ Supported | HTTP proxy configuration |
| **Credentials Override** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Custom credential providers via STS |

---

### Provider IDs

The provider ID passed to `TopicClient.builder(...)` and `SubscriptionClient.builder(...)` selects the backing implementation:

| Provider | Topic provider ID | Subscription provider ID |
|----------|-------------------|--------------------------|
| GCP (Google Cloud Pub/Sub) | `gcp` | `gcp` |
| AWS SNS | `awssns` | `aws` |
| AWS SQS | `awssqs` | `aws` |
| Alibaba SMQ (queue) | `alismqqueue` | `ali` |
| Alibaba SMQ (topic) | `alismqtopic` | `ali` |

On AWS, messages are published through either SNS (`awssns`) or SQS (`awssqs`), while messages are always received from an SQS queue using the `aws` subscription provider.

On Alibaba, messages are published either directly to an SMQ queue (`alismqqueue`) or to an SMQ topic (`alismqtopic`) that fans out to a bound queue subscription; messages are always received from an SMQ queue using the `ali` subscription provider.

### Provider-Specific Notes

**GCP (Google Cloud Pub/Sub)**
- Topic names must use the full resource format: `projects/{projectId}/topics/{topicId}`
- Subscription names must use the full resource format: `projects/{projectId}/subscriptions/{subscriptionId}`

**AWS (SNS / SQS)**
- SNS topics (`awssns`) are identified by their topic ARN, e.g. `arn:aws:sns:us-west-2:123456789012:my-topic`. The topic is validated to exist when the client is built.
- SQS topics (`awssqs`) accept either a queue name (resolved to a queue URL automatically) or a full queue URL.
- Subscriptions (`aws`) read from an SQS queue, identified by a queue name or queue URL.

**Alibaba (SMQ / MNS)**
- An account-scoped endpoint is **required** on every client: `https://<accountId>.mns.<region>.aliyuncs.com`. The account id cannot be derived from the region and credentials, so `.withEndpoint(...)` must be set. This is why the endpoint is required and the region is not consumed.
- Credentials are resolved from Alibaba's default (ambient) credential provider chain.
- Queue publish (`alismqqueue`) sends directly to an SMQ queue. Topic publish (`alismqtopic`) fans out to a bound SMQ queue subscription; the consumer reads that queue via the `ali` subscription provider.
- When binding a topic to a queue, use a **non-SIMPLIFIED (JSON)** message format so that message metadata is preserved end to end.
- You own provisioning and maintenance of the topic, the queue, and the topic-to-queue binding/subscription definition. The SDK does not create or manage these resources.
- Message metadata is carried as native SMQ user properties.

---

## Creating Clients

### Topic Client

```java
// GCP
TopicClient topicClient = TopicClient.builder("gcp")
    .withTopicName("projects/my-project/topics/my-topic")
    .build();

// AWS SNS
TopicClient snsTopicClient = TopicClient.builder("awssns")
    .withTopicName("arn:aws:sns:us-west-2:123456789012:my-topic")
    .withRegion("us-west-2")
    .build();

// AWS SQS
TopicClient sqsTopicClient = TopicClient.builder("awssqs")
    .withTopicName("my-queue")
    .withRegion("us-west-2")
    .build();

// Alibaba SMQ — publish directly to a queue
TopicClient aliQueueTopic = TopicClient.builder("alismqqueue")
    .withTopicName("my-queue")
    .withEndpoint(URI.create("https://1234567890123456.mns.cn-shanghai.aliyuncs.com"))
    .build();

// Alibaba SMQ — publish to a topic that fans out to a queue subscription
TopicClient aliTopic = TopicClient.builder("alismqtopic")
    .withTopicName("my-topic")
    .withEndpoint(URI.create("https://1234567890123456.mns.cn-shanghai.aliyuncs.com"))
    .build();
```

You can also configure advanced options:

```java
URI endpoint = URI.create("https://custom-endpoint.com");
URI proxy = URI.create("https://proxy.example.com");

topicClient = TopicClient.builder("gcp")
    .withTopicName("projects/my-project/topics/my-topic")
    .withEndpoint(endpoint)
    .withProxyEndpoint(proxy)
    .build();
```

### Subscription Client

```java
// GCP
SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
    .withSubscriptionName("projects/my-project/subscriptions/my-subscription")
    .build();

// AWS
SubscriptionClient awsSubscriptionClient = SubscriptionClient.builder("aws")
    .withSubscriptionName("my-queue")
    .withRegion("us-west-2")
    .build();

// Alibaba SMQ
SubscriptionClient aliSubscriptionClient = SubscriptionClient.builder("ali")
    .withSubscriptionName("my-queue")
    .withEndpoint(URI.create("https://1234567890123456.mns.cn-shanghai.aliyuncs.com"))
    .build();
```

You can also configure advanced options:

```java
URI endpoint = URI.create("https://custom-endpoint.com");
URI proxy = URI.create("https://proxy.example.com");

subscriptionClient = SubscriptionClient.builder("gcp")
    .withSubscriptionName("projects/my-project/subscriptions/my-subscription")
    .withEndpoint(endpoint)
    .withProxyEndpoint(proxy)
    .build();
```

To delay redelivery whenever a message is nacked, set a default nack visibility timeout on the builder:

```java
SubscriptionClient subscriptionClient = SubscriptionClient.builder("aws")
    .withSubscriptionName("my-queue")
    .withRegion("us-west-2")
    .withNackVisibilityTimeout(Duration.ofSeconds(30))
    .build();
```

`Duration.ZERO` (the default) makes nacked messages immediately available for redelivery; a positive value delays redelivery by that amount.

---

## Sending Messages

### Basic Message

```java
try (TopicClient topic = topicClient) {
    
    Message message = Message.builder()
        .withBody("Hello, World!".getBytes())
        .build();
    
    topic.send(message);
}
```

### Message with Metadata

```java
Message message = Message.builder()
    .withBody("Order processed".getBytes())
    .withMetadata(Map.of(
        "order-id", "12345",
        "priority", "high",
        "source", "api-service"
    ))
    .build();

topic.send(message);
```

### Sending Multiple Messages

```java
List<Message> messages = List.of(
    Message.builder().withBody("Message 1".getBytes()).withMetadata(Map.of("batch-id", "1")).build(),
    Message.builder().withBody("Message 2".getBytes()).withMetadata(Map.of("batch-id", "2")).build(),
    Message.builder().withBody("Message 3".getBytes()).withMetadata(Map.of("batch-id", "3")).build()
);

for (Message message : messages) {
    topic.send(message);
}
```

---

## Receiving Messages

### Single Message Receive

The `receive()` method blocks until a message is available:

```java
try (SubscriptionClient subscription = subscriptionClient) {
    
    Message message = subscription.receive();
    String data = new String(message.getBody());
    System.out.println("Received: " + data);
    
    // Process the message...
    
    // Acknowledge successful processing
    subscription.sendAck(message.getAckID());
}
```

### Continuous Message Processing

```java
try (SubscriptionClient subscription = subscriptionClient) {
    
    while (true) {
        Message message = subscription.receive();
        try {
            String data = new String(message.getBody());
            // Process the message
            processMessage(data);
            
            // Acknowledge on success
            subscription.sendAck(message.getAckID());
        } catch (Exception e) {
            // Nack on failure for redelivery (if supported)
            if (subscription.canNack()) {
                subscription.sendNack(message.getAckID());
            }
        }
    }
}
```

---

## Message Acknowledgment

### Single Acknowledgment

```java
Message message = subscription.receive();
if (message != null && message.getAckID() != null) {
    // Process the message...
    
    // Acknowledge successful processing
    subscription.sendAck(message.getAckID());
}
```

### Batch Acknowledgment

Batch acknowledgment returns a `CompletableFuture`:

```java
List<AckID> ackIDs = new ArrayList<>();

// Collect messages
for (int i = 0; i < 10; i++) {
    Message message = subscription.receive();
    if (message != null && message.getAckID() != null) {
        // Process message...
        ackIDs.add(message.getAckID());
    }
}

// Acknowledge all at once
subscription.sendAcks(ackIDs).join();
```

### Negative Acknowledgment (Nack)

Use nack to reject a message and make it available for redelivery. Check if nacking is supported first:

```java
Message message = subscription.receive();
if (message != null) {
    try {
        // Try to process
        processMessage(message);
        subscription.sendAck(message.getAckID());
    } catch (Exception e) {
        // Failed to process, nack for redelivery (if supported)
        if (subscription.canNack()) {
            subscription.sendNack(message.getAckID());
        } else {
            // Provider doesn't support nack
            System.err.println("Nack not supported");
        }
    }
}
```

### Nack with a Custom Visibility Timeout

Override the subscription's default nack visibility timeout for an individual message. This requests a specific redelivery delay without reconfiguring the subscription:

```java
Message message = subscription.receive();
if (message != null && subscription.canNack()) {
    // Delay redelivery of this message by 60 seconds
    subscription.sendNack(message.getAckID(), Duration.ofSeconds(60));
}
```

### Batch Negative Acknowledgment

```java
List<AckID> nackIDs = new ArrayList<>();

for (Message message : messages) {
    if (shouldReject(message)) {
        nackIDs.add(message.getAckID());
    }
}

if (subscription.canNack()) {
    subscription.sendNacks(nackIDs).join();
}
```

You can also apply a visibility timeout to an entire nack batch:

```java
if (subscription.canNack()) {
    // Delay redelivery of all nacked messages by 60 seconds
    subscription.sendNacks(nackIDs, Duration.ofSeconds(60)).join();
}
```

Passing `null` for the timeout uses the subscription's default nack visibility timeout.

---

## Subscription Information

### Check Nack Support

Not all providers support negative acknowledgment. Check before using:

```java
if (subscription.canNack()) {
    System.out.println("This subscription supports nacking");
} else {
    System.out.println("This subscription does not support nacking");
}
```

### Get Subscription Attributes

Retrieve common subscription metadata via `getAttributes()`, which returns a `GetAttributeResult`:

```java
GetAttributeResult attributes = subscription.getAttributes();
System.out.println("Subscription name: " + attributes.getName());
System.out.println("Bound topic: " + attributes.getTopic());
```

---

## Error Handling

### Null Check for AckID

Always validate that an AckID is not null before acknowledging:

```java
Message message = subscription.receive();
if (message != null && message.getAckID() != null) {
    subscription.sendAck(message.getAckID());
}
```

Attempting to acknowledge with a null AckID will throw `InvalidArgumentException`:

```java
try {
    subscription.sendAck(null);
} catch (InvalidArgumentException e) {
    System.err.println("Cannot acknowledge null AckID");
}
```

### Retryable Errors

Use `isRetryable()` to decide whether a failed operation is worth retrying or should be treated as a permanent failure:

```java
try {
    Message message = subscription.receive();
    subscription.sendAck(message.getAckID());
} catch (SubstrateSdkException e) {
    if (subscription.isRetryable(e)) {
        // Transient error - safe to retry
    } else {
        // Permanent failure - handle accordingly
    }
}
```

### Exception Handling

All topic operations may throw `SubstrateSdkException`:

```java
try {
    topic.send(message);
} catch (SubstrateSdkException e) {
    // Handle access denied, quota exceeded, network errors, etc.
    e.printStackTrace();
}
```
All subscription operations may throw `SubstrateSdkException`:

```java
try {
    Message message = subscription.receive();
    subscription.sendAck(message.getAckID());
} catch (SubstrateSdkException e) {
    // Handle access denied, network errors, etc.
    e.printStackTrace();
}
```
