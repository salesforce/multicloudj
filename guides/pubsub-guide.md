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

| Feature Name | GCP              | AWS | ALI | Comments |
|--------------|------------------|-----|-----|----------|
| **Send Messages** | ✅ Supported      | End of Nov '25 | 📅 In Roadmap | Send messages to topics |
| **Receive Messages** | ✅ Supported      | End of Nov '25 | 📅 In Roadmap | Pull messages from subscriptions |
| **Acknowledge Messages** | ✅ Supported      | End of Nov '25 | 📅 In Roadmap | Confirm message processing |
| **Batch Acknowledgment** | ✅ Supported | End of Nov '25 | 📅 In Roadmap | Acknowledge multiple messages at once |
| **Negative Acknowledgment** | ✅ Supported      | End of Nov '25 | 📅 In Roadmap | Reject messages for redelivery |

### Advanced Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Async Batch Acknowledgment** | ✅ Supported | End of Nov '25 | 📅 In Roadmap | CompletableFuture-based async ack/nack |
| **Double Acknowledgment Safety** | ✅ Supported | End of Nov '25 | 📅 In Roadmap | Safe to ack same message multiple times |

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Endpoint Override** | ✅ Supported | End of Nov '25 | 📅 In Roadmap | Custom endpoint configuration |
| **Proxy Support** | ✅ Supported | End of Nov '25 | 📅 In Roadmap | HTTP proxy configuration |
| **Credentials Override** | ✅ Supported | End of Nov '25 | 📅 In Roadmap | Custom credential providers via STS |

---

### Provider-Specific Notes

**GCP (Google Cloud Pub/Sub)**
- Topic names must use full resource format: `projects/{projectId}/topics/{topicId}`
- Subscription names must use full resource format: `projects/{projectId}/subscriptions/{subscriptionId}`

## Creating Clients

### Topic Client

```java
TopicClient topicClient = TopicClient.builder("gcp")
    .withTopicName("projects/my-project/topics/my-topic")
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
SubscriptionClient subscriptionClient = SubscriptionClient.builder("gcp")
    .withSubscriptionName("projects/my-project/subscriptions/my-subscription")
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

Retrieve provider-specific subscription metadata:

```java
Map<String, String> attributes = subscription.getAttributes();
for (Map.Entry<String, String> entry : attributes.entrySet()) {
    System.out.println(entry.getKey() + ": " + entry.getValue());
}
```

---

**Important GCP Format Requirements:**
- Topic names: `projects/{projectId}/topics/{topicId}`
- Subscription names: `projects/{projectId}/subscriptions/{subscriptionId}`

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