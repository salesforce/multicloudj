---
layout: default
title: How to Docstore
nav_order: 3
parent: Usage Guides
---
# Docstore

The `DocStoreClient` class in the `multicloudj` library provides a portable abstraction over document store providers like Amazon DynamoDB, Alibaba Tablestore, and Google Firestore. It supports core document operations like create, read, update, delete (CRUD), batching, and querying.

## Overview

Document stores persist semi-structured, schemaless documents, typically as key-value pairs. The `DocStoreClient` lets you interact with multiple providers through a unified API.

Each provider is implemented via a driver extending `AbstractDocStore`.

## Creating a Client

To begin using `DocStoreClient`, use the static builder:

```java
CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
        .withTableName("chameleon-test")
        .withPartitionKey("pName")
        .withSortKey("s")
        .withRevisionField("docRevision")
        .build();

DocStoreClient client  = DocStoreClient.builder("aws")
        .withRegion("us-west-2")
        .withCollectionOptions(collectionOptions)
        .build();
```

## Document Representation

The `Document` class accepts either a user-defined class or a generic map.

### Option 1: Using a POJO (Player)

```java
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Player {
    private String pName;
    private int i;
    private float f;
    private boolean b;
    private String s;
}

Player player = new Player("Alice", 42, 99.5f, true, "metadata");
Document doc = new Document(player);
```

### Option 2: Using a HashMap

```java
Map<String, Object> map = new HashMap<>();
map.put("pName", "Alice");
map.put("i", 42);
map.put("f", 99.5f);
map.put("b", true);
map.put("s", "metadata");

Document doc = new Document(map);
```

## Actions

Once you have initialized a docstore client, you can call action methods on it to read, modify, and write documents. These are referred to as **actions**, and can be executed individually or as part of a batch using an action list.

DocStore supports the following types of actions:

- **Get** retrieves a document.
- **Create** creates a new document.
- **Replace** replaces an existing document.
- **Put** puts a document whether or not it already exists.
- **Update** applies modifications to a document (not supported yet).
- **Delete** deletes a document.

Each of the following examples illustrates one of these actions.

## Basic Operations

### Create
Create will throw an exception `ResourceAlreadyExists` if the document already exists.

```java
client.create(doc);
```

### Get

To retrieve a document, you must provide a `Document` initialized with the corresponding object and pre-populate the fields that uniquely identify it (e.g., partition key and sort key):

```java
Player player = new Player();
player.setPName("Alice"); // Assuming pName is the partition key
player.setS("metadata");   // Assuming s is the sort key

client.get(new Document(player));
```

With optional fields you want to retrieve:

```java
client.get(new Document(player), "pName", "f");
```

### Replace
Replaces the existing doc, will throw `ResourceNotFound` is the document doesn't exist.

```java
client.replace(doc);
```

### Put
Put is similar to create but will not throw in case the document doesn't exist.

```java
client.put(doc);
```

### Delete

To delete a document, the input must also have the required key fields populated:

```java
Player player = new Player();
player.setPName("Alice");
player.setS("metadata");

client.delete(new Document(player));
```

### Update (Not Supported)

```java
client.update(doc, Map.of("f", 120.0f)); // Throws UnSupportedOperationException
```

## Batch Operations

### Batch Get

```java
List<Document> docs = List.of(
    new Document().put("pName", "Alice").put("s", "metadata"),
    new Document().put("pName", "Bob").put("s", "stats")
);
client.batchGet(docs);
```

### Batch Put

```java
List<Document> docs = List.of(
    new Document().put("pName", "Alice").put("f", 10.5f),
    new Document().put("pName", "Bob").put("f", 20.0f)
);
client.batchPut(docs);
```

## Queries

DocStore’s `get` action retrieves a single document by its primary key. However, when you need to retrieve or manipulate multiple documents that match a condition, you can use queries.

Queries allow you to:
- Retrieve all documents that match specific conditions.
- Delete or update documents in bulk based on criteria.

The query interface is chainable and supports filtering and sorting (depending on driver support).

DocStore can also optimize queries automatically. Based on your filter conditions, it attempts to determine whether a global secondary index (GSI) or a local secondary index (LSI) can be used to execute the query more efficiently. This helps reduce latency and improves performance.

Queries support the following methods:

- **Where**: Describes a condition on a document. You can ask whether a field is equal to, greater than, or less than a value. The “not equals” comparison isn’t supported, because it isn’t portable across providers.
- **OrderBy**: Specifies the order of the resulting documents, by field and direction. For portability, you can specify at most one OrderBy, and its field must also be mentioned in a Where clause.
- **Limit**: Limits the number of documents in the result.

```java
Query query = client.query();
// Apply filtering, sorting, etc.
```

(Depends on driver implementation.)

## Advanced Usage

### Action Lists

```java
ActionList actions = client.getActions();
actions.put(doc1).get(doc2).delete(doc3);
actions.run();
```

You can also chain operations directly using the fluent API with `enableAtomicWrites()` for atomic execution:

```java
client.getActions()
    .create(new Document(new Player("Alice", 1, 3.99f, true, "CA")))
    .create(new Document(new Player("Bob", 2, 3.99f, true, "PT")))
    .create(new Document(new Player("Carol", 3, 3.99f, true, "PA")))
    .enableAtomicWrites()
    .create(new Document(new Player("Dave", 4, 3.99f, true, "TX")))
    .create(new Document(new Player("Eve", 5, 3.99f, true, "OR")))
    .create(new Document(new Player("Frank", 6, 3.99f, true, "NJ")))
    .run();
```
### Atomic Writes:
If you want to write your documents atomically, just use the `enableAtomicWrites` as above
and all the writes are this will be executed atomically.

### Close the Client

```java
client.close();
```