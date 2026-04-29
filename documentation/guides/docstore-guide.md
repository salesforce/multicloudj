---
layout: default
title: How to Docstore
nav_order: 3
parent: Usage Guides
---
# Docstore

The `DocStoreClient` class in the `multicloudj` library provides a portable document store abstraction over NoSQL database providers like Amazon DynamoDB, Alibaba Tablestore, and Google Firestore. It supports core document operations like create, read, update, delete (CRUD), batching, and querying with the support of indexing.

Internally, each provider is implemented via a driver extending `AbstractDocStore`.

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|--------------|---------------|--------------|----------------|----------|
| **Create Document** | ✅ Supported | ✅ Supported | ✅ Supported | Insert new documents |
| **Get Document** | ✅ Supported | ✅ Supported | ✅ Supported | Get the document by key |
| **Put Document** | ✅ Supported | ✅ Supported | ✅ Supported | Insert or replace document |
| **Replace Document** | ✅ Supported | ✅ Supported | ✅ Supported | Replace existing document |
| **Delete Document** | ✅ Supported | ✅ Supported | ✅ Supported | Remove document by key |
| **Update Document** | ✅ Supported | ✅ Supported | ✅ Supported | Update specific fields of a document |

### Batch Operations

| Feature Name | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|--------------|---------------|--------------|----------------|----------|
| **Batch Get** | ✅ Supported | ✅ Supported | ✅ Supported | Retrieve multiple documents in one call |
| **Batch Write** | ✅ Supported | ✅ Supported | ✅ Supported | Write multiple documents atomically |
| **Atomic Writes** | ✅ Supported | ✅ Supported | ✅ Supported | Atomic write operations across multiple documents |

### Query Features

| Feature Name | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|--------------|---------------|--------------|----------------|----------|
| **Basic Queries** | ✅ Supported | ✅ Supported | ✅ Supported | Filter and projection queries |
| **Compound Filters** | ✅ Supported | ✅ Supported | ✅ Supported | Multiple filter conditions (AND logic) |
| **Order By** | ✅ Supported | ✅ Supported | ✅ Supported | Sort query results |
| **Order By in Full Scan** | ❌ Not Supported | ❌ Not Supported | ❌ Not Supported | Too expensive across providers |
| **Limit/Offset** | ✅ Supported | ✅ Supported | ✅ Supported | Pagination support |
| **Pagination Token** | ✅ Supported | ✅ Supported | ✅ Supported | Resume queries from a previous position |
| **Index-based Queries** | ✅ Supported | ✅ Supported | ✅ Supported | Query using secondary indexes |
| **Query Planning** | ✅ Supported | ✅ Supported | ✅ Supported | Explain query execution plans |
| **IN / NOT_IN Filters** | ✅ Supported | ✅ Supported | ✅ Supported | Membership testing with collections |

### Advanced Features

| Feature Name | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|--------------|---------------|--------------|----------------|----------|
| **Revision/Versioning** | ✅ Supported | ✅ Supported | ✅ Supported | Optimistic concurrency control |
| **Single Key Collections** | ✅ Supported | ✅ Supported | ✅ Supported | Collections with only partition key |
| **Two Key Collections** | ✅ Supported | ✅ Supported | ✅ Supported | Collections with partition + sort key (uses indexes in Firestore) |

### Configuration Options

| Configuration | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|---------------|---------------|--------------|----------------|----------|
| **Regional Support** | ✅ Supported | ✅ Supported | ✅ Supported | Region-specific operations |
| **Custom Endpoints** | ✅ Supported | ✅ Supported | ✅ Supported | Override default service endpoints |
| **Credentials Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom credential providers via STS |
| **Collection Options** | ✅ Supported | ✅ Supported | ✅ Supported | Table/collection configuration |
| **Instance ID** | ❌ Not Applicable | ❌ Not Applicable | ✅ Supported | Required for Alibaba Tablestore |


## Creating a Client

To begin using `DocStoreClient`, use the static builder:

```java
CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
        .withTableName("chameleon-test")
        .withPartitionKey("pName")
        .withSortKey("s")
        .withRevisionField("docRevision")
        .build();

DocStoreClient client = DocStoreClient.builder("aws")
        .withRegion("us-west-2")
        .withCollectionOptions(collectionOptions)
        .build();
```

For GCP Firestore, the provider ID is `"gcp-firestore"`:

```java
DocStoreClient client = DocStoreClient.builder("gcp-firestore")
        .withRegion("us-central1")
        .withCollectionOptions(collectionOptions)
        .withCredentialsOverrider(credentialsOverrider)
        .build();
```

For Alibaba Tablestore, use `withInstanceId` to specify the Tablestore instance:

```java
DocStoreClient client = DocStoreClient.builder("ali")
        .withRegion("cn-hangzhou")
        .withInstanceId("my-tablestore-instance")
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

The SDK supports the following standard types: `String`, `Number`, `Boolean`, `Date`, `List`, and `Map`. Note that `List` and `Map` are not supported for Alibaba Tablestore.

## Actions

Once you have initialized a docstore client, you can call action methods on it to read, modify, and write documents. These are referred to as **actions**, and can be executed individually or as part of a batch using an action list.

DocStore supports the following types of actions:

- **Get** retrieves a document.
- **Create** creates a new document.
- **Replace** replaces an existing document.
- **Put** puts a document whether or not it already exists.
- **Update** applies field-level modifications to a document.
- **Delete** deletes a document.

Each of the following examples illustrates one of these actions.

## Basic Operations

### Create
Create will throw a `ResourceAlreadyExists` exception if the document already exists.

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
Replaces the existing doc. Throws `ResourceNotFound` if the document doesn't exist.

```java
client.replace(doc);
```

### Put
Put is similar to replace but will not throw an exception if the document doesn't exist — it will create it.

```java
client.put(doc);
```

### Update
Updates specific fields of an existing document without replacing the entire document.

```java
client.update(doc, Map.of("f", 120.0f, "b", false));
```

### Delete

To delete a document, the input must have the required key fields populated:

```java
Player player = new Player();
player.setPName("Alice");
player.setS("metadata");

client.delete(new Document(player));
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

DocStore's `get` action retrieves a single document by its primary key. However, when you need to retrieve or manipulate multiple documents that match a condition, you can use queries.

Queries allow you to:
- Retrieve all documents that match specific conditions.
- Delete or update documents in bulk based on criteria.

The query interface is chainable and supports filtering and sorting across all providers.

DocStore can also optimize queries automatically. Based on your filter conditions, it attempts to determine whether a global secondary index (GSI) or a local secondary index (LSI) can be used to execute the query more efficiently.

### Filter Operations

| Operation | Description |
|-----------|-------------|
| `EQUAL` | Exact match for strings and numbers |
| `GREATER_THAN` | Strict greater-than comparison |
| `LESS_THAN` | Strict less-than comparison |
| `GREATER_THAN_OR_EQUAL_TO` | Inclusive greater-than comparison |
| `LESS_THAN_OR_EQUAL_TO` | Inclusive less-than comparison |
| `IN` | Field value is in a provided collection |
| `NOT_IN` | Field value is not in a provided collection |

> **Note:** The "not equals" comparison is not supported because it is not portable across providers.

### Query Methods

- **Where**: Adds a filter condition. Multiple `where` clauses are combined with AND logic.
- **OrderBy**: Specifies sort field and direction (`true` = ascending, `false` = descending). The order field must also appear in a `where` clause.
- **Limit**: Limits the number of results returned.
- **Offset**: Skips the first N results.
- **PaginationToken**: Resumes from a previous query's position.
- **Get**: Executes the query and returns a `DocumentIterator`.

### Basic Query Example

```java
DocumentIterator iter = client.query()
    .where("i", FilterOperation.GREATER_THAN, 10)
    .where("b", FilterOperation.EQUAL, true)
    .orderBy("i", true)   // ascending
    .limit(20)
    .get();

Player p = new Player();
while (iter.hasNext()) {
    iter.next(new Document(p));
    System.out.println(p);
}
```

### Pagination with Token

```java
// First page
DocumentIterator firstPage = client.query()
    .where("s", FilterOperation.EQUAL, "metadata")
    .limit(10)
    .get();

PaginationToken token = firstPage.getPaginationToken();

// Next page — use the token from the previous result
if (token != null) {
    DocumentIterator nextPage = client.query()
        .where("s", FilterOperation.EQUAL, "metadata")
        .limit(10)
        .paginationToken(token)
        .get();
}
```

### IN Filter Example

```java
DocumentIterator iter = client.query()
    .where("s", FilterOperation.IN, List.of("metadata", "stats", "profile"))
    .get();
```

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

### Atomic Writes

If you want to write your documents atomically, use `enableAtomicWrites()` as shown above — all writes after that call are executed atomically as a single transaction.

### Close the Client

```java
client.close();
```
