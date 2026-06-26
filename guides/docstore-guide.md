---
layout: default
title: How to Docstore
nav_order: 3
parent: Usage Guides
---
# Docstore

The `DocStoreClient` class in the `multicloudj` library provides a portable document-store-like abstraction over NoSQL database providers like Amazon DynamoDB, Alibaba Tablestore, and Google Firestore. It supports core document operations like create, read, update, delete (CRUD), batching, and querying with the support of indexing.


Internally, each provider is implemented via a driver extending `AbstractDocStore`.

### Provider IDs

| Provider | Provider ID |
|----------|-------------|
| AWS DynamoDB | `aws` |
| GCP Firestore | `gcp-firestore` |
| Alibaba Tablestore | `ali` |

> A GCP Spanner codec module (`docstore-gcp-spanner`) exists for value encoding/decoding, but it is not registered as a runnable docstore backend — there is no Spanner provider ID. Use `gcp-firestore` for GCP document storage.

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|--------------|---------------|--------------|----------------|----------|
| **Create Document** | ✅ Supported | ✅ Supported | ✅ Supported | Insert new documents |
| **Get Document** | ✅ Supported | ✅ Supported | ✅ Supported | Get the document by key |
| **Put Document** | ✅ Supported | ✅ Supported | ✅ Supported | Insert or replace document |
| **Replace Document** | ✅ Supported | ✅ Supported | ✅ Supported | Replace existing document |
| **Delete Document** | ✅ Supported | ✅ Supported | ✅ Supported | Remove document by key |
| **Update Document** | ⏱️ Coming Soon | ⏱️ Coming Soon | ⏱️ Coming Soon | Update operations not yet implemented in any provider |

### Batch Operations

| Feature Name | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|--------------|---------------|--------------|----------------|----------|
| **Batch Get** | ✅ Supported | ✅ Supported | ✅ Supported | Retrieve multiple documents in one call |
| **Batch Write** | ✅ Supported | ✅ Supported | ✅ Supported | Write multiple documents atomically |
| **Atomic Writes** | ✅ Supported | ✅ Supported | ✅ Supported | Atomic write operations across multiple documents |

### Query Features

| Feature Name              | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments                  |
|---------------------------|---------------|--------------|----------------|---------------------------|
| **Basic Queries**         | ✅ Supported | ✅ Supported | ✅ Supported | Filter and projection queries |
| **Compound Filters**      | ✅ Supported | ✅ Supported | ✅ Supported | Multiple filter conditions |
| **Order By**              | ✅ Supported | ✅ Supported | ✅ Supported | Sort query results        |
| **Order By in Full Scan** | ❌ **Not Supported** | ❌ **Not Supported** | ❌ **Not Supported** | ** It's too expensive **  |
| **Pagination Token**      | ✅ Supported | ✅ Supported | ✅ Supported | Query with pagination     |
| **Limit/Offset**          | ✅ Supported | ✅ Supported | ✅ Supported | Pagination support        |
| **Index-based Queries**   | ✅ Supported | ✅ Supported | ✅ Supported | Query using secondary indexes |
| **Query Planning**        | ✅ Supported | ✅ Supported | ✅ Supported | Explain query execution plans |

### Advanced Features

| Feature Name | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|--------------|---------------|--------------|----------------|----------|
| **Revision/Versioning** | ✅ Supported | ✅ Supported | ✅ Supported | Optimistic concurrency control |
| **Single Key Collections** | ✅ Supported | ✅ Supported | ✅ Supported | Collections with only partition key |
| **Two Key Collections** | ✅ Supported | ✅ Supported | ✅ Supported | Collections with partition + sort key(uses indexes in firestore) |

### Configuration Options

| Configuration | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|---------------|---------------|--------------|----------------|----------|
| **Regional Support** | ✅ Supported | ✅ Supported | ✅ Supported | Region-specific operations |
| **Custom Endpoints** | ✅ Supported | ✅ Supported | ✅ Supported | Override default service endpoints |
| **Credentials Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom credential providers via STS |
| **Collection Options** | ✅ Supported | ✅ Supported | ✅ Supported | Table/collection configuration |

### Important Notes about semantics:
1. If you are using in-equality filters (<, >, <=, >=) in query, make sure to put an order by on the same fields to get the consistent results. This is the limitation from gcp firestore ([ref](https://firebase.google.com/docs/firestore/query-data/order-limit-data)).
2. Nested Maps, List types are not supported in Alibaba. If your service is targeting alibaba as well, please consider serializing it yourself for this use-case.
3. Atomic writes doesn't support global transaction (across partition keys) across alibaba. 

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

A `Document` wraps either a POJO or a `Map<String, Object>`. The batch examples below use maps for brevity.

### Batch Get

```java
List<Document> docs = List.of(
    new Document(Map.of("pName", "Alice", "s", "metadata")),
    new Document(Map.of("pName", "Bob", "s", "stats"))
);
client.batchGet(docs);
```

### Batch Put

```java
List<Document> docs = List.of(
    new Document(Map.of("pName", "Alice", "f", 10.5f)),
    new Document(Map.of("pName", "Bob", "f", 20.0f))
);
client.batchPut(docs);
```

## Queries

DocStore's `get` action retrieves a single document by its primary key. However, when you need to retrieve multiple documents that match a condition, you can use queries.

A query is built fluently from `client.query()` and executed with `get(...)`, which returns a `DocumentIterator` for streaming the matching documents. Queries are read-only retrieval — there is no bulk update or bulk delete via the query API.

DocStore can also optimize queries automatically. Based on your filter conditions, it attempts to determine whether a global secondary index (GSI) or a local secondary index (LSI) can be used to execute the query more efficiently. This helps reduce latency and improves performance.

Queries support the following methods:

- **where(fieldPath, op, value)**: Describes a condition on a document. The supported `FilterOperation` values are `EQUAL`, `GREATER_THAN`, `LESS_THAN`, `GREATER_THAN_OR_EQUAL_TO`, `LESS_THAN_OR_EQUAL_TO`, `IN`, and `NOT_IN`. A "not equals" comparison isn't supported because it isn't portable across providers.
- **orderBy(fieldName, orderAscending)**: Specifies the order of the resulting documents. For portability, you can specify at most one `orderBy`, and its field must also be mentioned in a `where` clause.
- **limit(n)**: Limits the number of documents in the result.
- **offset(n)**: Skips the first `n` matching documents.
- **paginationToken(token)**: Resumes a query from a previous page.

```java
import com.salesforce.multicloudj.docstore.driver.FilterOperation;

DocumentIterator it = client.query()
    .where("i", FilterOperation.GREATER_THAN_OR_EQUAL_TO, 10)
    .orderBy("i", true)
    .limit(50)
    .get();

while (it.hasNext()) {
    Document result = new Document(new Player());
    it.next(result);
    // use result...
}

// Resume from where this page ended
PaginationToken nextPage = it.getPaginationToken();
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
### Atomic Writes:
If you want to write your documents atomically, just use the `enableAtomicWrites` as above
and all the writes are this will be executed atomically.

### Close the Client

```java
client.close();
```