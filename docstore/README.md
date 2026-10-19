# DocStore read consistency

`CollectionOptions.withConsistentRead(...)` sets the client default. Use immutable
`ReadOptions` to override that setting for one read, without changing the client:

```java
import com.salesforce.multicloudj.docstore.driver.ReadOptions;

ReadOptions strong = ReadOptions.builder().consistentRead(true).build();
ReadOptions normal = ReadOptions.builder().consistentRead(false).build();

client.get(document, strong);
client.get(document, strong, "name", "version");
client.batchGet(documents, strong);
client.getActions().get(document, strong).run();

DocumentIterator results = client.query()
    .where("keyId", FilterOperation.EQUAL, keyId)
    .get(strong);
try {
  // Consume results.
} finally {
  results.stop();
}
```

Omitting `ReadOptions`, passing a typed `(ReadOptions) null`, or leaving
`consistentRead` unset inherits the client setting. Explicit `true` requires strong
consistency. Explicit `false` uses the provider's normal read behavior; it does not
force eventual consistency on providers that already read strongly. Different
reads in an action list can use different options. A query's options apply to all
pages fetched by its returned iterator. If a pagination token is used in another
query call, pass the desired options again on that call.

| Provider / read path | `consistentRead=true` | `consistentRead=false` |
| --- | --- | --- |
| AWS DynamoDB key reads, table/LSI queries, table scans | Strong reads | Eventual reads |
| AWS DynamoDB GSI queries | Rejected with `InvalidArgumentException` | Eventual reads |
| GCP Firestore key reads and queries | Native strong reads | Native strong reads |
| Alibaba Tablestore base-table reads and local-index queries | Native strong reads | Native strong reads |
| Alibaba Tablestore global-index queries | Rejected with `InvalidArgumentException` | Native eventual reads |

The SDK validates the index selected by query planning. It does not silently
downgrade a strong query or turn it into a more expensive scan to work around an
unsupported index. Strong reads cannot guarantee that a global index includes
just-created records. Applications that require that guarantee must use a
supported access path and data model.

**Compatibility:** the client consistency default also applies to queries and
scans. Clients configured with `true` previously applied that setting only to key
reads. Such clients now request strong query reads and reject unsupported global
index queries. Use per-call `false` for a query that intentionally accepts eventual
consistency.

Strong reads do not make batch reads, multi-page queries, or scans an atomic
snapshot. DynamoDB strong reads consume more read capacity, and regional
consistency does not add cross-region guarantees to eventually replicated global
tables.

Provider references: [DynamoDB read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html),
[Firestore query consistency](https://docs.cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents/runQuery),
and [Tablestore secondary-index consistency](https://www.alibabacloud.com/help/en/tablestore/table-store-service/overview-of-secondary-index/).
