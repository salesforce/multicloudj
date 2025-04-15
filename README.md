# MultiCloudJ

Write once, deploy to any cloud provider...

**Multi-substrate Java SDK providing unified and substrate-neutral interfaces for cloud services such as Security Token Service (STS), Blob Storage, Key-Value Store, and more.**

---

### Introduction

MultiCloudJ is a versatile and powerful Java SDK designed to abstract away complexities related to interacting with multiple cloud substrates. By providing substrate-neutral interfaces, developers can seamlessly integrate their Java applications with various cloud services including:

- **Security Token Service (STS)**
- **Blob Store**
- **Document Store**
- more to common...

MultiCloudJ simplifies multi-cloud compatibility, enabling consistent codebases and accelerating development for applications that needs to be deployed across different cloud platforms.

---

### Key Features

- **Unified Interfaces**: Write once, interact across multiple cloud providers without changing your application code.
- **Multi-Cloud Support**: Compatible with major cloud providers like AWS, GCP, Alibaba.
- **Uniform Semantics**: SDK provides the uniform semantics to the end user irrespective of the cloud provider.
- **Extensible Architecture**: Easily extend and integrate additional cloud services into the SDK.
- **Flexibility**: Easily override the default implementations and inject your own custom implementation in the env.

---

### Getting Started

#### Requirements

- Java 11 or higher
- Maven 3.8 or higher build automation

#### Installation

Include MultiCloudJ in your project by adding the dependency to your project's `pom.xml`:

```xml
<dependency>
    <groupId>com.salesforce.multicloudj</groupId>
    <artifactId>docstore</artifactId>
    <version>0.0.1</version>
</dependency>
```

#### Quick Example

Here's how you might use MultiCloudJ to interact with Docstore interface to create a document in AWS dynamo:

```java
import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class DocstoreExample {
    @AllArgsConstructor
    @Data
    @NoArgsConstructor
    static class Book {
        private String title;
        private String author;
        private String publisher;
        private float price;
        private Map<String, Integer> tableOfContents;
        private String docRevision;
    }
    
    public static void main(String[] args) {
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("chameleon-test")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withRevisionField("docRevision")
                .build();

        DocStoreClient client  = DocStoreClient.builder("aws")
                .withRegion("us-west-2")
                .withCollectionOptions(collectionOptions)
                .build();
        
        Book book = new Book("YellowBook", "Zoe", "WA", 3.99f, new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)), null);

        client.create(new Document(book));
    }
}
```

For more examples: please refer to [multicloudj-examples](https://github.com/salesforce/multicloudj-examples/src/main/java/com/salesforce/multicloudj) where we have detailed examples on blob store, docstore and sts.

---

### Building and Contributing

To build MultiCloudJ from source:

```bash
git clone https://www.github.com/salesforce/multicloudj.git
cd multicloudj
mvn clean install
```

We welcome contributions! Please review our [Contribution Guidelines](CONTRIBUTING.md).

---

### Documentation

Detailed documentation can be found on our [official documentation site](https://supreme-meme-1699qwr.pages.github.io/).

---

### Community

- **Issues and Bug Reports**: [Github Issues](https://www.github.com/salesforce/multicloudj/issues)
- **Discussion and Q&A**: [Discussions](https://www.github.com/salesforce/multicloudj/issues)

---

### License

MultiCloudJ is released under the [Apache License 2.0](LICENSE).
