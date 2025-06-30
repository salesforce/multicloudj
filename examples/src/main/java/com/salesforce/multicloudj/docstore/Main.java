package com.salesforce.multicloudj.docstore;

import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

public class Main {
    @AllArgsConstructor
    @Data
    @NoArgsConstructor
    static class Person {
        private String firstName;
        private String lastName;
        private int Age;
    }

    @AllArgsConstructor
    @Data
    @NoArgsConstructor
    static class Book {
        private String title;
        private Person author;
        private String publisher;
        private float price;
        private Map<String, Integer> tableOfContents;
        private Object docRevision;
    }

    public static void main(String[] args) {
        // Example of how a user would specify the provider
        String provider = "gcp-firestore"; // or "ali"

        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("projects/substrate-sdk-gcp-poc1/databases/(default)/documents/docstore-test-1")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withRevisionField("docRevision")
                .withAllowScans(true)
                .withRevisionField("docRevision")
                .build();

        DocStoreClient client  = DocStoreClient.builder(provider)
                .withRegion("us-west-2")
                .withCollectionOptions(collectionOptions)
                .build();



        Book getBook = new Book("YellowBook", null, "WA", 0, null, null);
        client.getActions().put(new Document(new Book("YellowBook", null, "WA", 1, null, null)))
                .put(new Document(new Book("YellowBook", null, "NY", 2, null, null)))
                .put(new Document(new Book("YellowBook", null, "CA", 3, null, null)))
                .put(new Document(new Book("YellowBook", null, "TX", 4, null, null)))
                .get(new Document(getBook))
                .run();

        // The below tests assume there is a global index (Hash key = price, Sort key = title).
        // Scan table
        DocumentIterator iter = client.query().where("price", FilterOperation.LESS_THAN, 6).get();
        Book scanBook = new Book();
        while (iter.hasNext()) {
            iter.next(new Document(scanBook));
            System.out.println(scanBook);
        }
        client.delete(new Document(new Book("YellowBook", null, "CA", 0, null, null)));

        client.close();
    }
}
