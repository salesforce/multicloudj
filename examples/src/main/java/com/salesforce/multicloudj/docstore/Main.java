package com.salesforce.multicloudj.docstore;

import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
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
        private String docRevision;
    }

    public static void main(String[] args) {
        // Example of how a user would specify the provider
        String provider = "aws"; // or "ali"

        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("chameleon-test")
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

        Person person = new Person("Zoe", "Ford", 22);
        Book book = new Book("YellowBook", person, "WA", 3.99f, new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)), null);

        client.create(new Document(book));

        Book getBook = new Book("YellowBook", null, "WA", 0, null, null);
        client.get(new Document(getBook));
        System.out.println(getBook);

        Book getBookwithActions = new Book("YellowBook", null, "WA", 0, null, null);
        client.getActions().get(new Document(getBookwithActions), "author.lastName").run();
        System.out.println(getBookwithActions);

        getBook = new Book("YellowBook", null, "WA", 0, null, null);
        client.getActions().put(new Document(new Book("YellowBook", null, "WA", 1, null, null)))
                .create(new Document(new Book("YellowBook", null, "NY", 2, null, null)))
                .enableAtomicWrites()
                .create(new Document(new Book("YellowBook", null, "CA", 3, null, null)))
                .create(new Document(new Book("YellowBook", null, "TX", 4, null, null)))
                .get(new Document(getBook))
                .run();
        System.out.println(getBook);

        getBook.setPrice(getBook.getPrice() - 1);
        // The replace will update the doc for all the fields except the doc revision field.
        // If we need to also get back the doc revision field, we need to another get.
        client.getActions().replace(new Document(getBook)).get(new Document(getBook)).run();
        System.out.println(getBook);
        
        Book blueBook = new Book("BlueBook", person, "WA", 2.49f, new HashMap<>(Map.of("Chapter 5", 6, "Chapter 6", 9)), null);
        client.create(new Document(blueBook));

        // The below tests assume there is a global index (Hash key = price, Sort key = title).
        // Scan table
        DocumentIterator iter = client.query().where("price", FilterOperation.LESS_THAN, 6).get();
        Book scanBook = new Book();
        Document document = new Document(scanBook);
        iter.next(document);
        iter.next(document);
        System.out.println(scanBook);

        // Query tables
        DocumentIterator iter2 = client.query().where("title", FilterOperation.EQUAL, "YellowBook").where("publisher", FilterOperation.LESS_THAN, "WA").limit(2).get();
        Book queriedBook = new Book();
        document = new Document(queriedBook);
        while (iter2.hasNext()) {
            iter2.next(document);
            System.out.println("Query result from table:" + queriedBook);
        }

        // Continue from the offset
        iter2 = client.query().where("title", FilterOperation.EQUAL, "YellowBook").where("publisher", FilterOperation.LESS_THAN, "WA").limit(2).offset(2).get();
        while (iter2.hasNext()) {
            iter2.next(document);
            System.out.println("Query result from table:" + queriedBook);
        }

        // write with atomic writes
        client.getActions().create(new Document(new Book("YellowBook", person, "CA", 3.99f, null, null)))
                .create(new Document(new Book("YellowBook", person, "PT", 3.99f, null, null)))
                .create(new Document(new Book("YellowBook", person, "PA", 3.99f, null, null)))
                .enableAtomicWrites()
                .create(new Document(new Book("YellowBook", person, "TX", 3.99f, null, null)))
                .create(new Document(new Book("YellowBook", person, "OR", 3.99f, null, null)))
                .create(new Document(new Book("YellowBook", person, "NJ", 3.99f, null, null))).run();

        iter = client.query().where("title", FilterOperation.EQUAL, "YellowBook").offset(0).get();


        while (iter.hasNext()) {
            queriedBook = new Book();
            iter.next(new Document(queriedBook));
            System.out.println("Queried book: " + queriedBook);
        }

        client.close();

        Book deleteBook = getBook;
        client.getActions().delete(new Document(deleteBook)).run();
    }
}
