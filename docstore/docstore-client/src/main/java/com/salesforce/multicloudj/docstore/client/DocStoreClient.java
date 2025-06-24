package com.salesforce.multicloudj.docstore.client;

import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * DocStoreClient provides a unified interface for document/kv store operations across multiple cloud providers.
 * 
 * <p>This client supports various document operations such as create, read, update, delete (CRUD) operations along with Query interface
 * on documents stored in cloud-based document databases like AWS DynamoDB, GCP Firestore, and Alibaba TableStore.
 * The implementations also supports secondary indexes behind the scenes and uses them to optimize the queries.
 * 
 * <p>Usage example:
 * <pre>
 * DocStoreClient client = DocStoreClient.builder("aws")
 *     .withRegion("us-west-2")
 *     .build();
 * Person p = new Person("John", "Doe", 30);
 * Document doc = new Document(p);
 * client.create(doc);
 * client.query().where("age", 30).run();
 * </pre>
 * 
 * The client supports input and output data in the form of Java POJOs and maps.
 * The standard types supported are String, Number, Boolean, Date, List and Map. List and Maps are not supported for Alibaba tablestore.
 * 
 * @since 0.1.0
 */
public class DocStoreClient {
    protected AbstractDocStore docStore;

    /**
     * Protected constructor for DocStoreClient.
     * Use the builder pattern to create instances.
     * 
     * @param docStore the underlying document store implementation
     */
    protected DocStoreClient(AbstractDocStore docStore) {
        this.docStore = docStore;
    }

    /**
     * Creates a new builder for DocStoreClient.
     * 
     * @param providerId the cloud provider identifier (e.g., "aws", "gcp-firestore", "ali")
     * @return a new DocStoreClientBuilder instance
     */
    public static DocStoreClientBuilder builder(String providerId) {
        return new DocStoreClientBuilder(providerId);
    }

    /**
     * Builder class for creating DocStoreClient instances with fluent configuration.
     */
    public static class DocStoreClientBuilder {

        private final AbstractDocStore.Builder<?, ?> docStoreBuilder;

        /**
         * Creates a new builder for the specified provider.
         * 
         * @param providerId the cloud provider identifier (e.g., "aws", "gcp-firestore", "ali")
         * @throws IllegalArgumentException if providerId is null or unsupported
         */
        public DocStoreClientBuilder(String providerId) {
            this.docStoreBuilder = ProviderSupplier.findProviderBuilder(providerId);
        }

        /**
         * Method to supply region
         * @param region Region
         * @return An instance of self
         */
        public DocStoreClientBuilder withRegion(String region) {
            this.docStoreBuilder.withRegion(region);
            return this;
        }

        /**
         * Sets the instance ID for the document store.
         * This is typically used for cloud providers that require specific instance identification.
         * 
         * @param instanceId the instance identifier
         * @return this builder instance for method chaining
         */
        public DocStoreClientBuilder withInstanceId(String instanceId) {
            this.docStoreBuilder.withInstanceId(instanceId);
            return this;
        }


        /**
         * Method to supply an endpoint override
         * @param endpoint The endpoint override
         * @return An instance of self
         */
        public DocStoreClientBuilder withEndpoint(URI endpoint) {
            this.docStoreBuilder.withEndpoint(endpoint);
            return this;
        }

        /**
         * Method to supply collectionOptions
         * @param collectionOptions collectionOptions
         * @return An instance of self
         */
        public DocStoreClientBuilder withCollectionOptions(CollectionOptions collectionOptions) {
            this.docStoreBuilder.withCollectionOptions(collectionOptions);
            return this;
        }

        /**
         * Method to supply credentialsOverrider
         * @param credentialsOverrider credentialsOverrider
         * @return An instance of self
         */
        public DocStoreClientBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.docStoreBuilder.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        /**
         * Builds and returns an instance of DocStoreClient.
         * @return An instance of DocStoreClient.
         */
        public DocStoreClient build() {
            return new DocStoreClient(docStoreBuilder.build());
        }
    }

    /**
     * Creates a new document in the document store.
     * This operation will fail if a document with the same key already exists.
     * 
     * @param document the document to create, must have a valid key
     */
    public void create(Document document) {
        docStore.getActions().create(document).run();
    }

    /**
     * Replaces an existing document in the document store.
     * This operation will fail if the document does not exist.
     * 
     * @param document the document to replace, must have a valid key
     */
    public void replace(Document document) {
        docStore.getActions().replace(document).run();
    }

    /**
     * Creates or updates a document in the document store.
     * This operation will create the document if it doesn't exist, or update it if it does.
     * 
     * @param document the document to put, must have a valid key
     */
    public void put(Document document) {
        docStore.getActions().put(document).run();
    }

    /**
     * Deletes a document from the document store.
     * This operation is idempotent - it will not fail if the document doesn't exist.
     * 
     * @param document the document to delete, must have a valid key
     */
    public void delete(Document document) {
        docStore.getActions().delete(document).run();
    }

    /**
     * Retrieves a document from the document store.
     * The document input should have a valid key. The retrieved data is stored in the supplied document.
     * 
     * @param document the document to retrieve, must have a valid key. The result will be stored in this document
     * @param fieldPath optional field paths to retrieve specific fields only. If not provided, all fields are retrieved
     */
    public void get(Document document, String... fieldPath) {
        docStore.getActions().get(document, fieldPath).run();
    }

    /**
     * Updates specific fields of an existing document in the document store.
     * This operation is not yet supported and will throw an UnSupportedOperationException.
     * 
     * @param document the document to update, must have a valid key
     * @param mods the field modifications to apply
     */
    public void update(Document document, Map<String, Object> mods) {
        throw new UnSupportedOperationException("Update operation is not supported yet");
    }

    /**
     * Retrieves multiple documents from the document store in a single batch operation.
     * This is more efficient than multiple individual get operations.
     * The retrieved data is stored in the supplied documents.
     * 
     * @param documents the list of documents to retrieve, each must have a valid key. Results will be stored in these documents
     */
    public void batchGet(List<Document> documents) {
        ActionList actionList = docStore.getActions();
        for (Document document: documents) {
            actionList.get(document);
        }
        actionList.run();
    }

    /**
     * Creates or updates multiple documents in the document store in a single batch operation.
     * This is more efficient than multiple individual put operations.
     * Each document will be created if it doesn't exist, or updated if it does.
     * 
     * @param documents the list of documents to put, each must have a valid key
     */
    public void batchPut(List<Document> documents) {
        ActionList actionList = docStore.getActions();
        for (Document document: documents) {
            actionList.put(document);
        }
        actionList.run();
    }

    /**
     * Closes the document store client and releases associated resources.
     * After calling this method, the client should not be used for further operations.
     */
    public void close() {
        docStore.close();
    }

    /**
     * Returns the underlying ActionList for advanced batch operations.
     * This allows for fine-grained control over batch operations and mixing different operation types.
     * 
     * @return the ActionList instance for building custom batch operations
     */
    public ActionList getActions() {
        return docStore.getActions();
    }

    /**
     * Creates a new Query instance for querying the document store.
     * This allows for complex queries with filters, sorting, and pagination.
     * 
     * @return a new Query instance for building and executing queries
     */
    public Query query() {
        return new Query(docStore);
    }
}
