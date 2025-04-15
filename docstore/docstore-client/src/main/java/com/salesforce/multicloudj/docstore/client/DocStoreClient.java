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

public class DocStoreClient {
    protected AbstractDocStore docStore;

    protected DocStoreClient(AbstractDocStore docStore) {
        this.docStore = docStore;
    }

    public static DocStoreClientBuilder builder(String providerId) {
        return new DocStoreClientBuilder(providerId);
    }

    public static class DocStoreClientBuilder {

        private final AbstractDocStore.Builder<?, ?> docStoreBuilder;

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

    public void create(Document document) {
        docStore.getActions().create(document).run();
    }

    public void replace(Document document) {
        docStore.getActions().replace(document).run();
    }

    public void put(Document document) {
        docStore.getActions().put(document).run();
    }

    public void delete(Document document) {
        docStore.getActions().delete(document).run();
    }

    // The document input should have key. The get result is stored in the supplied document.
    public void get(Document document, String... fieldPath) {
        docStore.getActions().get(document, fieldPath).run();
    }

    public void update(Document document, Map<String, Object> mods) {
        throw new UnSupportedOperationException("Update operation is not supported yet");
    }

    public void batchGet(List<Document> documents) {
        ActionList actionList = docStore.getActions();
        for (Document document: documents) {
            actionList.get(document);
        }
        actionList.run();
    }

    public void batchPut(List<Document> documents) {
        ActionList actionList = docStore.getActions();
        for (Document document: documents) {
            actionList.put(document);
        }
        actionList.run();
    }

    public void close() {
        docStore.close();
    }

    public ActionList getActions() {
        return docStore.getActions();
    }

    public Query query() {
        return new Query(docStore);
    }
}
