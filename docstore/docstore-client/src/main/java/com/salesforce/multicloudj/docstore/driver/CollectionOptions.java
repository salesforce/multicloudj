package com.salesforce.multicloudj.docstore.driver;

import lombok.Getter;

@Getter
public class CollectionOptions {
    private final String tableName;
    private final String partitionKey;
    private final String sortKey;
    private final boolean allowScans;
    private final String revisionField;
    private final int maxOutstandingActionRPCs;
    
    private CollectionOptions(CollectionOptionsBuilder builder) {
        this.tableName = builder.tableName;
        this.partitionKey = builder.partitionKey;
        this.sortKey = builder.sortKey;
        this.allowScans = builder.allowScans;
        this.revisionField = builder.revisionField;
        this.maxOutstandingActionRPCs = builder.maxOutstandingActionRPCs;
    }

    public static class CollectionOptionsBuilder {
        private String tableName = null;
        private String partitionKey = null;
        private String sortKey = null;
        private boolean allowScans = true;
        private String revisionField = null;
        private int maxOutstandingActionRPCs = 0;

        public CollectionOptionsBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public CollectionOptionsBuilder withPartitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public CollectionOptionsBuilder withSortKey(String sortKey) {
            this.sortKey = sortKey;
            return this;
        }

        public CollectionOptionsBuilder withAllowScans(boolean allowScans) {
            this.allowScans = allowScans;
            return this;
        }

        public CollectionOptionsBuilder withRevisionField(String revisionField) {
            this.revisionField = revisionField;
            return this;
        }

        public CollectionOptionsBuilder withMaxOutstandingActionRPCs(int maxOutstandingActionRPCs) {
            this.maxOutstandingActionRPCs = maxOutstandingActionRPCs;
            return this;
        }

        public CollectionOptions build() {
            return new CollectionOptions(this);
        }
    }
}
