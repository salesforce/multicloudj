package com.salesforce.multicloudj.blob.driver;

/**
 * Wrapper object for list filters
 */
public class ListBlobsRequest {

    private final String prefix;
    private final String delimiter;

    private ListBlobsRequest(Builder builder) {
        this.prefix = builder.prefix;
        this.delimiter = builder.delimiter;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String prefix;
        private String delimiter;

        public Builder withPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public ListBlobsRequest build() {
            return new ListBlobsRequest(this);
        }
    }
}
