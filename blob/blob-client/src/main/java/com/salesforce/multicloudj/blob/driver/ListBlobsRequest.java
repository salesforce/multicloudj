package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * Wrapper object for list filters
 */
@Getter
public class ListBlobsRequest {

    private final String prefix;
    private final String delimiter;

    private ListBlobsRequest(Builder builder) {
        this.prefix = builder.prefix;
        this.delimiter = builder.delimiter;
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
