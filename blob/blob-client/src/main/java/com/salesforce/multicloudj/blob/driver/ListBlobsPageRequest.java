package com.salesforce.multicloudj.blob.driver;

/**
 * Request object for paginated list operations
 */
public class ListBlobsPageRequest {

    private final String prefix;
    private final String delimiter;
    private final String paginationToken;
    private final Integer maxResults;

    private ListBlobsPageRequest(Builder builder) {
        this.prefix = builder.prefix;
        this.delimiter = builder.delimiter;
        this.paginationToken = builder.paginationToken;
        this.maxResults = builder.maxResults;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getPaginationToken() {
        return paginationToken;
    }

    public Integer getMaxResults() {
        return maxResults;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String prefix;
        private String delimiter;
        private String paginationToken;
        private Integer maxResults;

        public Builder withPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder withPaginationToken(String paginationToken) {
            this.paginationToken = paginationToken;
            return this;
        }

        public Builder withMaxResults(Integer maxResults) {
            this.maxResults = maxResults;
            return this;
        }

        public ListBlobsPageRequest build() {
            return new ListBlobsPageRequest(this);
        }
    }
} 