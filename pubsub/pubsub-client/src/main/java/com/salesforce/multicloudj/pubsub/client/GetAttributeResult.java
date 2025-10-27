package com.salesforce.multicloudj.pubsub.client;


/**
 * Result object containing subscription attributes that are common across all Pub/Sub providers.
 * This class provides a standardized way to access subscription configuration properties
 * regardless of the underlying cloud provider (AWS, GCP, Aliyun, etc.).
 */
public class GetAttributeResult {

    // Core Identification
    private String name;
    private String topic;

    // Default constructor
    public GetAttributeResult() {}

    // Builder pattern for easy construction
    public static class Builder {
        private final GetAttributeResult result = new GetAttributeResult();

        public Builder name(String name) {
            result.name = name;
            return this;
        }

        public Builder topic(String topic) {
            result.topic = topic;
            return this;
        }

        public GetAttributeResult build() {
            return result;
        }
    }

    // Getters
    public String getName() { return name; }
    public String getTopic() { return topic; }

    // Setters
    public void setName(String name) { this.name = name; }
    public void setTopic(String topic) { this.topic = topic; }

}