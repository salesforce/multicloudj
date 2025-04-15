package com.salesforce.multicloudj.sts.model;

public class GetAccessTokenRequest {

    private final Integer durationSeconds;

    private GetAccessTokenRequest(Builder b) {
        this.durationSeconds = b.durationSeconds;
    }

    public Integer getDuration() {
        return durationSeconds;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Integer durationSeconds;

        public Builder() {
        }

        public Builder withDurationSeconds(Integer duration) {
            this.durationSeconds = duration;
            return this;
        }

        public GetAccessTokenRequest build(){
            return new GetAccessTokenRequest(this);
        }
    }
}