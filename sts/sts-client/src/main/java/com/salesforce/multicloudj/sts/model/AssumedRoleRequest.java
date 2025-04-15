package com.salesforce.multicloudj.sts.model;

public class AssumedRoleRequest {

    private final String role;
    private final String sessionName;
    private final int expiration;

    private AssumedRoleRequest(Builder b) {
        this.role = b.role;
        this.sessionName = b.sessionName;
        this.expiration = b.expiration;
    }

    public String getRole() {
        return role;
    }

    public String getSessionName() {
        return sessionName;
    }

    public int getExpiration() {
        return expiration;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String role;
        private String sessionName;
        private int expiration;

        public Builder() {
        }


        public Builder withRole(String role) {
            this.role = role;
            return this;
        }

        public Builder withSessionName(String sessionName) {
            this.sessionName = sessionName;
            return this;
        }

        public Builder withExpiration(int expiration) {
            this.expiration = expiration;
            return this;
        }

        public AssumedRoleRequest build(){
            return new AssumedRoleRequest(this);
        }
    }
}