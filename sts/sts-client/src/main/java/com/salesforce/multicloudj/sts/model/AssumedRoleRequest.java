package com.salesforce.multicloudj.sts.model;

public class AssumedRoleRequest {

    private final String role;
    private final String sessionName;
    private final int expiration;
    private final Object accessBoundary;

    private AssumedRoleRequest(Builder b) {
        this.role = b.role;
        this.sessionName = b.sessionName;
        this.expiration = b.expiration;
        this.accessBoundary = b.accessBoundary;
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

    public Object getAccessBoundary() {
        return accessBoundary;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String role;
        private String sessionName;
        private int expiration;
        private Object accessBoundary;

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

        public Builder withAccessBoundary(Object accessBoundary) {
            this.accessBoundary = accessBoundary;
            return this;
        }

        public AssumedRoleRequest build(){
            return new AssumedRoleRequest(this);
        }
    }
}