package com.salesforce.multicloudj.sts.model;

import lombok.Getter;

@Getter
public class AssumedRoleRequest {

    private final String role;
    private final String sessionName;
    private final int expiration;
    private final CredentialScope credentialScope;

    private AssumedRoleRequest(Builder b) {
        this.role = b.role;
        this.sessionName = b.sessionName;
        this.expiration = b.expiration;
        this.credentialScope = b.credentialScope;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String role;
        private String sessionName;
        private int expiration;
        private CredentialScope credentialScope;

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

        public Builder withCredentialScope(CredentialScope credentialScope) {
            this.credentialScope = credentialScope;
            return this;
        }

        public AssumedRoleRequest build(){
            return new AssumedRoleRequest(this);
        }
    }
}