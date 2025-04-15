package com.salesforce.multicloudj.sts.model;


public class CallerIdentity {
    String userId;
    String cloudResourceName;
    String accountId;

    public CallerIdentity(String userId, String cloudResourceName, String accountId) {
        this.userId = userId;
        this.cloudResourceName = cloudResourceName;
        this.accountId = accountId;
    }

    public String getUserId() {
        return userId;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getCloudResourceName() {
        return cloudResourceName;
    }
}