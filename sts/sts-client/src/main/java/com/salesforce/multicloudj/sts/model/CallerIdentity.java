package com.salesforce.multicloudj.sts.model;


import lombok.Getter;

@Getter
public class CallerIdentity {
    String userId;
    String cloudResourceName;
    String accountId;

    public CallerIdentity(String userId, String cloudResourceName, String accountId) {
        this.userId = userId;
        this.cloudResourceName = cloudResourceName;
        this.accountId = accountId;
    }
}