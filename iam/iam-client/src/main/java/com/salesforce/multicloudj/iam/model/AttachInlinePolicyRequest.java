package com.salesforce.multicloudj.iam.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class AttachInlinePolicyRequest {
    private final PolicyDocument policyDocument;
    private final String tenantId;
    private final String region;
    private final String identityName;
}