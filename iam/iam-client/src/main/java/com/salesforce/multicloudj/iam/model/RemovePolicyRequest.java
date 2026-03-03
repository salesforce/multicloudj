package com.salesforce.multicloudj.iam.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RemovePolicyRequest {
    private final String identityName;
    private final String policyName;
    private final String tenantId;
    private final String region;
}
