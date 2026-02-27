package com.salesforce.multicloudj.iam.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class GetIdentityRequest {
    private final String identityName;
    private final String tenantId;
    private final String region;
}
