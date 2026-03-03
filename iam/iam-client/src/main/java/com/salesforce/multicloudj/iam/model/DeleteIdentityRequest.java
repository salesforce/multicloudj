package com.salesforce.multicloudj.iam.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DeleteIdentityRequest {
    private final String identityName;
    private final String tenantId;
    private final String region;
}
