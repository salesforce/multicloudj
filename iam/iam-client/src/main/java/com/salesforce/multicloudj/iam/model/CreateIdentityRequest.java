package com.salesforce.multicloudj.iam.model;

import lombok.Builder;
import lombok.Getter;

import java.util.Optional;

@Getter
@Builder
public class CreateIdentityRequest {
    private final String identityName;
    private final String description;
    private final String tenantId;
    private final String region;
    private final Optional<TrustConfiguration> trustConfig;
    private final Optional<CreateOptions> options;
}
