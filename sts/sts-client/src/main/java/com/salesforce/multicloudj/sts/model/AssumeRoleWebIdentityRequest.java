package com.salesforce.multicloudj.sts.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class AssumeRoleWebIdentityRequest {

    private final String role;
    private final String webIdentityToken;
    private final String sessionName;
    private final int expiration;
}
