package com.salesforce.multicloudj.sts.model;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class GetCallerIdentityRequest {
    // optional param to set the target audience for the identity token
    private String aud;
}