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

  // Identifier (ARN) of the web-identity (OIDC) provider to assume the role through. Each provider
  // decides whether to consume this or resolve the provider some other way.
  private final String webIdentityProviderArn;
}
