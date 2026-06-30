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

  // Optional: identifier of the web-identity (OIDC) provider to assume the role through. Some
  // substrates require the provider to be named explicitly on the request; others resolve it
  // implicitly from the token issuer and ignore this field.
  private final String webIdentityProviderArn;
}
