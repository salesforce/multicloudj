package com.salesforce.multicloudj.sts.model;

import java.net.http.HttpRequest;
import lombok.Getter;

@Getter
public class SignedAuthRequest {
  HttpRequest request;
  StsCredentials credentials;
  String signedIdentity;

  public SignedAuthRequest(HttpRequest request, StsCredentials credentials) {
    this(request, credentials, null);
  }

  /**
   * Constructs a SignedAuthRequest with a signed identity.
   *
   * @param request the signed STS request
   * @param credentials the credentials used to sign the request
   * @param signedIdentity a self-contained, replayable representation of the signed identity, or
   *     null when the provider does not produce one
   */
  public SignedAuthRequest(
      HttpRequest request, StsCredentials credentials, String signedIdentity) {
    this.request = request;
    this.credentials = credentials;
    this.signedIdentity = signedIdentity;
  }

}
