package com.salesforce.multicloudj.sts.model;

import java.net.http.HttpRequest;

public class SignedAuthRequest {
  HttpRequest request;
  StsCredentials credentials;

  public SignedAuthRequest(HttpRequest request, StsCredentials credentials) {
    this.request = request;
    this.credentials = credentials;
  }

  public HttpRequest getRequest() {
    return request;
  }

  public StsCredentials getCredentials() {
    return credentials;
  }
}
