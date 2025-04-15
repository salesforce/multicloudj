package com.salesforce.multicloudj.sts.model;
import java.net.http.HttpRequest;

public class SignedAuthRequest {
    HttpRequest request;
    StsCredentials Credentials;

    public SignedAuthRequest(HttpRequest request, StsCredentials Credentials) {
        this.request = request;
        this.Credentials = Credentials;
    }

    public HttpRequest getRequest() {
        return request;
    }

    public StsCredentials getCredentials() {
        return Credentials;
    }

}