package com.salesforce.multicloudj.sts.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;

import static org.mockito.Mockito.mock;

class SignedAuthRequestTest {
    private HttpRequest request;
    private StsCredentials credentials;

    @BeforeEach
    void setUp() {
        this.request = mock(HttpRequest.class);
        this.credentials = mock(StsCredentials.class);
    }

    @Test
    void getRequest() {
        SignedAuthRequest signedAuthRequest = new SignedAuthRequest(request, credentials);
        Assertions.assertEquals(request, signedAuthRequest.getRequest());
    }

    @Test
    void getCredentials() {
        SignedAuthRequest signedAuthRequest = new SignedAuthRequest(request, credentials);
        Assertions.assertEquals(credentials, signedAuthRequest.getCredentials());
    }
}