package com.salesforce.multicloudj.sts.model;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CredentialsOverriderTest {
    @Test
    public void TestCredentialsOverriderBuilderWithProvidedValues() {
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .withRole("some-role")
                .withDurationSeconds(60).build();
        Assertions.assertEquals(stsCredentials, credentialsOverrider.getSessionCredentials());
        StsCredentials actualStsCredentials = credentialsOverrider.getSessionCredentials();
        Assertions.assertEquals(stsCredentials.getAccessKeyId(), actualStsCredentials.getAccessKeyId());
        Assertions.assertEquals(stsCredentials.getAccessKeySecret(), actualStsCredentials.getAccessKeySecret());
        Assertions.assertEquals(stsCredentials.getSecurityToken(), actualStsCredentials.getSecurityToken());
        Assertions.assertEquals("some-role", credentialsOverrider.getRole());
        Assertions.assertEquals(60, credentialsOverrider.getDurationSeconds());
    }

    @Test
    public void TestCredentialsOverriderBuilderWithNoValues() {
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION).build();
        Assertions.assertNull(credentialsOverrider.getSessionCredentials());
        Assertions.assertNull(credentialsOverrider.getRole());
        Assertions.assertNull(credentialsOverrider.getDurationSeconds());
    }

    @Test
    public void TestCredentialsOverriderWithSessionName() {
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
                .withRole("test-role")
                .withSessionName("testSession")
                .build();
        Assertions.assertEquals("test-role", credentialsOverrider.getRole());
        Assertions.assertEquals("testSession", credentialsOverrider.getSessionName());
    }

    @Test
    public void TestCredentialsOverriderWithWebIdentityTokenSupplier() {
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
                .withRole("test-role")
                .withWebIdentityTokenSupplier(() -> "test-token")
                .build();
        Assertions.assertEquals("test-role", credentialsOverrider.getRole());
        Assertions.assertNotNull(credentialsOverrider.getWebIdentityTokenSupplier());
        Assertions.assertEquals("test-token", credentialsOverrider.getWebIdentityTokenSupplier().get());
    }

    @Test
    public void TestCredentialsOverriderWithWebIdentityTokenSupplierAndSessionName() {
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
                .withRole("test-role")
                .withWebIdentityTokenSupplier(() -> "test-token")
                .withSessionName("webSession")
                .withDurationSeconds(3600)
                .build();
        Assertions.assertEquals("test-role", credentialsOverrider.getRole());
        Assertions.assertNotNull(credentialsOverrider.getWebIdentityTokenSupplier());
        Assertions.assertEquals("test-token", credentialsOverrider.getWebIdentityTokenSupplier().get());
        Assertions.assertEquals("webSession", credentialsOverrider.getSessionName());
        Assertions.assertEquals(3600, credentialsOverrider.getDurationSeconds());
    }
}
