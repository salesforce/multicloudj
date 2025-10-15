package com.salesforce.multicloudj.common.gcp;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Test;

public class GcpCredentialsProviderTest {

    @Test
    void testGetCredentialsWithNullOverrider() {
        Credentials credentials = GcpCredentialsProvider.getCredentials(null);
        assertNull(credentials, "Credentials should be null when overrider is null");
    }

    @Test
    void testGetCredentialsWithNullType() {
        CredentialsOverrider overrider = new CredentialsOverrider.Builder(null).build();
        Credentials credentials = GcpCredentialsProvider.getCredentials(overrider);
        assertNull(credentials, "Credentials should be null when type is null");
    }

    @Test
    void testGetCredentialsWithSessionType() {
        // Arrange
        String testToken = "test-security-token-12345";
        StsCredentials stsCredentials = new StsCredentials(
                "test-access-key-id",
                "test-access-key-secret",
                testToken
        );
        
        CredentialsOverrider overrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        // Act
        Credentials credentials = GcpCredentialsProvider.getCredentials(overrider);

        // Assert
        assertNotNull(credentials, "Credentials should not be null");
        assertTrue(credentials instanceof GoogleCredentials,
                "Credentials should be GoogleCredentials");
    }

    @Test
    void testGetCredentialsWithSessionTypeAndNullSessionCredentials() {
        // Arrange
        CredentialsOverrider overrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .build();

        // Act & Assert - Should throw NullPointerException when sessionCredentials is null
        assertThrows(NullPointerException.class, () -> {
            GcpCredentialsProvider.getCredentials(overrider);
        }, "NullPointerException expected when sessionCredentials is null");
    }
}

