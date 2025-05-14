package com.salesforce.multicloud.sts.gcp;

import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.salesforce.multicloudj.sts.gcp.GcpSts;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GcpStsTest {

    private static IamCredentialsClient mockStsClient;

    @BeforeAll
    public static void setUp() {
        mockStsClient = Mockito.mock(IamCredentialsClient.class);
        GenerateAccessTokenResponse mockAccessTokenResponse = Mockito.mock(GenerateAccessTokenResponse.class);
        Mockito.when(mockStsClient.generateAccessToken(Mockito.any(GenerateAccessTokenRequest.class))).thenReturn(mockAccessTokenResponse);
        Mockito.when(mockAccessTokenResponse.getAccessToken()).thenReturn("testAccessToken");
    }

    @Test
    public void TestGcpStsInitialization() {
        GcpSts sts = new GcpSts();
        Assertions.assertEquals("gcp", sts.getProviderId());
    }

    @Test
    public void TestAssumedRoleSts() {
        GcpSts sts = new GcpSts().builder().build(mockStsClient);
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder().withRole("testRole").withSessionName("testSession").build();
        StsCredentials credentials = sts.assumeRole(request);
        Assertions.assertEquals(null, credentials.getAccessKeyId());
        Assertions.assertEquals(null, credentials.getAccessKeySecret());
        Assertions.assertEquals("testAccessToken", credentials.getSecurityToken());
    }
}
