package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This test is meant to verify that the ProviderSupplier can load each of the substrate implementation.
 * Extend this class in each of the substrate implementations.
 */
public abstract class ProviderSupplierIT {

    protected abstract String getSubstrate();

    @Test
    void testSyncProviderSupplier() {
        StsCredentials credentials = new StsCredentials("accessKeyId", "accessKeySecret", "securityToken");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(credentials)
                .build();
        BucketClient bucketClient = BucketClient.builder(getSubstrate())
                .withBucket("bucketName")
                .withRegion("regionName")
                .withCredentialsOverrider(credentialsOverrider)
                .build();
        assertNotNull(bucketClient);
    }
}
