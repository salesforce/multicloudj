package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;

/**
 * Integration tests for proxy configuration overrides
 *
 * These tests verify that:
 * 1. useSystemPropertyProxyValues(false) correctly ignores invalide system property proxies
 * 2. useSystemPropertyProxyValues(true) picks up system property proxies (causing failure with invalid proxy)
 * 3. useEnvironmentVariableProxyValues(false) correctly ignores environment variable proxies
 * 4. Both AwsBlobStore (via BucketClient) and AwsBlobClient proxy override flags work end-to-end
 */
public class AwsProxyConfigIT {

    private static final String ENDPOINT = "https://s3.us-west-2.amazonaws.com";
    private static final String BUCKET_NAME = "chameleon-jcloud";
    private static final String REGION = "us-west-2";

    private String accessKeyId;
    private String secretAccessKey;
    private String sessionToken;

    @BeforeEach
    void setUp() {
        accessKeyId = getEnvWithFallback("ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
        secretAccessKey = getEnvWithFallback("SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "FAKE_SECRET_ACCESS_KEY");
        sessionToken = getEnvWithFallback("SESSION_TOKEN", "AWS_SESSION_TOKEN", "FAKE_SESSION_TOKEN");
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("http.proxyHost");
        System.clearProperty("http.proxyPort");
        System.clearProperty("https.proxyHost");
        System.clearProperty("https.proxyPort");
    }

    private static String getEnvWithFallback(String primary, String fallback, String defaultValue) {
        String value = System.getenv(primary);
        if (value == null || value.isEmpty()) {
            value = System.getenv(fallback);
        }
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    private CredentialsOverrider getCredentialsOverrider() {
        StsCredentials sessionCreds = new StsCredentials(accessKeyId, secretAccessKey, sessionToken);
        return new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(sessionCreds).build();
    }

    /**
     * Baseline: AwsBlobStore with useSystemPropertyProxyValues(false) can reach AWS.
     */
    @Test
    void testBlobStoreWithSystemPropertyProxyDisabled_happyPath() throws Exception {
        try (BucketClient client = BucketClient.builder("aws")
                .withEndpoint(URI.create(ENDPOINT))
                .withBucket(BUCKET_NAME)
                .withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseSystemPropertyProxyValues(false)
                .build()) {
            Assertions.assertTrue(client.doesBucketExist(), "Bucket should exist");
        }
    }

    /**
     * Baseline: AwsBlobStore with useEnvironmentVariableProxyValues(false) can reach AWS.
     */
    @Test
    void testBlobStoreWithEnvVarProxyDisabled_happyPath() throws Exception {
        try (BucketClient client = BucketClient.builder("aws")
                .withEndpoint(URI.create(ENDPOINT))
                .withBucket(BUCKET_NAME)
                .withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseEnvironmentVariableProxyValues(false)
                .build()) {
            Assertions.assertTrue(client.doesBucketExist(), "Bucket should exist");
        }
    }

    /**
     * Baseline: AwsBlobStore with both proxy flags set to false can reach AWS.
     */
    @Test
    void testBlobStoreWithBothProxyFlagsDisabled_happyPath() throws Exception {
        try (BucketClient client = BucketClient.builder("aws")
                .withEndpoint(URI.create(ENDPOINT))
                .withBucket(BUCKET_NAME)
                .withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseSystemPropertyProxyValues(false)
                .withUseEnvironmentVariableProxyValues(false)
                .build()) {
            Assertions.assertTrue(client.doesBucketExist(), "Bucket should exist");
        }
    }

    /**
     * Invalid system property proxy is set, but useSystemPropertyProxyValues(false)
     * causes it to be IGNORED. The doesBucketExist() call should succeed.
     */
    @Test
    void testBlobStoreWithInvalidSystemPropertyProxy_disabledIgnoresProxy() throws Exception {
        System.setProperty("http.proxyHost", "127.0.0.1");
        System.setProperty("http.proxyPort", "19999");
        System.setProperty("https.proxyHost", "127.0.0.1");
        System.setProperty("https.proxyPort", "19999");

        try (BucketClient client = BucketClient.builder("aws")
                .withBucket(BUCKET_NAME)
                .withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseSystemPropertyProxyValues(false)
                .build()) {
            Assertions.assertTrue(client.doesBucketExist(), "Bucket should exist even with invalid proxy set");
        }
    }

    /**
     * invalid system property proxy is set, and useSystemPropertyProxyValues(true)
     * causes it to be USED. The doesBucketExist() call should fail with a connection error.
     */
    @Test
    void testBlobStoreWithInvalidSystemPropertyProxy_enabledUsesProxy() throws Exception {
        System.setProperty("http.proxyHost", "127.0.0.1");
        System.setProperty("http.proxyPort", "19999");
        System.setProperty("https.proxyHost", "127.0.0.1");
        System.setProperty("https.proxyPort", "19999");

        try (BucketClient client = BucketClient.builder("aws")
                .withBucket(BUCKET_NAME)
                .withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseSystemPropertyProxyValues(true)
                .build()) {
            Assertions.assertThrows(Exception.class, client::doesBucketExist,
                    "Expected failure when invalid proxy is enabled via system properties");
        }
    }

    // ==================== AwsBlobClient Tests (direct builder) ====================

    /**
     * AwsBlobClient with useSystemPropertyProxyValues(false) can list buckets.
     */
    @Test
    void testBlobClientWithSystemPropertyProxyDisabled_happyPath() {
        AwsBlobClient.Builder builder = new AwsBlobClient.Builder();
        builder.withEndpoint(URI.create(ENDPOINT))
                .withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseSystemPropertyProxyValues(false);

        try (AwsBlobClient blobClient = builder.build()) {
            ListBucketsResponse response = blobClient.listBuckets();
            Assertions.assertNotNull(response);
            Assertions.assertFalse(response.getBucketInfoList().isEmpty());
        }
    }

    /**
     * AwsBlobClient with useEnvironmentVariableProxyValues(false) can list buckets.
     */
    @Test
    void testBlobClientWithEnvVarProxyDisabled_happyPath() {
        AwsBlobClient.Builder builder = new AwsBlobClient.Builder();
        builder.withEndpoint(URI.create(ENDPOINT))
                .withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseEnvironmentVariableProxyValues(false);

        try (AwsBlobClient blobClient = builder.build()) {
            ListBucketsResponse response = blobClient.listBuckets();
            Assertions.assertNotNull(response);
            Assertions.assertFalse(response.getBucketInfoList().isEmpty());
        }
    }

    /**
     * Invalid proxy set + useSystemPropertyProxyValues(false) → listBuckets succeeds.
     */
    @Test
    void testBlobClientWithInvalidSystemPropertyProxy_disabledIgnoresProxy() {
        System.setProperty("http.proxyHost", "127.0.0.1");
        System.setProperty("http.proxyPort", "19999");
        System.setProperty("https.proxyHost", "127.0.0.1");
        System.setProperty("https.proxyPort", "19999");

        AwsBlobClient.Builder builder = new AwsBlobClient.Builder();
        builder.withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseSystemPropertyProxyValues(false);

        try (AwsBlobClient blobClient = builder.build()) {
            Assertions.assertNotNull(blobClient.listBuckets());
        }
    }

    /**
     * Invalid proxy set + useSystemPropertyProxyValues(true) → listBuckets fails.
     */
    @Test
    void testBlobClientWithInvalidSystemPropertyProxy_enabledUsesProxy() {
        System.setProperty("http.proxyHost", "127.0.0.1");
        System.setProperty("http.proxyPort", "19999");
        System.setProperty("https.proxyHost", "127.0.0.1");
        System.setProperty("https.proxyPort", "19999");

        AwsBlobClient.Builder builder = new AwsBlobClient.Builder();
        builder.withRegion(REGION)
                .withCredentialsOverrider(getCredentialsOverrider())
                .withUseSystemPropertyProxyValues(true);

        try (AwsBlobClient blobClient = builder.build()) {
            Assertions.assertThrows(Exception.class, blobClient::listBuckets,
                    "Expected failure when invalid proxy is enabled via system properties");
        }
    }
}
