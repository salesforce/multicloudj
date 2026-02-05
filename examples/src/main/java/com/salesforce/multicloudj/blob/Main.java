package com.salesforce.multicloudj.blob;

import com.google.api.client.json.webtoken.JsonWebSignature;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.sts.client.StsClient;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.zip.CRC32;

public class Main {
    static BucketClient bucketClient;

    /**
     * Demonstrates a full multipart upload lifecycle: initiate, upload parts, complete.
     */
    public static void fullMultipartUploadExample() {
        if (bucketClient == null) {
            System.out.println("Bucket Client is null");
            bucketClient = getBucketClient(getProvider());
        }
        String key = "multipart-example-" + System.currentTimeMillis();

        getLogger().info("Starting full multipart upload example for key: {}", key);

        // 1. Initiate
        MultipartUploadRequest request = new MultipartUploadRequest.Builder()
                .withKey(key)
                .withMetadata(Map.of("type", "multipart-example"))
                .build();

        MultipartUpload upload = bucketClient.initiateMultipartUpload(request);
        getLogger().info("Initiated multipart upload: id={}", upload.getId());

        try {
            List<UploadPartResponse> partResponses = new ArrayList<>();

            // 2. Upload Parts (Example: 2 parts, 5MB each to meet minimum part size requirements for some providers)
            int partSize = 5 * 1024 * 1024; // 5MB
            byte[] partData = new byte[partSize];

            for (int i = 0; i < 2; i++) {
                int partNumber = i + 1;
                // Fill with some data
                Arrays.fill(partData, (byte) ('0' + i));

                MultipartPart part = new MultipartPart(partNumber, partData);

                getLogger().info("Uploading part {} ({} bytes)...", partNumber, partSize);
                UploadPartResponse response = bucketClient.uploadMultipartPart(upload, part);
                partResponses.add(response);
                getLogger().info("Uploaded part {}: etag={}", partNumber, response.getEtag());
            }

            // 3. Complete
            getLogger().info("Completing multipart upload...");
            MultipartUploadResponse response = bucketClient.completeMultipartUpload(upload, partResponses);
            getLogger().info("Completed multipart upload. Etag={}", response.getEtag());

        } catch (Exception e) {
            getLogger().error("Multipart upload failed, aborting...", e);
            try {
                bucketClient.abortMultipartUpload(upload);
                getLogger().info("Aborted multipart upload");
            } catch (Exception abortEx) {
                getLogger().error("Failed to abort multipart upload", abortEx);
            }
        }
    }

    private static BucketClient getBucketClient(String provider) {
        // Get configuration from environment variables or system properties
        String bucketName = System.getProperty("bucket.name", "chameleon-jcloud");
        String region = System.getProperty("bucket.region", System.getenv("BUCKET_REGION"));
        String endpoint = System.getProperty("bucket.endpoint", System.getenv("BUCKET_ENDPOINT"));
        String proxyEndpoint = System.getProperty("bucket.proxy.endpoint", System.getenv("BUCKET_PROXY_ENDPOINT"));

        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket name must be provided via 'bucket.name' system property or 'BUCKET_NAME' environment variable");
        }

        BucketClient.BlobBuilder builder = BucketClient.builder(provider)
                .withBucket(bucketName);

        if (region != null && !region.trim().isEmpty()) {
            builder.withRegion(region);
        }

        if (endpoint != null && !endpoint.trim().isEmpty()) {
            builder.withEndpoint(URI.create(endpoint));
        }

        if (proxyEndpoint != null && !proxyEndpoint.trim().isEmpty()) {
            builder.withProxyEndpoint(URI.create(proxyEndpoint));
        }

        Supplier<String> tokenSupplier = () -> {
            StsClient clientGcp = StsClient.builder("gcp").build();
            CallerIdentity identity = clientGcp.getCallerIdentity(GetCallerIdentityRequest.builder().aud("multicloudj").build());
            CRC32 crc = new CRC32();
            // Convert the string to bytes using a consistent character encoding (e.g., UTF-8)
            crc.update(identity.getCloudResourceName().getBytes(StandardCharsets.UTF_8));
            System.out.println("Checksum: " + crc.getValue());

            JsonWebSignature jws = null;
            try {
                jws = JsonWebSignature.parse(com.google.api.client.json.gson.GsonFactory.getDefaultInstance(), identity.getCloudResourceName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Long expirationTimeInSeconds = jws.getPayload().getExpirationTimeSeconds();
            System.out.println("Expiration: " + expirationTimeInSeconds);


            return identity.getCloudResourceName();
        };

        tokenSupplier.get();
        tokenSupplier.get();


        CredentialsOverrider overrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE_WEB_IDENTITY)
                .withRole("arn:aws:iam::654654370895:role/chameleon-web")
                .withWebIdentityTokenSupplier(tokenSupplier)
                .withDurationSeconds(900)
                .build();
        builder.withCredentialsOverrider(overrider);

        return builder.build();
    }

    private static String getProvider() {
        // Change this to test different providers
        return "aws";  // or "aws" or "ali"
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger("Main");
    }

    /**
     * Main method to test directory operations
     */
    public static void main(String[] args) {
        System.out.println("=== STARTING DIRECTORY OPERATIONS TEST ===");
        System.out.println("Provider: " + getProvider());
        try {
            while (true) {
                System.out.println("=== Testing Multipart Upload ===");
                fullMultipartUploadExample();
                System.out.println("Multipart upload test completed!");
                Thread.sleep(300000);
            }
        } catch (Exception e) {
            System.out.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }


    }
}