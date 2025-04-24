package com.salesforce.multicloudj.blob.driver;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.salesforce.multicloudj.blob.driver.BlobStoreValidator.INVALID_TAGS_COLLECTION_MSG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class BlobStoreValidatorTest {

    private final BlobStoreValidator validator = new BlobStoreValidator();

    @Test
    void testValidateKey() {
        Arrays.asList(
                null,
                "",
                "    "
        ).forEach(input -> {
            var e = assertThrows(IllegalArgumentException.class, () -> validator.validateKey(input));
            assertEquals("Object name cannot be null or empty", e.getMessage());
        });

        // these should not throw an exception
        validator.validateKey("abc");
        validator.validateKey("some/path/to/file.txt");
        validator.validateKey("some-file.txt");
    }

    @Test
    void testValidateKeys() {
        var keys = Arrays.asList(
                "valid",
                "also-valid",
                "",
                "       ",
                "last-valid"
        );

        var e = assertThrows(IllegalArgumentException.class, () -> validator.validateKeys(keys));
        assertEquals("Object name cannot be null or empty", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> validator.validateKeys(null));
        assertEquals("Collection of object names cannot be null or empty", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> validator.validateKeys(List.of()));
        assertEquals("Collection of object names cannot be null or empty", e.getMessage());
    }

    @Test
    void testRequireNotBlank() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> validator.requireNotBlank(null, "Argument is null")
        );
        assertEquals("Argument is null", e.getMessage());

        e = assertThrows(
            IllegalArgumentException.class,
            () -> validator.requireNotBlank("         ", "Argument is empty")
        );
        assertEquals("Argument is empty", e.getMessage());

        validator.requireNotBlank("bucket-1", "Argument is null");
    }

    @Test
    void testRequireNotEmpty() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            validator.requireNotEmpty((Collection<String>) null, "Argument is null");
        });
        assertEquals("Argument is null", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> {
            validator.requireNotEmpty(new ArrayList<>(), "Argument is empty");
        });
        assertEquals("Argument is empty", e.getMessage());
    }

    @Test
    void testRequireNotEmptyMap() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            validator.requireNotEmpty((Map<String, String>) null, "Argument is null");
        });
        assertEquals("Argument is null", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> {
            validator.requireNotEmpty(new HashMap<>(), "Argument is empty");
        });
        assertEquals("Argument is empty", e.getMessage());
    }

    @Test
    void testRequireEqualsIgnoreCase() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            validator.requireEqualsIgnoreCase(null, "value2", "Arguments not equal");
        });
        assertEquals("Arguments not equal", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> {
            validator.requireEqualsIgnoreCase("value1", "    ", "Arguments not equal");
        });
        assertEquals("Arguments not equal", e.getMessage());
    }

    @Test
    void testValidateDelete() {
        var key = "some/path/to/file.txt";
        var v = spy(validator);
        v.validateDelete(key);
        verify(v).validateKey(key);
    }

    @Test
    void testValidateBlobIdentifiers() {
        assertThrows(IllegalArgumentException.class, () -> validator.validateBlobIdentifiers(null));
        assertThrows(IllegalArgumentException.class, () -> validator.validateBlobIdentifiers(Collections.emptyList()));

        List<BlobIdentifier> objects = new ArrayList<>();
        objects.add(new BlobIdentifier("key1","version1"));
        objects.add(new BlobIdentifier("key2","version2"));
        objects.add(new BlobIdentifier("key3",null));
        validator.validateBlobIdentifiers(objects);

        objects.add(new BlobIdentifier("","version3"));
        assertThrows(IllegalArgumentException.class, () -> validator.validateBlobIdentifiers(objects));
    }

    @Test
    void testValidateUpload() {
        var key = "some/path/to/file.txt";
        var request = new UploadRequest.Builder().withKey(key).build();
        var v = spy(validator);
        v.validate(request);
        verify(v).validateKey(key);
    }

    @Test
    void testValidateDownload() {
        var key = "some/path/to/file.txt";
        var request = new DownloadRequest.Builder().withKey(key).build();
        var v = spy(validator);
        v.validate(request);
        verify(v).validateKey(key);
    }

    @Test
    void testValidateCopy() {
        var srcKey = "some/path/to/file.txt";
        var srcVersionId = "version-1";
        var destKey = "some/other/path/to/file.txt";
        var destBucket = "some-bucket";
        var request = CopyRequest.builder()
                .srcKey(srcKey)
                .srcVersionId(srcVersionId)
                .destKey(destKey)
                .destBucket(destBucket)
                .build();

        var v = spy(validator);
        v.validate(request);
        verify(v).validateKey(srcKey);
        verify(v).validateKey(destKey);
        verify(v).validateBucket(destBucket);
    }

    @Test
    void testValidateTags() {
        var e = assertThrows(IllegalArgumentException.class, () -> validator.validateTags(null));
        assertEquals(INVALID_TAGS_COLLECTION_MSG, e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> validator.validateTags(new HashMap<String, String>()));
        assertEquals(INVALID_TAGS_COLLECTION_MSG, e.getMessage());

        Map<String, String> tags = Map.of("tag-1", "tag-value-1", "tag-2", "tag-value-2");
        validator.validateTags(tags);   // This is the happy-path
    }

    @Test
    void testValidateDuration() {
        validator.validateDuration(Duration.ofHours(12));
        assertThrows(IllegalArgumentException.class, () -> validator.validateDuration(null));
        assertThrows(IllegalArgumentException.class, () -> validator.validateDuration(Duration.ofHours(0)));
        assertThrows(IllegalArgumentException.class, () -> validator.validateDuration(Duration.ofHours(-10)));
    }

    @Test
    void testValidatePresignedOperationType() {
        validator.validatePresignedOperationType(PresignedOperation.UPLOAD);
        validator.validatePresignedOperationType(PresignedOperation.DOWNLOAD);
        assertThrows(IllegalArgumentException.class, () -> validator.validatePresignedOperationType(null));
    }

    @Test
    void testValidatePresignedUploadRequest() {
        String key = "object-1";
        Map<String, String> metadata = Map.of("key-1", "value-1");
        Map<String, String> tags = Map.of("tag-1", "tag-value-1");
        Duration duration = Duration.ofHours(12);

        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.UPLOAD)
                .key(key)
                .duration(duration)
                .metadata(metadata)
                .tags(tags)
                .build();

        var v = spy(validator);

        v.validate(presignedUrlRequest);
        verify(v).validatePresignedOperationType(PresignedOperation.UPLOAD);
        verify(v).validateKey(key);
        verify(v).validateDuration(duration);
    }

    @Test
    void testValidatePresignedDownloadRequest() {
        String key = "object-1";
        Duration duration = Duration.ofHours(12);

        PresignedUrlRequest presignedUrlRequest = PresignedUrlRequest.builder()
                .type(PresignedOperation.DOWNLOAD)
                .key(key)
                .duration(duration)
                .build();

        var v = spy(validator);
        v.validate(presignedUrlRequest);
        verify(v).validatePresignedOperationType(PresignedOperation.DOWNLOAD);
        verify(v).validateKey(key);
        verify(v).validateDuration(duration);
    }

    @Test
    void testValidateEndpoint() {

        for(String protocol : Arrays.asList(null, "", "  ", "ftp", "http", "HTTP", "https", "HTTPS")) {
            boolean protocolValid = protocol != null && ("http".equals(protocol.toLowerCase()) || "https".equals(protocol.toLowerCase()));

            for(String host : Arrays.asList(null, "", "  ", "endpoint.example.com")) {
                boolean hostValid = host != null && !host.isBlank();

                for(int port : Arrays.asList(-1, 0, 80)) {
                    for(boolean requirePort : Arrays.asList(true, false)) {
                        boolean portValid = !requirePort || port >= 0;

                        for (String path : Arrays.asList(null, "  ", "/", "/path")) {
                            boolean pathValid = path==null;

                            for (String query : Arrays.asList(null, "?", "?key=value")) {
                                boolean queryValid = query==null;
                                boolean shouldBeValid = protocolValid && hostValid && portValid && pathValid && queryValid;

                                try {
                                    URI endpoint = new URI(protocol, null, host, port, path, query, null);
                                    if (shouldBeValid) {
                                        validator.validateEndpoint(endpoint, requirePort);
                                    } else {
                                        assertThrows(IllegalArgumentException.class, () -> {
                                                    validator.validateEndpoint(endpoint, requirePort);
                                                }, "endpoint=" + endpoint
                                        );
                                    }
                                } catch (URISyntaxException e) {
                                    // Ignore these errors. It's a state users can't even get to
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    void testValidateMaxConnections() {
        validator.validateMaxConnections(10);
        validator.validateMaxConnections(500000);
        assertThrows(IllegalArgumentException.class, () -> validator.validateMaxConnections(0));
        assertThrows(IllegalArgumentException.class, () -> validator.validateMaxConnections(-1));
        assertThrows(IllegalArgumentException.class, () -> validator.validateMaxConnections(-100));
        assertThrows(IllegalArgumentException.class, () -> validator.validateDuration(Duration.ofHours(0)));
        assertThrows(IllegalArgumentException.class, () -> validator.validateDuration(Duration.ofHours(-10)));
    }

    @Test
    void testValidateSocketTimeout() {
        validator.validateSocketTimeout(Duration.ofSeconds(60));
        validator.validateSocketTimeout(Duration.ofHours(24));
        validator.validateSocketTimeout(Duration.ofDays(0));
        validator.validateSocketTimeout(Duration.ofSeconds(0));
        assertThrows(IllegalArgumentException.class, () -> validator.validateSocketTimeout(Duration.ofHours(-1)));
        assertThrows(IllegalArgumentException.class, () -> validator.validateSocketTimeout(Duration.ofHours(-10)));
    }

    @Test
    void testValidateRange() {
        validator.validateRange(0L, 100L);
        validator.validateRange(100L, 100L);
        validator.validateRange(100L, 500L);
        validator.validateRange(null, 100L);
        validator.validateRange(100L, null);
        validator.validateRange(null, null);
        assertThrows(IllegalArgumentException.class, () -> validator.validateRange(-1L, 100L));
        assertThrows(IllegalArgumentException.class, () -> validator.validateRange(0L, -100L));
        assertThrows(IllegalArgumentException.class, () -> validator.validateRange(-1L, null));
        assertThrows(IllegalArgumentException.class, () -> validator.validateRange(null, -100L));
        assertThrows(IllegalArgumentException.class, () -> validator.validateRange(-100L, -100L));
        assertThrows(IllegalArgumentException.class, () -> validator.validateRange(100L, 50L));
    }
}
