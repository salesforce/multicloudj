package com.salesforce.multicloudj.registry.driver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for ImageReference parsing.
 * Tests OCI image reference parsing including registry with ports, tags, and digests.
 */
class ImageReferenceTest {


    @Test
    void testParse_SimpleRepo_DefaultTag() {
        ImageReference ref = ImageReference.parse("myrepo");
        assertEquals("myrepo", ref.getRepository());
        assertEquals("latest", ref.getReference());
        assertEquals("myrepo:latest", ref.toString());
    }

    @Test
    void testParse_RepoWithTag() {
        ImageReference ref = ImageReference.parse("myrepo:v1.0");
        assertEquals("myrepo", ref.getRepository());
        assertEquals("v1.0", ref.getReference());
        assertEquals("myrepo:v1.0", ref.toString());
    }

    @Test
    void testParse_RepoWithLatestTag() {
        ImageReference ref = ImageReference.parse("myrepo:latest");
        assertEquals("myrepo", ref.getRepository());
        assertEquals("latest", ref.getReference());
        assertEquals("myrepo:latest", ref.toString());
    }


    @Test
    void testParse_RegistryWithPort_NoTag() {
        // Previously broken: would parse port as tag
        ImageReference ref = ImageReference.parse("registry.example.com:5000/myrepo");
        assertEquals("registry.example.com:5000/myrepo", ref.getRepository());
        assertEquals("latest", ref.getReference());
        assertEquals("registry.example.com:5000/myrepo:latest", ref.toString());
    }

    @Test
    void testParse_RegistryWithPort_WithTag() {
        ImageReference ref = ImageReference.parse("registry.example.com:5000/myrepo:v1.0");
        assertEquals("registry.example.com:5000/myrepo", ref.getRepository());
        assertEquals("v1.0", ref.getReference());
        assertEquals("registry.example.com:5000/myrepo:v1.0", ref.toString());
    }

    @Test
    void testParse_LocalhostWithPort_NoTag() {
        ImageReference ref = ImageReference.parse("localhost:8080/test/repo");
        assertEquals("localhost:8080/test/repo", ref.getRepository());
        assertEquals("latest", ref.getReference());
    }

    @Test
    void testParse_LocalhostWithPort_WithTag() {
        ImageReference ref = ImageReference.parse("localhost:8080/test/repo:tag");
        assertEquals("localhost:8080/test/repo", ref.getRepository());
        assertEquals("tag", ref.getReference());
    }

    @Test
    void testParse_RegistryWithStandardPort() {
        ImageReference ref = ImageReference.parse("registry.io:443/myrepo");
        assertEquals("registry.io:443/myrepo", ref.getRepository());
        assertEquals("latest", ref.getReference());
    }


    @Test
    void testParse_NestedPath_NoTag() {
        ImageReference ref = ImageReference.parse("host:5000/path/to/repo");
        assertEquals("host:5000/path/to/repo", ref.getRepository());
        assertEquals("latest", ref.getReference());
    }

    @Test
    void testParse_NestedPath_WithTag() {
        ImageReference ref = ImageReference.parse("host:5000/path/to/repo:v1");
        assertEquals("host:5000/path/to/repo", ref.getRepository());
        assertEquals("v1", ref.getReference());
    }

    @Test
    void testParse_GcrStyle() {
        ImageReference ref = ImageReference.parse("gcr.io/project-id/repo:tag");
        assertEquals("gcr.io/project-id/repo", ref.getRepository());
        assertEquals("tag", ref.getReference());
    }

    @Test
    void testParse_DeepNestedPath() {
        ImageReference ref = ImageReference.parse("registry:5000/org/team/project/repo:v2.0");
        assertEquals("registry:5000/org/team/project/repo", ref.getRepository());
        assertEquals("v2.0", ref.getReference());
    }


    @Test
    void testParse_SimpleDigest() {
        String digest = "sha256:" + "a".repeat(64);
        ImageReference ref = ImageReference.parse("myrepo@" + digest);
        assertEquals("myrepo", ref.getRepository());
        assertEquals(digest, ref.getReference());
        assertEquals("myrepo@" + digest, ref.toString());
    }

    @Test
    void testParse_RegistryWithPort_Digest() {
        String digest = "sha256:" + "b".repeat(64);
        ImageReference ref = ImageReference.parse("host:5000/repo@" + digest);
        assertEquals("host:5000/repo", ref.getRepository());
        assertEquals(digest, ref.getReference());
    }

    @Test
    void testParse_NestedPath_Digest() {
        String digest = "sha256:" + "c".repeat(64);
        ImageReference ref = ImageReference.parse("host:5000/path/to/repo@" + digest);
        assertEquals("host:5000/path/to/repo", ref.getRepository());
        assertEquals(digest, ref.getReference());
    }


    @Test
    void testParse_EmptyTag() {
        // Trailing colon with empty tag - defaults to latest but keeps original format
        ImageReference ref = ImageReference.parse("repo:");
        assertEquals("repo:", ref.getRepository());  // Keeps trailing colon
        assertEquals("latest", ref.getReference());
        assertEquals("repo::latest", ref.toString());
    }

    @Test
    void testParse_RegistryWithPort_EmptyTag() {
        ImageReference ref = ImageReference.parse("host:5000/repo:");
        assertEquals("host:5000/repo:", ref.getRepository());  // Keeps trailing colon
        assertEquals("latest", ref.getReference());
    }

    @Test
    void testParse_MultipleColons_OnlyLastIsTag() {
        // Registry with port AND repository with colon (finds first colon after last slash)
        ImageReference ref = ImageReference.parse("host:5000/special:repo:v1");
        assertEquals("host:5000/special", ref.getRepository());  // First colon after slash
        assertEquals("repo:v1", ref.getReference());  // Everything after first colon
    }


    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  ", "\t", "\n"})
    void testParse_InvalidInput_ThrowsException(String input) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse(input));
        assertEquals("Image reference cannot be null or empty", exception.getMessage());
    }

    @Test
    void testParse_InvalidDigest_NotSha256() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse("repo@md5:abc123"));
        assertEquals("unsupported digest algorithm: md5:abc123 (expected sha256)", exception.getMessage());
    }

    @Test
    void testParse_InvalidDigest_TooShort() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse("repo@sha256:abc123"));
        assertEquals("invalid checksum digest length: expected 64 characters, got 6: repo@sha256:abc123",
                exception.getMessage());
    }

    @Test
    void testParse_InvalidDigest_TooLong() {
        String digest = "sha256:" + "a".repeat(65);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse("repo@" + digest));
        assertEquals("invalid checksum digest length: expected 64 characters, got 65: repo@" + digest,
                exception.getMessage());
    }

    @Test
    void testParse_InvalidDigest_UpperCase() {
        String digest = "sha256:" + "A".repeat(64);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse("repo@" + digest));
        assertEquals("invalid checksum digest format: must be 64 lowercase hex characters [a-f0-9]: repo@" + digest,
                exception.getMessage());
    }

    @Test
    void testParse_InvalidDigest_InvalidHexChars() {
        String digest = "sha256:" + "xyz".repeat(21) + "x";  // 64 chars but invalid hex
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse("repo@" + digest));
        assertEquals("invalid checksum digest format: must be 64 lowercase hex characters [a-f0-9]: repo@" + digest,
                exception.getMessage());
    }

    @Test
    void testParse_MultipleAtSymbols() {
        // Multiple @ gets split on first @, then digest validation fails
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse("repo@sha256:abc@def"));
        // Error occurs during digest validation (length check), not @ count check
        assertEquals("invalid checksum digest length: expected 64 characters, got 7: repo@sha256:abc@def",
                exception.getMessage());
    }

    @Test
    void testParse_EmptyRepository() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse("@sha256:" + "a".repeat(64)));
        assertEquals("Repository cannot be empty: @sha256:" + "a".repeat(64), exception.getMessage());
    }

    @Test
    void testParse_EmptyDigest() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ImageReference.parse("repo@"));
        assertEquals("Digest cannot be empty: repo@", exception.getMessage());
    }
}
