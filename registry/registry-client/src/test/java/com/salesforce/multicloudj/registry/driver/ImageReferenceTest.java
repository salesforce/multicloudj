package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for ImageReference.
 */
public class ImageReferenceTest {

    private static final String VALID_SHA256_HEX = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t", "\n"})
    void testParse_ThrowsException_WhenReferenceIsBlank(String ref) {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class,
                () -> ImageReference.parse(ref));

        assertTrue(exception.getMessage().contains("cannot be null or empty"));
    }


    @ParameterizedTest
    @CsvSource({
        "my-repo@sha256:" + VALID_SHA256_HEX + ",                            my-repo",
        "registry.example.com:5000/org/repo@sha256:" + VALID_SHA256_HEX + ",  registry.example.com:5000/org/repo",
    })
    void testParse_Digest_ValidFormats(String ref, String expectedRepo) {
        ImageReference imageRef = ImageReference.parse(ref);

        assertNotNull(imageRef);
        assertEquals(expectedRepo.trim(), imageRef.getRepository());
        assertEquals("sha256:" + VALID_SHA256_HEX, imageRef.getReference());
    }

    @Test
    void testParse_Digest_TrimsWhitespace() {
        String ref = "  my-repo@sha256:" + VALID_SHA256_HEX + "  ";

        ImageReference imageRef = ImageReference.parse(ref);

        assertEquals("my-repo", imageRef.getRepository());
        assertEquals("sha256:" + VALID_SHA256_HEX, imageRef.getReference());
    }

    @Test
    void testParse_Digest_ThrowsException_WhenMultipleAtSymbols() {
        String ref = "my-repo@sha256:abc@xyz";

        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class,
                () -> ImageReference.parse(ref));

        assertTrue(exception.getMessage().contains("exactly one '@' separator"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"@sha256:" + VALID_SHA256_HEX, "   @sha256:" + VALID_SHA256_HEX})
    void testParse_Digest_ThrowsException_WhenRepositoryIsEmptyOrBlank(String ref) {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class,
                () -> ImageReference.parse(ref));

        assertTrue(exception.getMessage().contains("Repository cannot be empty"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"my-repo@", "my-repo@   "})
    void testParse_Digest_ThrowsException_WhenDigestIsEmptyOrBlank(String ref) {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class,
                () -> ImageReference.parse(ref));

        assertTrue(exception.getMessage().contains("Digest cannot be empty"));
    }

    @Test
    void testParse_Digest_ThrowsException_WhenAlgorithmIsNotSha256() {
        String ref = "my-repo@md5:abcdef123456";

        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class,
                () -> ImageReference.parse(ref));

        assertTrue(exception.getMessage().contains("unsupported digest algorithm"));
        assertTrue(exception.getMessage().contains("expected sha256"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"my-repo@sha256:abcdef123456", "my-repo@sha256:" + VALID_SHA256_HEX + "extra"})
    void testParse_Digest_ThrowsException_WhenHexLengthIsNot64(String ref) {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class,
                () -> ImageReference.parse(ref));

        assertTrue(exception.getMessage().contains("invalid checksum digest length"));
        assertTrue(exception.getMessage().contains("expected 64 characters"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "my-repo@sha256:ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        "my-repo@sha256:0123456789abcdef-123456789abcdef0123456789abcdef0123456789abcdef"
    })
    void testParse_Digest_ThrowsException_WhenHexFormatInvalid(String ref) {
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class,
                () -> ImageReference.parse(ref));

        assertTrue(exception.getMessage().contains("invalid checksum digest format"));
    }

    @Test
    void testParse_Tag_SimpleFormat() {
        String ref = "my-repo:v1.0.0";

        ImageReference imageRef = ImageReference.parse(ref);

        assertNotNull(imageRef);
        assertEquals("my-repo", imageRef.getRepository());
        assertEquals("v1.0.0", imageRef.getReference());
        assertEquals(ref, imageRef.toString());
    }

    @Test
    void testParse_Tag_RegistryPortNotConfusedWithTag() {
        // Colon in registry:port must not be mistaken for tag delimiter
        String ref = "registry:5000/org:special/repo:v1";

        ImageReference imageRef = ImageReference.parse(ref);

        assertEquals("registry:5000/org:special/repo", imageRef.getRepository());
        assertEquals("v1", imageRef.getReference());
    }

    @ParameterizedTest
    @ValueSource(strings = {"my-repo", "org/my-repo"})
    void testParse_Tag_DefaultsToLatest_WhenNoTagProvided(String ref) {
        ImageReference imageRef = ImageReference.parse(ref);

        assertNotNull(imageRef);
        assertEquals(ref, imageRef.getRepository());
        assertEquals("latest", imageRef.getReference());
        assertEquals(ref + ":latest", imageRef.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"my-repo:", "my-repo:   "})
    void testParse_Throws_WhenTagIsEmptyOrBlankAfterColon(String ref) {
        InvalidArgumentException exception =
                assertThrows(InvalidArgumentException.class, () -> ImageReference.parse(ref));

        assertTrue(exception.getMessage().contains("tag cannot be empty after ':'"));
    }

}
