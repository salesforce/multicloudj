package com.salesforce.multicloudj.iam.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for CreateOptions builder pattern and functionality.
 */
public class CreateOptionsTest {

    @Test
    public void testCreateOptionsBuilder() {
        CreateOptions options = CreateOptions.builder()
            .path("/service-roles/")
            .maxSessionDuration(3600)
            .permissionBoundary("arn:aws:iam::123456789012:policy/PowerUserBoundary")
            .build();

        assertEquals("/service-roles/", options.getPath());
        assertEquals(Integer.valueOf(3600), options.getMaxSessionDuration());
        assertEquals("arn:aws:iam::123456789012:policy/PowerUserBoundary", options.getPermissionBoundary());
    }

    @Test
    public void testCreateOptionsBuilderMinimal() {
        CreateOptions options = CreateOptions.builder()
            .build();

        assertNull(options.getPath());
        assertNull(options.getMaxSessionDuration());
        assertNull(options.getPermissionBoundary());
    }

    @Test
    public void testCreateOptionsBuilderIndividualFields() {
        // Test path only
        CreateOptions pathOptions = CreateOptions.builder()
            .path("/application/backend/")
            .build();

        assertEquals("/application/backend/", pathOptions.getPath());
        assertNull(pathOptions.getMaxSessionDuration());
        assertNull(pathOptions.getPermissionBoundary());

        // Test maxSessionDuration only
        CreateOptions durationOptions = CreateOptions.builder()
            .maxSessionDuration(7200)
            .build();

        assertNull(durationOptions.getPath());
        assertEquals(Integer.valueOf(7200), durationOptions.getMaxSessionDuration());
        assertNull(durationOptions.getPermissionBoundary());

        // Test permissionBoundary only
        CreateOptions boundaryOptions = CreateOptions.builder()
            .permissionBoundary("arn:aws:iam::123456789012:policy/DeveloperBoundary")
            .build();

        assertNull(boundaryOptions.getPath());
        assertNull(boundaryOptions.getMaxSessionDuration());
        assertEquals("arn:aws:iam::123456789012:policy/DeveloperBoundary", boundaryOptions.getPermissionBoundary());
    }

    @Test
    public void testCreateOptionsBuilderWithCustomSessionDurations() {
        // Test minimum duration (900 seconds = 15 minutes)
        CreateOptions minOptions = CreateOptions.builder()
            .maxSessionDuration(900)
            .build();
        assertEquals(Integer.valueOf(900), minOptions.getMaxSessionDuration());

        // Test maximum duration (43200 seconds = 12 hours)
        CreateOptions maxOptions = CreateOptions.builder()
            .maxSessionDuration(43200)
            .build();
        assertEquals(Integer.valueOf(43200), maxOptions.getMaxSessionDuration());

        // Test common duration (7200 seconds = 2 hours)
        CreateOptions commonOptions = CreateOptions.builder()
            .maxSessionDuration(7200)
            .build();
        assertEquals(Integer.valueOf(7200), commonOptions.getMaxSessionDuration());
    }


    @Test
    public void testCreateOptionsBuilderComplexScenario() {
        CreateOptions options = CreateOptions.builder()
            .path("/microservices/user-service/")
            .maxSessionDuration(14400)  // 4 hours
            .permissionBoundary("arn:aws:iam::123456789012:policy/MicroserviceBoundary")
            .build();

        assertEquals("/microservices/user-service/", options.getPath());
        assertEquals(Integer.valueOf(14400), options.getMaxSessionDuration());
        assertEquals("arn:aws:iam::123456789012:policy/MicroserviceBoundary", options.getPermissionBoundary());
    }

    @Test
    public void testCreateOptionsEqualsAndHashCode() {
        CreateOptions options1 = CreateOptions.builder()
            .path("/test/")
            .maxSessionDuration(3600)
            .permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary")
            .build();

        CreateOptions options2 = CreateOptions.builder()
            .path("/test/")
            .maxSessionDuration(3600)
            .permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary")
            .build();

        CreateOptions differentPath = CreateOptions.builder()
            .path("/different/")
            .maxSessionDuration(3600)
            .permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary")
            .build();

        CreateOptions differentDuration = CreateOptions.builder()
            .path("/test/")
            .maxSessionDuration(7200)
            .permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary")
            .build();

        CreateOptions nullOptions = CreateOptions.builder().build();
        CreateOptions anotherNullOptions = CreateOptions.builder().build();

        // Test equals
        assertEquals(options1, options2);
        assertEquals(options1, options1); // same object
        assertNotEquals(options1, differentPath);
        assertNotEquals(options1, differentDuration);
        assertNotEquals(options1, null);
        assertNotEquals(options1, "not create options");
        assertEquals(nullOptions, anotherNullOptions);

        // Test hashCode
        assertEquals(options1.hashCode(), options2.hashCode());
        assertNotEquals(options1.hashCode(), differentPath.hashCode());
        assertEquals(nullOptions.hashCode(), anotherNullOptions.hashCode());
    }

    @Test
    public void testCreateOptionsToString() {
        // Test with all fields populated
        CreateOptions options = CreateOptions.builder()
            .path("/test/")
            .maxSessionDuration(7200)
            .permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary")
            .build();

        String result = options.toString();
        assertTrue(result.contains("path='/test/'"));
        assertTrue(result.contains("maxSessionDuration=7200"));
        assertTrue(result.contains("permissionBoundary='arn:aws:iam::123456789012:policy/TestBoundary'"));

        // Test with null values
        CreateOptions nullOptions = CreateOptions.builder().build();
        String nullResult = nullOptions.toString();
        assertTrue(nullResult.contains("CreateOptions"));
        assertTrue(nullResult.contains("path='null'"));
        assertTrue(nullResult.contains("maxSessionDuration=null"));
        assertTrue(nullResult.contains("permissionBoundary='null'"));

        // Test with partial values
        CreateOptions partialOptions = CreateOptions.builder()
            .path("/test/")
            .maxSessionDuration(null)
            .permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary")
            .build();

        String partialResult = partialOptions.toString();
        assertTrue(partialResult.contains("path='/test/'"));
        assertTrue(partialResult.contains("maxSessionDuration=null"));
        assertTrue(partialResult.contains("permissionBoundary='arn:aws:iam::123456789012:policy/TestBoundary'"));
    }

    @Test
    public void testCreateOptionsBuilderMethodChaining() {
        CreateOptions.Builder builder = CreateOptions.builder();

        // Test that each method returns the same builder instance
        assertSame(builder, builder.path("/test/"));
        assertSame(builder, builder.maxSessionDuration(3600));
        assertSame(builder, builder.permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary"));
    }

    @Test
    public void testCreateOptionsBuilderNullValues() {
        CreateOptions options = CreateOptions.builder()
            .path(null)
            .maxSessionDuration(null)
            .permissionBoundary(null)
            .build();

        assertNull(options.getPath());
        assertNull(options.getMaxSessionDuration());
        assertNull(options.getPermissionBoundary());
    }


    @Test
    public void testCreateOptionsBuilderOverwriteValues() {
        CreateOptions options = CreateOptions.builder()
            .path("/first/")
            .path("/second/") // This should overwrite the first value
            .maxSessionDuration(3600)
            .maxSessionDuration(7200) // This should overwrite the first value
            .permissionBoundary("arn:aws:iam::123456789012:policy/FirstBoundary")
            .permissionBoundary("arn:aws:iam::123456789012:policy/SecondBoundary") // This should overwrite
            .build();

        assertEquals("/second/", options.getPath());
        assertEquals(Integer.valueOf(7200), options.getMaxSessionDuration());
        assertEquals("arn:aws:iam::123456789012:policy/SecondBoundary", options.getPermissionBoundary());
    }
}