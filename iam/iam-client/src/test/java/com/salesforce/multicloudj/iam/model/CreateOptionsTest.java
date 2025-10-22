package com.salesforce.multicloudj.iam.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
    public void testCreateOptionsBuilderWithPath() {
        CreateOptions options = CreateOptions.builder()
            .path("/application/backend/")
            .build();

        assertEquals("/application/backend/", options.getPath());
        assertNull(options.getMaxSessionDuration());
        assertNull(options.getPermissionBoundary());
    }

    @Test
    public void testCreateOptionsBuilderWithMaxSessionDuration() {
        CreateOptions options = CreateOptions.builder()
            .maxSessionDuration(7200)
            .build();

        assertNull(options.getPath());
        assertEquals(Integer.valueOf(7200), options.getMaxSessionDuration());
        assertNull(options.getPermissionBoundary());
    }

    @Test
    public void testCreateOptionsBuilderWithPermissionBoundary() {
        CreateOptions options = CreateOptions.builder()
            .permissionBoundary("arn:aws:iam::123456789012:policy/DeveloperBoundary")
            .build();

        assertNull(options.getPath());
        assertNull(options.getMaxSessionDuration());
        assertEquals("arn:aws:iam::123456789012:policy/DeveloperBoundary", options.getPermissionBoundary());
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
    public void testCreateOptionsBuilderWithDifferentPaths() {
        // Test root path
        CreateOptions rootOptions = CreateOptions.builder()
            .path("/")
            .build();
        assertEquals("/", rootOptions.getPath());

        // Test nested path
        CreateOptions nestedOptions = CreateOptions.builder()
            .path("/division/team/service/")
            .build();
        assertEquals("/division/team/service/", nestedOptions.getPath());

        // Test simple path
        CreateOptions simpleOptions = CreateOptions.builder()
            .path("/service-roles/")
            .build();
        assertEquals("/service-roles/", simpleOptions.getPath());
    }

    @Test
    public void testCreateOptionsBuilderWithDifferentPermissionBoundaries() {
        // Test AWS IAM policy ARN
        CreateOptions awsOptions = CreateOptions.builder()
            .permissionBoundary("arn:aws:iam::123456789012:policy/PowerUserBoundary")
            .build();
        assertEquals("arn:aws:iam::123456789012:policy/PowerUserBoundary", awsOptions.getPermissionBoundary());

        // Test different policy name
        CreateOptions devOptions = CreateOptions.builder()
            .permissionBoundary("arn:aws:iam::987654321098:policy/DeveloperBoundary")
            .build();
        assertEquals("arn:aws:iam::987654321098:policy/DeveloperBoundary", devOptions.getPermissionBoundary());
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
    public void testCreateOptionsEquality() {
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

        CreateOptions options3 = CreateOptions.builder()
            .path("/different/")
            .maxSessionDuration(3600)
            .permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary")
            .build();

        assertEquals(options1, options2);
        assertNotEquals(options1, options3);
        assertEquals(options1.hashCode(), options2.hashCode());
    }

    @Test
    public void testCreateOptionsToString() {
        CreateOptions options = CreateOptions.builder()
            .path("/test/")
            .maxSessionDuration(7200)
            .permissionBoundary("arn:aws:iam::123456789012:policy/TestBoundary")
            .build();

        String toString = options.toString();
        assertTrue(toString.contains("path='/test/'"));
        assertTrue(toString.contains("maxSessionDuration=7200"));
        assertTrue(toString.contains("permissionBoundary='arn:aws:iam::123456789012:policy/TestBoundary'"));
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
}