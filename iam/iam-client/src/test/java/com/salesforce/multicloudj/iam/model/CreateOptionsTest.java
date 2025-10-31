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

        // Test permissionBoundary only (AWS example)
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
    public void testCreateOptionsBuilderMethodChaining() {
        CreateOptions.CreateOptionsBuilder builder = CreateOptions.builder();

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

    @Test
    public void testCreateOptionsBuilderProviderSpecificExamples() {
        // AWS Example
        CreateOptions awsOptions = CreateOptions.builder()
            .path("/foo/")
            .maxSessionDuration(43200) // 12 hours
            .permissionBoundary("arn:aws:iam::123456789012:policy/PowerUserBoundary")
            .build();

        assertEquals("/foo/", awsOptions.getPath());
        assertEquals(Integer.valueOf(43200), awsOptions.getMaxSessionDuration());
        assertEquals("arn:aws:iam::123456789012:policy/PowerUserBoundary", awsOptions.getPermissionBoundary());

        // GCP Example
        CreateOptions gcpOptions = CreateOptions.builder()
            .path("/foo/")
            .maxSessionDuration(3600) // 1 hour
            .permissionBoundary("constraints/compute.restrictLoadBalancerCreationForTypes")
            .build();

        assertEquals("/foo/", gcpOptions.getPath());
        assertEquals(Integer.valueOf(3600), gcpOptions.getMaxSessionDuration());
        assertEquals("constraints/compute.restrictLoadBalancerCreationForTypes", gcpOptions.getPermissionBoundary());

        // AliCloud Example (using Control Policy)
        CreateOptions aliOptions = CreateOptions.builder()
            .path("/foo/")
            .maxSessionDuration(7200) // 2 hours
            .permissionBoundary("cp-bp1example") // Control Policy ID
            .build();

        assertEquals("/foo/", aliOptions.getPath());
        assertEquals(Integer.valueOf(7200), aliOptions.getMaxSessionDuration());
        assertEquals("cp-bp1example", aliOptions.getPermissionBoundary()); // AliCloud Control Policy
    }
}