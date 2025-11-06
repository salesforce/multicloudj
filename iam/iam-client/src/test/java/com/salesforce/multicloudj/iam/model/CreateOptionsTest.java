package com.salesforce.multicloudj.iam.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}