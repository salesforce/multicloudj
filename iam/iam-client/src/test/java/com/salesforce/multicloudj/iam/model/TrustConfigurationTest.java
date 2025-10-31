package com.salesforce.multicloudj.iam.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TrustConfiguration builder pattern and functionality.
 */
public class TrustConfigurationTest {

    @Test
    public void testTrustConfigurationBuilder() {
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")
            .addTrustedPrincipal("service-account@project.iam.gserviceaccount.com")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .addCondition("StringEquals", "aws:userid", "AIDACKCEVSQ6C2EXAMPLE")
            .build();

        List<String> expectedPrincipals = Arrays.asList(
            "arn:aws:iam::123456789012:root",
            "service-account@project.iam.gserviceaccount.com"
        );

        assertEquals(expectedPrincipals, trustConfig.getTrustedPrincipals());

        Map<String, Map<String, Object>> conditions = trustConfig.getConditions();
        assertTrue(conditions.containsKey("StringEquals"));
        assertEquals("us-west-2", conditions.get("StringEquals").get("aws:RequestedRegion"));
        assertEquals("AIDACKCEVSQ6C2EXAMPLE", conditions.get("StringEquals").get("aws:userid"));
    }

    @Test
    public void testTrustConfigurationBuilderMinimal() {
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::987654321098:root")
            .build();

        assertEquals(Arrays.asList("arn:aws:iam::987654321098:root"), trustConfig.getTrustedPrincipals());
        assertTrue(trustConfig.getConditions().isEmpty());
    }

    @Test
    public void testTrustConfigurationBuilderMultipleTrustedPrincipals() {
        List<String> expectedPrincipals = Arrays.asList(
            "arn:aws:iam::111111111111:root",
            "arn:aws:iam::222222222222:root",
            "arn:aws:iam::333333333333:user/CrossAccountUser"
        );

        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::111111111111:root")
            .addTrustedPrincipal("arn:aws:iam::222222222222:root")
            .addTrustedPrincipal("arn:aws:iam::333333333333:user/CrossAccountUser")
            .build();

        assertEquals(expectedPrincipals, trustConfig.getTrustedPrincipals());
    }

    @Test
    public void testTrustConfigurationBuilderAddTrustedPrincipals() {
        List<String> principalsToAdd = Arrays.asList(
            "arn:aws:iam::111111111111:root",
            "arn:aws:iam::222222222222:root"
        );

        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")
            .addTrustedPrincipals(principalsToAdd)
            .build();

        List<String> expectedPrincipals = Arrays.asList(
            "arn:aws:iam::123456789012:root",
            "arn:aws:iam::111111111111:root",
            "arn:aws:iam::222222222222:root"
        );

        assertEquals(expectedPrincipals, trustConfig.getTrustedPrincipals());
    }

    @Test
    public void testTrustConfigurationBuilderMultipleConditions() {
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .addCondition("DateGreaterThan", "aws:CurrentTime", "2024-01-01T00:00:00Z")
            .addCondition("IpAddress", "aws:SourceIp", "203.0.113.0/24")
            .build();

        Map<String, Map<String, Object>> conditions = trustConfig.getConditions();

        assertTrue(conditions.containsKey("StringEquals"));
        assertTrue(conditions.containsKey("DateGreaterThan"));
        assertTrue(conditions.containsKey("IpAddress"));

        assertEquals("us-west-2", conditions.get("StringEquals").get("aws:RequestedRegion"));
        assertEquals("2024-01-01T00:00:00Z", conditions.get("DateGreaterThan").get("aws:CurrentTime"));
        assertEquals("203.0.113.0/24", conditions.get("IpAddress").get("aws:SourceIp"));
    }

    @Test
    public void testTrustConfigurationBuilderSameOperatorMultipleConditions() {
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .addCondition("StringEquals", "aws:userid", "AIDACKCEVSQ6C2EXAMPLE")
            .build();

        Map<String, Map<String, Object>> conditions = trustConfig.getConditions();
        assertTrue(conditions.containsKey("StringEquals"));

        Map<String, Object> stringEqualsConditions = conditions.get("StringEquals");
        assertEquals(2, stringEqualsConditions.size());
        assertEquals("us-west-2", stringEqualsConditions.get("aws:RequestedRegion"));
        assertEquals("AIDACKCEVSQ6C2EXAMPLE", stringEqualsConditions.get("aws:userid"));
    }

    @Test
    public void testTrustConfigurationBuilderGcpServiceAccount() {
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("service-account@my-project.iam.gserviceaccount.com")
            .addTrustedPrincipal("another-sa@different-project.iam.gserviceaccount.com")
            .build();

        List<String> expectedPrincipals = Arrays.asList(
            "service-account@my-project.iam.gserviceaccount.com",
            "another-sa@different-project.iam.gserviceaccount.com"
        );

        assertEquals(expectedPrincipals, trustConfig.getTrustedPrincipals());
    }

    @Test
    public void testTrustConfigurationBuilderAliCloudPrincipals() {
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("1234567890123456")  // AliCloud account ID
            .addTrustedPrincipal("acs:ram::1234567890123456:user/AliUser")  // AliCloud RAM user
            .build();

        List<String> expectedPrincipals = Arrays.asList(
            "1234567890123456",
            "acs:ram::1234567890123456:user/AliUser"
        );

        assertEquals(expectedPrincipals, trustConfig.getTrustedPrincipals());
    }

    @Test
    public void testTrustConfigurationBuilderComplexScenario() {
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")  // AWS account
            .addTrustedPrincipal("service-account@project.iam.gserviceaccount.com")  // GCP service account
            .addTrustedPrincipal("1234567890123456")  // AliCloud account
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .addCondition("StringEquals", "sts:ExternalId", "cross-cloud-external-id")
            .addCondition("Bool", "aws:MultiFactorAuthPresent", "true")
            .build();

        List<String> expectedPrincipals = Arrays.asList(
            "arn:aws:iam::123456789012:root",
            "service-account@project.iam.gserviceaccount.com",
            "1234567890123456"
        );

        assertEquals(expectedPrincipals, trustConfig.getTrustedPrincipals());

        Map<String, Map<String, Object>> conditions = trustConfig.getConditions();
        assertTrue(conditions.containsKey("StringEquals"));
        assertTrue(conditions.containsKey("Bool"));

        assertEquals("us-west-2", conditions.get("StringEquals").get("aws:RequestedRegion"));
        assertEquals("cross-cloud-external-id", conditions.get("StringEquals").get("sts:ExternalId"));
        assertEquals("true", conditions.get("Bool").get("aws:MultiFactorAuthPresent"));
    }

    @Test
    public void testTrustConfigurationBuilderEmptyBuilder() {
        TrustConfiguration trustConfig = TrustConfiguration.builder().build();

        assertTrue(trustConfig.getTrustedPrincipals().isEmpty());
        assertTrue(trustConfig.getConditions().isEmpty());
    }

    @Test
    public void testNullAndEmptyValueHandling() {
        // Test null and empty principals
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")
            .addTrustedPrincipal(null)
            .addTrustedPrincipal("")
            .addTrustedPrincipal("   ")  // whitespace only
            .addTrustedPrincipal("arn:aws:iam::987654321098:root")
            .addTrustedPrincipals(null) // null list
            .addTrustedPrincipals(Arrays.asList("valid-principal", null, "", "   "))
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .addCondition(null, "key", "value")  // null operator
            .addCondition("StringEquals", null, "value")  // null key
            .addCondition("StringEquals", "key", null)  // null value
            .build();

        // Should only have valid principals
        assertEquals(3, trustConfig.getTrustedPrincipals().size());
        assertTrue(trustConfig.getTrustedPrincipals().contains("arn:aws:iam::123456789012:root"));
        assertTrue(trustConfig.getTrustedPrincipals().contains("arn:aws:iam::987654321098:root"));
        assertTrue(trustConfig.getTrustedPrincipals().contains("valid-principal"));

        // Should only have valid condition
        assertEquals(1, trustConfig.getConditions().size());
        assertTrue(trustConfig.getConditions().containsKey("StringEquals"));
        assertEquals("us-west-2", trustConfig.getConditions().get("StringEquals").get("aws:RequestedRegion"));
    }



    @Test
    public void testTrustConfigurationToString() {
        TrustConfiguration trustConfig = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .build();

        String toString = trustConfig.toString();
        assertTrue(toString.contains("trustedPrincipals"));
        assertTrue(toString.contains("conditions"));
        assertTrue(toString.contains("arn:aws:iam::123456789012:root"));
    }


    @Test
    public void testTrustConfigurationEqualsAndHashCodeWithNullChecks() {
        TrustConfiguration trust1 = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .build();

        TrustConfiguration trust2 = TrustConfiguration.builder()
            .addTrustedPrincipal("arn:aws:iam::123456789012:root")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .build();

        // Test equals with null and different types
        assertNotEquals(trust1, null);
        assertNotEquals(trust1, "not a trust config");
        assertEquals(trust1, trust1); // same object
        assertEquals(trust1, trust2); // equal objects

        // Test hashCode consistency
        assertEquals(trust1.hashCode(), trust2.hashCode());
    }

}