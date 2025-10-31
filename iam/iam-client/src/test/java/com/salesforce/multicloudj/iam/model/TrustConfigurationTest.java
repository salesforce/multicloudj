package com.salesforce.multicloudj.iam.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}