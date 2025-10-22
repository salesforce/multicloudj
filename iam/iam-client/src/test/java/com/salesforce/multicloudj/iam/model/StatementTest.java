package com.salesforce.multicloudj.iam.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Statement builder pattern and functionality.
 */
public class StatementTest {

    @Test
    public void testStatementBuilder() {
        Statement statement = Statement.builder()
            .sid("TestStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addAction("storage:PutObject")
            .addResource("storage://my-bucket/*")
            .addPrincipal("arn:aws:iam::123456789012:user/TestUser")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .build();

        assertEquals("TestStatement", statement.getSid());
        assertEquals("Allow", statement.getEffect());
        assertEquals(Arrays.asList("storage:GetObject", "storage:PutObject"), statement.getActions());
        assertEquals(Arrays.asList("storage://my-bucket/*"), statement.getResources());
        assertEquals(Arrays.asList("arn:aws:iam::123456789012:user/TestUser"), statement.getPrincipals());

        assertTrue(statement.getConditions().containsKey("StringEquals"));
        assertEquals("us-west-2", statement.getConditions().get("StringEquals").get("aws:RequestedRegion"));
    }

    @Test
    public void testStatementBuilderMinimal() {
        Statement statement = Statement.builder()
            .sid("MinimalStatement")
            .effect("Deny")
            .addAction("storage:DeleteObject")
            .addResource("storage://sensitive-bucket/*")
            .build();

        assertEquals("MinimalStatement", statement.getSid());
        assertEquals("Deny", statement.getEffect());
        assertEquals(Arrays.asList("storage:DeleteObject"), statement.getActions());
        assertEquals(Arrays.asList("storage://sensitive-bucket/*"), statement.getResources());
        assertTrue(statement.getPrincipals().isEmpty());
        assertTrue(statement.getConditions().isEmpty());
    }

    @Test
    public void testStatementBuilderMultipleActions() {
        List<String> expectedActions = Arrays.asList("storage:GetObject", "storage:PutObject", "storage:ListObjects");

        Statement statement = Statement.builder()
            .sid("MultiActionStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addAction("storage:PutObject")
            .addAction("storage:ListObjects")
            .addResource("storage://multi-action-bucket/*")
            .build();

        assertEquals(expectedActions, statement.getActions());
    }

    @Test
    public void testStatementBuilderMultipleResources() {
        List<String> expectedResources = Arrays.asList("storage://bucket1/*", "storage://bucket2/*");

        Statement statement = Statement.builder()
            .sid("MultiResourceStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://bucket1/*")
            .addResource("storage://bucket2/*")
            .build();

        assertEquals(expectedResources, statement.getResources());
    }

    @Test
    public void testStatementBuilderMultiplePrincipals() {
        List<String> expectedPrincipals = Arrays.asList(
            "arn:aws:iam::123456789012:user/User1",
            "arn:aws:iam::123456789012:user/User2"
        );

        Statement statement = Statement.builder()
            .sid("MultiPrincipalStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://shared-bucket/*")
            .addPrincipal("arn:aws:iam::123456789012:user/User1")
            .addPrincipal("arn:aws:iam::123456789012:user/User2")
            .build();

        assertEquals(expectedPrincipals, statement.getPrincipals());
    }

    @Test
    public void testStatementBuilderMultipleConditions() {
        Statement statement = Statement.builder()
            .sid("MultiConditionStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://conditional-bucket/*")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .addCondition("DateGreaterThan", "aws:CurrentTime", "2024-01-01T00:00:00Z")
            .build();

        assertTrue(statement.getConditions().containsKey("StringEquals"));
        assertTrue(statement.getConditions().containsKey("DateGreaterThan"));
        assertEquals("us-west-2", statement.getConditions().get("StringEquals").get("aws:RequestedRegion"));
        assertEquals("2024-01-01T00:00:00Z", statement.getConditions().get("DateGreaterThan").get("aws:CurrentTime"));
    }

    @Test
    public void testStatementBuilderDenyEffect() {
        Statement statement = Statement.builder()
            .sid("DenyStatement")
            .effect("Deny")
            .addAction("*")
            .addResource("storage://restricted-bucket/*")
            .build();

        assertEquals("Deny", statement.getEffect());
        assertEquals(Arrays.asList("*"), statement.getActions());
    }

    @Test
    public void testStatementWithoutSid() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://no-sid-bucket/*")
            .build();

        assertNull(statement.getSid());
        assertEquals("Allow", statement.getEffect());
    }

    @Test
    public void testEmptyStatementThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            Statement.builder().build();
        });
    }

    @Test
    public void testStatementWithoutEffectThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            Statement.builder()
                .sid("NoEffectStatement")
                .addAction("storage:GetObject")
                .addResource("storage://test-bucket/*")
                .build();
        });
    }

    @Test
    public void testStatementWithoutActionsThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            Statement.builder()
                .sid("NoActionsStatement")
                .effect("Allow")
                .addResource("storage://test-bucket/*")
                .build();
        });
    }
}