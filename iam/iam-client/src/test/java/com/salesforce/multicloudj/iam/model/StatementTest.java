package com.salesforce.multicloudj.iam.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    @Test
    public void testStatementWithEmptyEffect() {
        assertThrows(IllegalArgumentException.class, () -> {
            Statement.builder()
                .sid("EmptyEffectStatement")
                .effect("")
                .addAction("storage:GetObject")
                .build();
        });
    }

    @Test
    public void testStatementWithWhitespaceEffect() {
        assertThrows(IllegalArgumentException.class, () -> {
            Statement.builder()
                .sid("WhitespaceEffectStatement")
                .effect("   ")
                .addAction("storage:GetObject")
                .build();
        });
    }

    @Test
    public void testNullAndEmptyValueHandling() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .addAction(null)
            .addAction("")
            .addAction("   ")
            .addAction("storage:GetObject")
            .addResource(null)
            .addResource("")
            .addResource("   ")
            .addResource("storage://test-bucket/*")
            .addPrincipal(null)
            .addPrincipal("")
            .addPrincipal("   ")
            .addPrincipal("valid-principal")
            .addCondition(null, "key", "value")
            .addCondition("StringEquals", null, "value")
            .addCondition("StringEquals", "key", null)
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .build();

        assertEquals(1, statement.getActions().size());
        assertEquals("storage:GetObject", statement.getActions().get(0));

        assertEquals(1, statement.getResources().size());
        assertEquals("storage://test-bucket/*", statement.getResources().get(0));

        assertEquals(1, statement.getPrincipals().size());
        assertEquals("valid-principal", statement.getPrincipals().get(0));

        assertEquals(1, statement.getConditions().size());
        assertTrue(statement.getConditions().containsKey("StringEquals"));
        assertEquals("us-west-2", statement.getConditions().get("StringEquals").get("aws:RequestedRegion"));
    }

    @Test
    public void testListMethodsWithNullValues() {
        List<String> principals = Arrays.asList("principal1", null, "", "   ", "principal2");
        List<String> actions = Arrays.asList("storage:GetObject", null, "", "   ", "storage:PutObject");
        List<String> resources = Arrays.asList("storage://bucket1/*", null, "", "   ", "storage://bucket2/*");

        Statement statement = Statement.builder()
            .effect("Allow")
            .addActions(actions)
            .addResources(resources)
            .addPrincipals(principals)
            .build();

        assertEquals(2, statement.getActions().size());
        assertTrue(statement.getActions().contains("storage:GetObject"));
        assertTrue(statement.getActions().contains("storage:PutObject"));

        assertEquals(2, statement.getResources().size());
        assertTrue(statement.getResources().contains("storage://bucket1/*"));
        assertTrue(statement.getResources().contains("storage://bucket2/*"));

        assertEquals(2, statement.getPrincipals().size());
        assertTrue(statement.getPrincipals().contains("principal1"));
        assertTrue(statement.getPrincipals().contains("principal2"));
    }

    @Test
    public void testListMethodsWithNullLists() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://test-bucket/*")
            .addPrincipals(null)
            .addActions(null)
            .addResources(null)
            .build();

        assertEquals(1, statement.getActions().size());
        assertEquals(1, statement.getResources().size());
        assertTrue(statement.getPrincipals().isEmpty());
    }

    @Test
    public void testStatementEqualsAndHashCode() {
        Statement statement1 = Statement.builder()
            .sid("TestStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://test-bucket/*")
            .addPrincipal("principal1")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .build();

        Statement statement2 = Statement.builder()
            .sid("TestStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://test-bucket/*")
            .addPrincipal("principal1")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .build();

        Statement statement3 = Statement.builder()
            .sid("DifferentStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://test-bucket/*")
            .build();

        // Test equals
        assertEquals(statement1, statement2);
        assertNotEquals(statement1, statement3);
        assertNotEquals(statement1, null);
        assertNotEquals(statement1, "not a statement");
        assertEquals(statement1, statement1); // same object

        // Test hashCode
        assertEquals(statement1.hashCode(), statement2.hashCode());
        assertNotEquals(statement1.hashCode(), statement3.hashCode());
    }

    @Test
    public void testStatementToString() {
        Statement statement = Statement.builder()
            .sid("TestStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://test-bucket/*")
            .addPrincipal("principal1")
            .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .build();

        String result = statement.toString();
        assertTrue(result.contains("TestStatement"));
        assertTrue(result.contains("Allow"));
        assertTrue(result.contains("storage:GetObject"));
        assertTrue(result.contains("storage://test-bucket/*"));
        assertTrue(result.contains("principal1"));
        assertTrue(result.contains("StringEquals"));
    }

}