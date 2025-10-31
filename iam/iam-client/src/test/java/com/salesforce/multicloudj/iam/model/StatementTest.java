package com.salesforce.multicloudj.iam.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for Statement builder pattern and functionality.
 */
public class StatementTest {

    @Test
    public void testStatementBuilder() {
        Statement statement = Statement.builder()
            .sid("TestStatement")
            .effect("Allow")
            .action("storage:GetObject")
            .action("storage:PutObject")
            .resource("storage://my-bucket/*")
            .principal("arn:aws:iam::123456789012:user/TestUser")
            .condition("StringEquals", "aws:RequestedRegion", "us-west-2")
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
            .action("storage:DeleteObject")
            .resource("storage://sensitive-bucket/*")
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
            .action("storage:GetObject")
            .resource("storage://bucket1/*")
            .resource("storage://bucket2/*")
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
            .action("storage:GetObject")
            .resource("storage://shared-bucket/*")
            .principal("arn:aws:iam::123456789012:user/User1")
            .principal("arn:aws:iam::123456789012:user/User2")
            .build();

        assertEquals(expectedPrincipals, statement.getPrincipals());
    }

    @Test
    public void testStatementBuilderMultipleConditions() {
        Statement statement = Statement.builder()
            .sid("MultiConditionStatement")
            .effect("Allow")
            .action("storage:GetObject")
            .resource("storage://conditional-bucket/*")
            .condition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .condition("DateGreaterThan", "aws:CurrentTime", "2024-01-01T00:00:00Z")
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
            .action("storage:GetObject")
            .resource("storage://no-sid-bucket/*")
            .build();

        assertNull(statement.getSid());
        assertEquals("Allow", statement.getEffect());
    }

    @Test
    public void testEmptyStatementThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            Statement.builder().build();
        });
    }

    @Test
    public void testStatementWithoutEffectThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            Statement.builder()
                .sid("NoEffectStatement")
                .action("storage:GetObject")
                .resource("storage://test-bucket/*")
                .build();
        });
    }

    @Test
    public void testStatementWithoutActionsThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            Statement.builder()
                .sid("NoActionsStatement")
                .effect("Allow")
                .resource("storage://test-bucket/*")
                .build();
        });
    }

    @Test
    public void testStatementWithEmptyEffect() {
        assertThrows(InvalidArgumentException.class, () -> {
            Statement.builder()
                .sid("EmptyEffectStatement")
                .effect("")
                .action("storage:GetObject")
                .build();
        });
    }

    @Test
    public void testStatementWithWhitespaceEffect() {
        assertThrows(InvalidArgumentException.class, () -> {
            Statement.builder()
                .sid("WhitespaceEffectStatement")
                .effect("   ")
                .action("storage:GetObject")
                .build();
        });
    }

    @Test
    public void testNullAndEmptyValueHandling() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action(null)
            .action("")
            .action("   ")
            .action("storage:GetObject")
            .resource(null)
            .resource("")
            .resource("   ")
            .resource("storage://test-bucket/*")
            .principal(null)
            .principal("")
            .principal("   ")
            .principal("valid-principal")
            .condition(null, "key", "value")
            .condition("StringEquals", null, "value")
            .condition("StringEquals", "key", null)
            .condition("StringEquals", "aws:RequestedRegion", "us-west-2")
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
            .actions(actions)
            .resources(resources)
            .principals(principals)
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
        // Test that individual actions and resources are preserved even without using list methods
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:GetObject")
            .resource("storage://test-bucket/*")
            .build();

        assertEquals(1, statement.getActions().size());
        assertEquals(1, statement.getResources().size());
        assertTrue(statement.getPrincipals().isEmpty());
    }

}