package com.salesforce.multicloudj.iam.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PolicyDocument builder pattern.
 */
public class PolicyDocumentTest {

    @Test
    public void testPolicyDocumentBuilder() {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2024-01-01")
            .statement("StorageAccess")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addAction("storage:PutObject")
                .addPrincipal("arn:aws:iam::123456789012:user/ExampleUser")
                .addResource("storage://my-bucket/*")
                .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .endStatement()
            .build();

        assertEquals("2024-01-01", policy.getVersion());
        assertEquals(1, policy.getStatements().size());

        Statement statement = policy.getStatements().get(0);
        assertEquals("StorageAccess", statement.getSid());
        assertEquals("Allow", statement.getEffect());
        assertEquals(Arrays.asList("storage:GetObject", "storage:PutObject"), statement.getActions());
        assertEquals(Arrays.asList("arn:aws:iam::123456789012:user/ExampleUser"), statement.getPrincipals());
        assertEquals(Arrays.asList("storage://my-bucket/*"), statement.getResources());

        assertTrue(statement.getConditions().containsKey("StringEquals"));
        assertEquals("us-west-2", statement.getConditions().get("StringEquals").get("aws:RequestedRegion"));
    }

    @Test
    public void testMultipleStatements() {
        PolicyDocument policy = PolicyDocument.builder()
            .statement("ReadAccess")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://my-bucket/*")
            .endStatement()
            .statement("WriteAccess")
                .effect("Allow")
                .addAction("storage:PutObject")
                .addResource("storage://my-bucket/*")
            .endStatement()
            .build();

        assertEquals(2, policy.getStatements().size());
        assertEquals("ReadAccess", policy.getStatements().get(0).getSid());
        assertEquals("WriteAccess", policy.getStatements().get(1).getSid());
    }

    @Test
    public void testAddCompleteStatement() {
        Statement statement = Statement.builder()
            .sid("TestStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://my-bucket/*")
            .build();

        PolicyDocument policy = PolicyDocument.builder()
            .addStatement(statement)
            .build();

        assertEquals(1, policy.getStatements().size());
        assertEquals("TestStatement", policy.getStatements().get(0).getSid());
    }

    @Test
    public void testEmptyPolicyThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            PolicyDocument.builder().build();
        });
    }

    @Test
    public void testStatementWithoutEffectThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            PolicyDocument.builder()
                .statement("TestStatement")
                    .addAction("storage:GetObject")
                .endStatement()
                .build();
        });
    }

    @Test
    public void testStatementWithoutActionsThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            PolicyDocument.builder()
                .statement("TestStatement")
                    .effect("Allow")
                .endStatement()
                .build();
        });
    }
}