package com.salesforce.multicloudj.docstore.driver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;

public class ActionTest {
    @Test
    void testAction() {
        Document document = mock(Document.class);
        Action action = new Action(ActionKind.ACTION_KIND_CREATE, document, List.of("path"), null, false);
        Assertions.assertEquals(ActionKind.ACTION_KIND_CREATE, action.getKind());
    }
}
