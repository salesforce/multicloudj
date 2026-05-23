package com.salesforce.multicloudj.docstore.driver;

import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ActionTest {
  @Test
  void testAction() {
    Document document = mock(Document.class);
    Action action =
        new Action(ActionKind.ACTION_KIND_CREATE, document, List.of("path"), null, false);
    Assertions.assertEquals(ActionKind.ACTION_KIND_CREATE, action.getKind());
  }
}
