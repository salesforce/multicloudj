package com.salesforce.multicloudj.docstore.driver;

import com.salesforce.multicloudj.docstore.client.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class QueryTest {

    AbstractDocStore docStore = mock(AbstractDocStore.class);

    @Test
    void testWhere() {
        Query query = new Query(docStore);
        Assertions.assertDoesNotThrow(() -> query.where("goodField", FilterOperation.EQUAL, 5));
        Assertions.assertDoesNotThrow(() -> query.where("goodField", FilterOperation.IN, List.of(1, 2, 3)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.where("badField", FilterOperation.LESS_THAN, new Object()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.where("badField", FilterOperation.IN, "InvalidCollection"));
    }

    @Test
    void testOffset() {
        Query query = new Query(docStore);
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.offset(-1));
        query.offset(10);
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.offset(8));
        PaginationToken paginationToken = mock(PaginationToken.class);
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.paginationToken(paginationToken));
    }

    @Test
    void testLimit() {
        Query query = new Query(docStore);
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.limit(-1));
        query.limit(10);
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.limit(8));
    }

    @Test
    void testPaginationToken() {
        Query query = new Query(docStore);
        PaginationToken paginationToken = mock(PaginationToken.class);
        query.paginationToken(null);
        query.paginationToken(paginationToken);
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.offset(8));
        PaginationToken paginationToken2 = mock(PaginationToken.class);
        Assertions.assertDoesNotThrow(() -> query.paginationToken(paginationToken2));
    }

    @Test
    void testOrderBy() {
        Query query = new Query(docStore);
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.orderBy(null, true));
        query.orderBy("orderBy", true);
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.orderBy("orderBy", false));
    }

    @Test
    void testBeforeQuery() {
        Query query = new Query(docStore);
        Assertions.assertDoesNotThrow(() -> query.beforeQuery(null));
    }

    @Test
    void testGet() {
        Query query = new Query(docStore);
        DocumentIterator iterator = mock(DocumentIterator.class);
        when(docStore.runGetQuery(any())).thenReturn(iterator);
        doNothing().when(docStore).checkClosed();
        Assertions.assertEquals(iterator, query.get("field"));
    }

    @Test
    void testInitGet() {
        Query query = new Query(docStore);
        doNothing().when(docStore).checkClosed();
        List<String> fieldPaths = new ArrayList<>();
        fieldPaths.add("goodField");
        fieldPaths.add("good.field");
        query.orderBy("goodField", true);
        query.where("good.field", FilterOperation.EQUAL, "value");
        Assertions.assertThrows(IllegalArgumentException.class, () -> query.initGet(fieldPaths));

        query.where("goodField", FilterOperation.EQUAL, "value");
        Assertions.assertDoesNotThrow(() -> query.initGet(fieldPaths));
    }
}
