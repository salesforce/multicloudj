package com.salesforce.multicloudj.docstore.driver;

import com.salesforce.multicloudj.docstore.driver.testtypes.Book;
import com.salesforce.multicloudj.docstore.driver.testtypes.BookWithAnnotation;
import com.salesforce.multicloudj.docstore.driver.testtypes.TextbookWithAnnotation;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FieldCacheTest {


    @Test
    public void testFieldCache() {
        FieldCache cache = new FieldCache();
        List<FieldInfo> fields = cache.getFields(Book.class);
        assertEquals(7, fields.size());
        assertEquals("author", fields.get(0).getName());

        fields = cache.getFields(BookWithAnnotation.class);
        assertEquals(3, fields.size());
        assertEquals("Author", fields.get(0).getName());
        assertEquals(true, fields.get(1).isNameFromAnnotation());

        fields = cache.getFields(TextbookWithAnnotation.class);
        assertEquals(5, fields.size());
        assertEquals("Publisher", fields.get(2).getName());
    }
}
