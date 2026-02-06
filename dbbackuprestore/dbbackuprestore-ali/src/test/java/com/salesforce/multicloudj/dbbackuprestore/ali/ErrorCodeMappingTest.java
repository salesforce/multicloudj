package com.salesforce.multicloudj.dbbackuprestore.ali;

import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

import static com.salesforce.multicloudj.dbbackuprestore.ali.ErrorCodeMapping.getException;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for Alibaba ErrorCodeMapping.
 */
public class ErrorCodeMappingTest {

    @Test
    void testGetException() {
        assertEquals(UnknownException.class, getException(new Exception("EntityNotExist: Resource not found")));
    }
}
