package com.salesforce.multicloudj.dbbackrestore.ali;

import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

import static com.salesforce.multicloudj.dbbackrestore.ali.ErrorCodeMapping.getException;
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
