package com.salesforce.multicloudj.blob.ali;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

import static com.salesforce.multicloudj.blob.ali.ErrorCodeMapping.getException;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ErrorCodeMappingTest {
    @Test
    void testAwsMapping() {
        assertEquals(UnAuthorizedException.class, getException("AccessDenied"));
        assertEquals(InvalidArgumentException.class, getException("InvalidSecurityToken"));
        assertEquals(UnAuthorizedException.class, getException("InvalidToken"));
        assertEquals(InvalidArgumentException.class, getException("InvalidAccessKeyId"));
        assertEquals(UnknownException.class, getException("ServerError"));
        assertEquals(InvalidArgumentException.class, getException("NoSuchBucket"));
        assertEquals(InvalidArgumentException.class, getException("NoSuchKey"));
        assertEquals(InvalidArgumentException.class, getException("InvalidRequest"));
        assertEquals(UnAuthorizedException.class, getException("SignatureDoesNotMatch"));
        assertEquals(InvalidArgumentException.class, getException("BucketAlreadyExists"));
        assertEquals(InvalidArgumentException.class, getException("InvalidBucketName"));
        assertEquals(InvalidArgumentException.class, getException("MissingArgument"));
    }
}
