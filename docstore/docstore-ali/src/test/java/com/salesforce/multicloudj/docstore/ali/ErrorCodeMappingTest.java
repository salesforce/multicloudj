package com.salesforce.multicloudj.docstore.ali;

import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

import static com.salesforce.multicloudj.docstore.ali.ErrorCodeMapping.getException;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ErrorCodeMappingTest {
    @Test
    void testAErrorCodeMapping() {
        assertEquals(UnAuthorizedException.class, getException("OTSAuthFailed"));
        assertEquals(UnAuthorizedException.class, getException("OTSNoPermissionAccess"));
        assertEquals(InvalidArgumentException.class, getException("OTSRequestBodyTooLarge"));
        assertEquals(ResourceExhaustedException.class, getException("OTSRequestTimeout"));
        assertEquals(InvalidArgumentException.class, getException("OTSMethodNotAllowed"));
        assertEquals(UnknownException.class, getException("OTSInternalServerError"));
        assertEquals(ResourceExhaustedException.class, getException("OTSQuotaExhausted"));
        assertEquals(InvalidArgumentException.class, getException("OTSInvalidPK"));
        assertEquals(ResourceExhaustedException.class, getException("OTSOutOfRowSizeLimit"));
        assertEquals(ResourceExhaustedException.class, getException("OTSOutOfColumnCountLimit"));
        assertEquals(UnknownException.class, getException("OTSServerBusy"));
        assertEquals(DeadlineExceededException.class, getException("OTSTimeout"));
        assertEquals(UnknownException.class, getException("OTSServerUnavailable"));
        assertEquals(UnknownException.class, getException("OTSServerUnavailable"));
        assertEquals(UnknownException.class, getException("OTSPartitionUnavailable"));
        assertEquals(FailedPreconditionException.class, getException("OTSRowOperationConflict"));
        assertEquals(ResourceAlreadyExistsException.class, getException("OTSObjectAlreadyExist"));
        assertEquals(ResourceNotFoundException.class, getException("OTSObjectNotExist"));
        assertEquals(ResourceNotFoundException.class, getException("OTSTableNotReady"));
    }
}
