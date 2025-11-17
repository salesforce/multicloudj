package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.iam.driver.AbstractIam;

/**
 * Test helper class for creating IamClient instances in tests.
 * This class is in the same package as IamClient, so it can access the protected constructor.
 */
public class TestIamClient {
    
    /**
     * Creates an IamClient instance for testing purposes.
     * 
     * @param iam the AbstractIam driver to use
     * @return a new IamClient instance
     */
    public static IamClient create(AbstractIam iam) {
        return new IamClient(iam);
    }
    
    private TestIamClient() {
        // Utility class, prevent instantiation
    }
}
