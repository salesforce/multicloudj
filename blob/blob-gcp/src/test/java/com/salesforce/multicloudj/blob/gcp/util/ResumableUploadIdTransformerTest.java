package com.salesforce.multicloudj.blob.gcp.util;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for ResumableUploadIdTransformer to verify it correctly transforms
 * resumable upload URLs with upload_id parameters.
 */
public class ResumableUploadIdTransformerTest {

    @Test
    public void testTransformResumableUploadWithUploadId() {
        ResumableUploadIdTransformer transformer = new ResumableUploadIdTransformer();
        
        // Create a stub mapping with a resumable upload URL containing upload_id
        String originalUrl = "/upload/storage/v1/b/test-bucket/o?name=test&uploadType=resumable&upload_id=AHVrFxP498RdLzh199N6DM2-mQs6VgQWStbFNwJc832D10rpVroiwapqkWWWrEOcBAvGJHEz";
        // Create a RequestPattern using the builder - get method from a temporary pattern
        RequestPattern tempPattern = putRequestedFor(urlEqualTo(originalUrl)).build();
        RequestPattern requestPattern = RequestPatternBuilder.newRequestPattern(
                tempPattern.getMethod(),
                urlEqualTo(originalUrl)
        ).build();
        StubMapping stubMapping = new StubMapping(requestPattern, aResponse().withStatus(200).build());
        
        // Transform the stub
        StubMapping transformed = transformer.transform(stubMapping, null, Parameters.empty());
        
        // Verify the transformation
        RequestPattern transformedPattern = transformed.getRequest();
        String urlPattern = transformedPattern.getUrlPattern();
        String url = transformedPattern.getUrl();
        
        // The transformer should have converted the exact URL to a URL pattern
        assertNotNull(urlPattern, "URL pattern should be set after transformation: " + urlPattern);
        assertTrue(urlPattern.contains("upload_id=[^&]+"), 
                "URL pattern should contain regex pattern for upload_id: " + urlPattern);
        assertFalse(urlPattern.contains("upload_id=AHVrFxP498RdLzh199N6DM2"), 
                "URL pattern should not contain the exact upload_id value");
    }
    
    @Test
    public void testTransformDoesNotModifyNonResumableUploads() {
        ResumableUploadIdTransformer transformer = new ResumableUploadIdTransformer();
        
        // Create a stub mapping with a non-resumable upload URL
        String originalUrl = "/storage/v1/b/test-bucket/o/test-object";
        RequestPattern tempPattern = getRequestedFor(urlEqualTo(originalUrl)).build();
        RequestPattern requestPattern = RequestPatternBuilder.newRequestPattern(
                tempPattern.getMethod(),
                urlEqualTo(originalUrl)
        ).build();
        StubMapping stubMapping = new StubMapping(requestPattern, aResponse().withStatus(200).build());
        
        // Transform the stub
        StubMapping transformed = transformer.transform(stubMapping, null, Parameters.empty());
        
        // Verify it wasn't modified
        RequestPattern transformedPattern = transformed.getRequest();
        assertEquals(originalUrl, transformedPattern.getUrl(), 
                "Non-resumable upload URLs should not be modified");
    }
    
    @Test
    public void testTransformResumableUploadWithoutUploadId() {
        ResumableUploadIdTransformer transformer = new ResumableUploadIdTransformer();
        
        // Create a stub mapping with a resumable upload URL but no upload_id (initial request)
        String originalUrl = "/upload/storage/v1/b/test-bucket/o?name=test&uploadType=resumable";
        RequestPattern tempPattern = postRequestedFor(urlEqualTo(originalUrl)).build();
        RequestPattern requestPattern = RequestPatternBuilder.newRequestPattern(
                tempPattern.getMethod(),
                urlEqualTo(originalUrl)
        ).build();
        StubMapping stubMapping = new StubMapping(requestPattern, aResponse().withStatus(200).build());
        
        // Transform the stub
        StubMapping transformed = transformer.transform(stubMapping, null, Parameters.empty());
        
        // Verify it wasn't modified (no upload_id to transform)
        RequestPattern transformedPattern = transformed.getRequest();
        assertEquals(originalUrl, transformedPattern.getUrl(), 
                "Resumable upload URLs without upload_id should not be modified");
    }
}

