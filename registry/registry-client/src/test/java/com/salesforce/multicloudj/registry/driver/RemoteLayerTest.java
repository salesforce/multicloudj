package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
public class RemoteLayerTest {

    private static final String REPOSITORY = "test-repo/test-image";
    private static final String DIGEST = "sha256:abc123";

    @Mock
    private OciRegistryClient mockClient;

    private RemoteLayer remoteLayer;

    @BeforeEach
    void setUp() {
        remoteLayer = new RemoteLayer(mockClient, REPOSITORY, DIGEST);
    }

    @Test
    void testGetUncompressed_DecompressesGzipData() throws Exception {
        String originalData = "This is test data for gzip compression";
        byte[] gzipData = createGzipData(originalData);
        InputStream compressedStream = new ByteArrayInputStream(gzipData);

        when(mockClient.downloadBlob(REPOSITORY, DIGEST)).thenReturn(compressedStream);

        InputStream uncompressed = remoteLayer.getUncompressed();

        assertNotNull(uncompressed);
        assertTrue(uncompressed instanceof GzipCompressorInputStream);

        // Read and verify the decompressed data
        String decompressedData = new String(uncompressed.readAllBytes());
        assertEquals(originalData, decompressedData);
    }

    @Test
    void testGetUncompressed_ThrowsException_WhenDownloadBlobFails() throws Exception {
        Answer<InputStream> throwIo = inv -> { throw new IOException("Network error"); };
        doAnswer(throwIo).when(mockClient).downloadBlob(REPOSITORY, DIGEST);

        UnknownException exception = assertThrows(UnknownException.class,
                () -> remoteLayer.getUncompressed());

        assertTrue(exception.getMessage().contains("Failed to decompress layer"));
        assertNotNull(exception.getCause());
        assertEquals(IOException.class, exception.getCause().getClass());
    }

    @Test
    void testGetUncompressed_ThrowsAndClosesStream_WhenGzipDecompressionFails() throws Exception {
        CloseTrackingInputStream trackingStream = new CloseTrackingInputStream("not gzip data".getBytes());
        when(mockClient.downloadBlob(REPOSITORY, DIGEST)).thenReturn(trackingStream);

        UnknownException exception = assertThrows(UnknownException.class,
                () -> remoteLayer.getUncompressed());

        assertTrue(exception.getMessage().contains("Failed to decompress layer"));
        assertNotNull(exception.getCause());
        assertEquals(IOException.class, exception.getCause().getClass());
        assertTrue(trackingStream.wasClosed(), "Compressed stream should be closed on error");
    }

    /**
     * Creates gzip-compressed data from a string.
     */
    private byte[] createGzipData(String data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(data.getBytes());
        }
        return baos.toByteArray();
    }

    /**
     * Custom InputStream that tracks if it was closed.
     */
    private static class CloseTrackingInputStream extends ByteArrayInputStream {
        private boolean closed = false;

        CloseTrackingInputStream(byte[] data) {
            super(data);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }

        boolean wasClosed() {
            return closed;
        }
    }
}
