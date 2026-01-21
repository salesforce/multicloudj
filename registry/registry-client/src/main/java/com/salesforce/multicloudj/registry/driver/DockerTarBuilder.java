package com.salesforce.multicloudj.registry.driver;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Builds a Docker image tar file from manifest, config, and layers.
 * Follows the Docker image tar format specification.
 */
class DockerTarBuilder {
    private final Path destination;  // May be null if using OutputStream
    private final TarArchiveOutputStream tarOut;
    private final boolean closeOnClose;  // Whether to close the underlying stream

    /**
     * Creates a DockerTarBuilder that writes to a Path (file).
     */
    DockerTarBuilder(Path destination) throws IOException {
        this.destination = destination;
        // Docker images are typically saved as .tar files (not .tar.gz)
        // But we'll support both based on file extension
        OutputStream fileOut = Files.newOutputStream(destination);
        
        if (destination.toString().endsWith(".gz") || destination.toString().endsWith(".tgz")) {
            this.tarOut = new TarArchiveOutputStream(new GzipCompressorOutputStream(fileOut));
        } else {
            this.tarOut = new TarArchiveOutputStream(fileOut);
        }
        
        tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
        this.closeOnClose = true;
    }

    /**
     * Creates a DockerTarBuilder that writes to an OutputStream.
     * The caller is responsible for closing the OutputStream.
     */
    DockerTarBuilder(OutputStream outputStream, boolean gzip) throws IOException {
        this.destination = null;
        if (gzip) {
            this.tarOut = new TarArchiveOutputStream(new GzipCompressorOutputStream(outputStream));
        } else {
            this.tarOut = new TarArchiveOutputStream(outputStream);
        }
        tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
        this.closeOnClose = false;  // Don't close user's stream
    }

    /**
     * Adds the manifest.json file to the tar.
     */
    void addManifest(String manifestJson) throws IOException {
        TarArchiveEntry entry = new TarArchiveEntry("manifest.json");
        byte[] data = manifestJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        entry.setSize(data.length);
        tarOut.putArchiveEntry(entry);
        tarOut.write(data);
        tarOut.closeArchiveEntry();
    }

    /**
     * Adds a layer tar file to the image tar.
     * Each layer is a separate tar file within the image tar.
     */
    void addLayer(String layerId, InputStream layerData) throws IOException {
        String layerPath = layerId + "/layer.tar";
        TarArchiveEntry entry = new TarArchiveEntry(layerPath);
        
        // Read layer data to get size
        byte[] buffer = new byte[8192];
        long size = 0;
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        int bytesRead;
        while ((bytesRead = layerData.read(buffer)) != -1) {
            baos.write(buffer, 0, bytesRead);
            size += bytesRead;
        }
        
        entry.setSize(size);
        tarOut.putArchiveEntry(entry);
        baos.writeTo(tarOut);
        tarOut.closeArchiveEntry();
    }

    /**
     * Adds the config.json file for a layer.
     */
    void addConfig(String layerId, String configJson) throws IOException {
        String configPath = layerId + "/json";
        TarArchiveEntry entry = new TarArchiveEntry(configPath);
        byte[] data = configJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        entry.setSize(data.length);
        tarOut.putArchiveEntry(entry);
        tarOut.write(data);
        tarOut.closeArchiveEntry();
    }

    /**
     * Adds the VERSION file (legacy Docker format).
     */
    void addVersion() throws IOException {
        TarArchiveEntry entry = new TarArchiveEntry("VERSION");
        String version = "1.0";
        byte[] data = version.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        entry.setSize(data.length);
        tarOut.putArchiveEntry(entry);
        tarOut.write(data);
        tarOut.closeArchiveEntry();
    }

    void close() throws IOException {
        tarOut.close();
        // Note: If using OutputStream constructor, we don't close the underlying stream
        // The caller is responsible for closing it
    }

    /**
     * Gets the underlying TarArchiveOutputStream for advanced usage.
     */
    TarArchiveOutputStream getTarOutputStream() {
        return tarOut;
    }
}
