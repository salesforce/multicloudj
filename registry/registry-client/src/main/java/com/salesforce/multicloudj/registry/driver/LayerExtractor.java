package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.registry.model.Layer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extracts and flattens OCI image layers into a single tar stream.
 * 
 * <p>Implements the OCI layer flattening algorithm:
 * <ul>
 *   <li>Layers are processed in order (bottom to top)</li>
 *   <li>Whiteout files (.wh.{name}) mark deletions of {name} from lower layers</li>
 *   <li>Opaque whiteouts (.wh..wh..opq) indicate entire directory replacement</li>
 *   <li>Later layers override earlier layers for the same path</li>
 * </ul>
 * 
 * @see <a href="https://github.com/opencontainers/image-spec/blob/main/layer.md">OCI Layer Specification</a>
 */
final class LayerExtractor {

    private static final String WHITEOUT_PREFIX = ".wh.";
    private static final String OPAQUE_WHITEOUT = ".wh..wh..opq";
    private static final char PATH_SEPARATOR_CHAR = '/';
    private static final String PATH_SEPARATOR = "/";
    private static final String CURRENT_DIR_PREFIX = "./";
    private static final String THREAD_NAME = "layer-extractor";
    private static final int PIPE_BUFFER_SIZE = 65536;
    private static final int COPY_BUFFER_SIZE = 8192;

    private final List<Layer> layers;

    LayerExtractor(List<Layer> layers) {
        this.layers = layers;
    }

    /**
     * Extracts all layers into a flattened tar stream.
     * 
     * <p>The extraction runs in a background thread, writing to a piped stream.
     * The returned InputStream can be consumed by the caller.
     *
     * @return InputStream of the flattened filesystem tar
     * @throws UnknownException if extraction setup fails
     */
    public InputStream extract() {
        PipedInputStream pipedIn = new PipedInputStream(PIPE_BUFFER_SIZE);
        PipedOutputStream pipedOut;
        try {
            pipedOut = new PipedOutputStream(pipedIn);
        } catch (IOException e) {
            throw new UnknownException("Failed to set up extraction pipe", e);
        }
        
        AtomicReference<Throwable> extractionError = new AtomicReference<>();
        
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, THREAD_NAME);
            t.setDaemon(true);
            return t;
        });
        
        executor.submit(() -> {
            try (TarArchiveOutputStream tarOut = new TarArchiveOutputStream(pipedOut)) {
                tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
                
                // Track files to handle whiteouts and overwrites
                Set<String> seenPaths = new HashSet<>();
                Set<String> deletedPaths = new HashSet<>();
                Set<String> opaqueDirectories = new HashSet<>();
                
                // Process layers in reverse order (top to bottom) - most recent layer first
                // This ensures that the top layer's files take precedence
                for (int i = layers.size() - 1; i >= 0; i--) {
                    processLayer(layers.get(i), tarOut, seenPaths, deletedPaths, opaqueDirectories);
                }
                
                tarOut.finish();
            } catch (Throwable t) {
                extractionError.set(t);
            } finally {
                try {
                    pipedOut.close();
                } catch (IOException ignored) {
                    // Ignore close errors
                }
            }
        });
        
        // Return a wrapper that checks for extraction errors and cleans up executor on close
        return new ExtractionInputStream(pipedIn, extractionError, executor);
    }

    /**
     * Processes a single layer, applying whiteout rules.
     */
    private void processLayer(Layer layer, TarArchiveOutputStream tarOut,
                              Set<String> seenPaths, Set<String> deletedPaths,
                              Set<String> opaqueDirectories) throws IOException {
        
        try (InputStream layerStream = layer.getUncompressed();
             TarArchiveInputStream tarIn = new TarArchiveInputStream(layerStream)) {
            
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                String name = normalizePath(entry.getName());
                
                if (handleWhiteout(name, deletedPaths, opaqueDirectories)) {
                    continue;
                }
                
                if (shouldSkipEntry(name, seenPaths, deletedPaths, opaqueDirectories)) {
                    continue;
                }
                
                seenPaths.add(name);
                writeEntry(tarOut, tarIn, entry, name);
            }
        }
    }

    private boolean handleWhiteout(String name, Set<String> deletedPaths, Set<String> opaqueDirectories) {
        String baseName = getBaseName(name);
        String parentDir = getParentDir(name);
        
        if (baseName.equals(OPAQUE_WHITEOUT)) {
            opaqueDirectories.add(parentDir);
            return true;
        }
        
        if (baseName.startsWith(WHITEOUT_PREFIX)) {
            deletedPaths.add(parentDir + baseName.substring(WHITEOUT_PREFIX.length()));
            return true;
        }
        
        return false;
    }

    private boolean shouldSkipEntry(String name, Set<String> seenPaths, 
                                    Set<String> deletedPaths, Set<String> opaqueDirectories) {
        return deletedPaths.contains(name) 
                || isUnderOpaqueDir(name, opaqueDirectories) 
                || seenPaths.contains(name);
    }

    private void writeEntry(TarArchiveOutputStream tarOut, TarArchiveInputStream tarIn,
                            TarArchiveEntry entry, String name) throws IOException {
        TarArchiveEntry outEntry = createOutputEntry(entry, name);
        tarOut.putArchiveEntry(outEntry);
        
        if (!entry.isDirectory() && entry.getSize() > 0) {
            copyStream(tarIn, tarOut, entry.getSize());
        }
        
        tarOut.closeArchiveEntry();
    }

    private TarArchiveEntry createOutputEntry(TarArchiveEntry entry, String name) {
        TarArchiveEntry outEntry = new TarArchiveEntry(name);
        outEntry.setSize(entry.getSize());
        outEntry.setMode(entry.getMode());
        outEntry.setModTime(entry.getModTime());
        outEntry.setUserId(entry.getLongUserId());
        outEntry.setGroupId(entry.getLongGroupId());
        outEntry.setUserName(entry.getUserName());
        outEntry.setGroupName(entry.getGroupName());
        
        if (entry.isSymbolicLink() || entry.isLink()) {
            outEntry.setLinkName(entry.getLinkName());
        }
        
        return outEntry;
    }

    private String normalizePath(String path) {
        if (path.startsWith(CURRENT_DIR_PREFIX)) {
            path = path.substring(CURRENT_DIR_PREFIX.length());
        }
        if (path.endsWith(PATH_SEPARATOR) && path.length() > 1) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    private String getBaseName(String path) {
        int lastSlash = path.lastIndexOf(PATH_SEPARATOR_CHAR);
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    }

    private String getParentDir(String path) {
        int lastSlash = path.lastIndexOf(PATH_SEPARATOR_CHAR);
        return lastSlash >= 0 ? path.substring(0, lastSlash + 1) : StringUtils.EMPTY;
    }

    private boolean isUnderOpaqueDir(String path, Set<String> opaqueDirectories) {
        return opaqueDirectories.stream()
                .anyMatch(opaqueDir -> !opaqueDir.isEmpty() 
                        && path.startsWith(opaqueDir) 
                        && !path.equals(opaqueDir.substring(0, opaqueDir.length() - 1)));
    }

    private void copyStream(InputStream in, TarArchiveOutputStream out, long size) throws IOException {
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        long remaining = size;
        while (remaining > 0) {
            int toRead = (int) Math.min(buffer.length, remaining);
            int read = in.read(buffer, 0, toRead);
            if (read < 0) {
                break;
            }
            out.write(buffer, 0, read);
            remaining -= read;
        }
        if (remaining > 0) {
            throw new UnknownException(String.format(
                    "Layer stream ended prematurely: expected %d bytes but only received %d", size, size - remaining));
        }
    }

    /** InputStream wrapper that propagates extraction errors and cleans up executor on close. */
    private static class ExtractionInputStream extends InputStream {
        private final PipedInputStream delegate;
        private final AtomicReference<Throwable> error;
        private final ExecutorService executor;

        ExtractionInputStream(PipedInputStream delegate, AtomicReference<Throwable> error, ExecutorService executor) {
            this.delegate = delegate;
            this.error = error;
            this.executor = executor;
        }

        @Override
        public int read() throws IOException {
            return checkErrorAfterRead(delegate.read());
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return checkErrorAfterRead(delegate.read(b, off, len));
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            } finally {
                executor.shutdownNow();
            }
        }

        private int checkErrorAfterRead(int result) throws IOException {
            Throwable t = error.get();
            if (t != null) {
                if (t instanceof IOException) {
                    throw (IOException) t;
                }
                throw new UnknownException("Layer extraction failed", t);
            }
            return result;
        }
    }
}
