package com.mongodb.util;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.*;
import java.util.zip.GZIPOutputStream;

/**
 * A rolling log file utility that automatically rolls existing files with timestamps
 * before creating new ones. Supports optional compression of rolled files.
 */
public class RollingLogFile {
    private final Path baseFile;
    private final boolean compress;

    /**
     * Creates a new RollingLogFile instance.
     * 
     * @param baseFileName The base file name for the log file
     * @param compress Whether to compress rolled files with gzip
     * @throws IOException If there's an error during file operations
     */
    public RollingLogFile(String baseFileName, boolean compress) throws IOException {
        this.baseFile = Paths.get(baseFileName);
        this.compress = compress;
        rollIfExists();
    }

    /**
     * Rolls the existing file if it exists by adding a timestamp suffix.
     * If compression is enabled, the rolled file will be gzipped.
     */
    private void rollIfExists() throws IOException {
        if (Files.exists(baseFile)) {
            String ts = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm")
                                         .format(LocalDateTime.now());
            
            String baseFileName = baseFile.getFileName().toString();
            String rolledFileName;
            
            // Insert timestamp before the file extension, then add .gz at the end if compressing
            int lastDotIndex = baseFileName.lastIndexOf('.');
            if (lastDotIndex > 0) {
                String nameWithoutExt = baseFileName.substring(0, lastDotIndex);
                String extension = baseFileName.substring(lastDotIndex);
                rolledFileName = nameWithoutExt + "_" + ts + extension + (compress ? ".gz" : "");
            } else {
                rolledFileName = baseFileName + "_" + ts + (compress ? ".gz" : "");
            }
            
            Path rolled = baseFile.resolveSibling(rolledFileName);

            if (compress) {
                try (InputStream in = Files.newInputStream(baseFile);
                     OutputStream out = new GZIPOutputStream(Files.newOutputStream(rolled))) {
                    in.transferTo(out);
                }
                Files.delete(baseFile);
            } else {
                Files.move(baseFile, rolled);
            }
        }
    }

    /**
     * Opens the base file for writing, creating it if it doesn't exist.
     * 
     * @return An OutputStream for writing to the file
     * @throws IOException If there's an error opening the file
     */
    public OutputStream openForWrite() throws IOException {
        // Ensure the parent directory exists
        Path parentDir = baseFile.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }
        
        return Files.newOutputStream(baseFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }

    /**
     * Gets the path to the base file.
     * 
     * @return The Path to the base file
     */
    public Path getBaseFile() {
        return baseFile;
    }

    /**
     * Checks if compression is enabled for rolled files.
     * 
     * @return true if compression is enabled, false otherwise
     */
    public boolean isCompressionEnabled() {
        return compress;
    }
}