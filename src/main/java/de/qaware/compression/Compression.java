package de.qaware.compression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@RequiredArgsConstructor
public enum Compression {
    NONE(""),
    GZIP(".gzip"),
    BZIP2(".bzip2"),

    ;

    @Getter
    private final String ending;

    public static Compression fromFile(Path file) {
        if (file.toFile().getName().endsWith(Compression.GZIP.getEnding())) {
            return Compression.GZIP;
        }
        if (file.toFile().getName().endsWith(Compression.BZIP2.getEnding())) {
            return Compression.BZIP2;
        }
        return Compression.NONE;
    }

    public OutputStream outputStream(OutputStream outputStream) throws IOException {
        switch (this) {
            case GZIP:
                return new GZIPOutputStream(outputStream);
            case BZIP2:
                return new BZip2CompressorOutputStream(outputStream);
            case NONE:
            default:
                return outputStream;
        }
    }

    public InputStream inputStream(InputStream inputStream) throws IOException {
        switch (this) {
            case GZIP:
                return new GZIPInputStream(inputStream);
            case BZIP2:
                return new BZip2CompressorInputStream(inputStream);
            case NONE:
            default:
                return inputStream;
        }
    }
}
