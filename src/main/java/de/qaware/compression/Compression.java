package de.qaware.compression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

@RequiredArgsConstructor
public enum Compression {
    NONE(""),
    GZIP(".gzip"),
    BZIP2(".bzip2"),

    ;

    @Getter
    private final String ending;

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
}
