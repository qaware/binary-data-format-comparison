package de.qaware.parquet;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.nio.file.Path;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HadoopFile {

    public static InputFile hadoopInputFile(Path path) throws IOException {
        org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(path.toUri());
        return HadoopInputFile.fromPath(tablePath, new Configuration());
    }

    public static OutputFile hadoopOutputFile(Path path) throws IOException {
        org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(path.toUri());
        return HadoopOutputFile.fromPath(tablePath, new Configuration());
    }
}
