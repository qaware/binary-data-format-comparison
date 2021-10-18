package de.qaware.parquet.group;

import de.qaware.api.TestReader;
import de.qaware.parquet.HadoopFile;
import lombok.RequiredArgsConstructor;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class GroupTestReader implements TestReader {
    private final Path inputFile;

    @Override
    public List<Group> read() throws IOException {
        ParquetReadOptions parquetReadOptions = ParquetReadOptions.builder().build();
        InputFile file = HadoopFile.hadoopInputFile(inputFile);
        try (ParquetFileReader reader = ParquetFileReader.open(file, parquetReadOptions)) {

            MessageType schema = reader.getFileMetaData().getSchema();
            MessageColumnIO columnIo = new ColumnIOFactory().getColumnIO(schema);

            List<Group> result = new ArrayList<>();
            PageReadStore pages;
            while (null != (pages = reader.readNextRowGroup())) {
                final long rows = pages.getRowCount();

                RecordReader<Group> recordReader = columnIo.getRecordReader(
                        pages,
                        new GroupRecordConverter(schema)
                );
                for (var i = 0; i < rows; i++) {
                    Group g = recordReader.read();
                    result.add(g);
                }
            }

            return result;
        }
    }

    @Override
    public Path getInputFile() {
        return inputFile;
    }
}
