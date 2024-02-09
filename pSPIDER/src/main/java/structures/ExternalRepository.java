package structures;

import io.ReadPointer;
import io.RelationalFileInput;
import runner.Config;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class ExternalRepository {

    public long tableLength = 0;
    public long[] nullCounts;
    private int count;

    public Path[] store(RelationalFileInput table) throws IOException {

        final Path[] paths = new Path[table.numberOfColumns()];
        final BufferedWriter[] writers = new BufferedWriter[table.numberOfColumns()];
        openForWriting(paths, writers);
        this.nullCounts = write(table, writers);
        close(writers);
        return paths;
    }

    private void openForWriting(final Path[] paths, final BufferedWriter[] writers) throws IOException {
        for (int index = 0; index < paths.length; ++index) {
            final Path path = getPath();
            paths[index] = path;
            writers[index] = Files.newBufferedWriter(path);
        }

    }

    private long[] write(final RelationalFileInput file, final BufferedWriter[] writers) throws IOException {

        long[] nullCounts = new long[writers.length];
        tableLength = 0L;
        while (file.hasNext()) {
            tableLength++;
            final List<String> next = file.next();
            for (int index = 0; index < writers.length; index++) {
                final String value = index >= next.size() ? null : next.get(index);
                if (value != null) {
                    writers[index].write(escape(value));
                    writers[index].newLine();
                } else {
                    nullCounts[index]++;
                }
            }
        }
        return nullCounts;
    }

    private void close(final Closeable[] toClose) throws IOException {
        for (final Closeable item : toClose) {
            item.close();
        }
    }

    private ReadPointer[] open(final Path[] paths) throws IOException {
        final ReadPointer[] result = new ReadPointer[paths.length];
        for (int index = 0; index < paths.length; index++) {
            result[index] = new ReadPointer(paths[index]);
        }
        return result;
    }

    private String escape(final String value) {
        return value.replace('\n', '\0');
    }

    private Path getPath() {
        this.count++;
        File tempFile = new File(".\\temp\\attribute_" + count + ".txt");
        return tempFile.toPath();
    }
}
