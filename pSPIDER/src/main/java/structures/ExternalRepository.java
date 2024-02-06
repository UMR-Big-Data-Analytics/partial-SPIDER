package structures;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.util.TPMMSConfiguration;
import io.ReadPointer;
import io.RelationalFileInput;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class ExternalRepository {

    public long tableLength = 0;
    private int count;

    public ReadPointer[] uniqueAndSort(final TPMMSConfiguration configuration, RelationalFileInput table) throws AlgorithmExecutionException, IOException {
        tableLength = 0;
        final Path[] paths = store(table);
        // can we get away without storing the tables to new files first?
        try {
            uniqueAndSort(paths, configuration);
            return open(paths);
        } catch (final IOException e) {
            throw new AlgorithmExecutionException("MultiwayMergeSort failure", e);
        }
    }

    private void uniqueAndSort(final Path[] paths, final TPMMSConfiguration configuration)
            throws IOException {

        final MultiwayMergeSort multiwayMergeSort = new MultiwayMergeSort(configuration);
        for (final Path path : paths) {
            multiwayMergeSort.uniqueAndSort(path);
        }
    }

    private Path[] store(RelationalFileInput table) throws AlgorithmExecutionException, IOException {

        final Path[] paths = new Path[table.numberOfColumns()];
        final BufferedWriter[] writers = new BufferedWriter[table.numberOfColumns()];
        openForWriting(paths, writers);
        write(table, writers);
        close(writers);
        return paths;
    }

    private void openForWriting(final Path[] paths, final BufferedWriter[] writers) throws AlgorithmExecutionException {
        try {
            for (int index = 0; index < paths.length; ++index) {
                final Path path = getPath();
                paths[index] = path;
                writers[index] = Files.newBufferedWriter(path);
            }
        } catch (final IOException e) {
            throw new AlgorithmExecutionException("cannot open file for writing", e);
        }
    }

    private void write(final RelationalFileInput file, final BufferedWriter[] writers)
            throws AlgorithmExecutionException, IOException {

        while (file.hasNext()) {
            tableLength++;
            final List<String> next = file.next();
            for (int index = 0; index < writers.length; index++) {
                final String value = index >= next.size() ? null : next.get(index);
                if (value != null) {
                    writers[index].write(escape(value));
                    writers[index].newLine();
                }
            }
        }
    }

    private void close(final Closeable[] toClose) throws AlgorithmExecutionException {
        try {
            for (final Closeable item : toClose) {
                item.close();
            }
        } catch (final IOException e) {
            throw new AlgorithmExecutionException("cannot close", e);
        }
    }

    private ReadPointer[] open(final Path[] paths) throws AlgorithmExecutionException {

        try {
            final ReadPointer[] result = new ReadPointer[paths.length];
            for (int index = 0; index < paths.length; index++) {
                result[index] = ReadPointer.of(paths[index]);
            }
            return result;
        } catch (final IOException e) {
            throw new AlgorithmExecutionException("cannot open file for reading", e);
        }
    }

    private String escape(final String value) {
        return value.replace('\n', '\0');
    }

    private Path getPath() throws IOException {
        this.count++;
        File tempFile = new File(".\\temp\\temp_" + count + ".txt");
        return tempFile.toPath();
    }
}
