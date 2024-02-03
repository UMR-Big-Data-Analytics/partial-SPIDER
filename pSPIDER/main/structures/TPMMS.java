package structures;

import de.metanome.util.TPMMSConfiguration;

import java.beans.ConstructorProperties;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class TPMMS {
    private final TPMMSConfiguration configuration;

    public TPMMS(TPMMSConfiguration configuration) {
        this.configuration = configuration;
    }

    public void uniqueAndSort(Path path) throws IOException {
        this.uniqueAndSort(path, new TPMMS.DefaultOutput());
    }

    public void uniqueAndSort(Path path, TPMMS.Output output) throws IOException {
        (new TPMMS.Execution(this.configuration, path, output)).uniqueAndSort();
    }

    public interface Output extends Closeable {
        void open(Path var1) throws IOException;

        void write(String var1) throws IOException;
    }

    private static class DefaultOutput implements TPMMS.Output {
        private BufferedWriter writer;

        private DefaultOutput() {
        }

        public void open(Path to) throws IOException {
            this.writer = Files.newBufferedWriter(to, StandardOpenOption.TRUNCATE_EXISTING);
        }

        public void write(String value) throws IOException {
            this.writer.write(value);
            this.writer.newLine();
        }

        public void close() throws IOException {
            this.writer.flush();
            this.writer.close();
        }
    }

    private static class Merger {
        private final TPMMS.Output output;
        private PriorityQueue<TPMMSTuple> topFileValues;
        private BufferedReader[] readers;

        @ConstructorProperties({"output"})
        public Merger(TPMMS.Output output) {
            this.output = output;
        }

        private void init(List<Path> files) throws IOException {
            this.topFileValues = new PriorityQueue<>(files.size());
            this.readers = new BufferedReader[files.size()];

            for (int index = 0; index < files.size(); ++index) {
                BufferedReader reader = Files.newBufferedReader(files.get(index));
                this.readers[index] = reader;
                String firstLine = reader.readLine();
                if (firstLine != null) {
                    long occurrence = Long.parseLong(reader.readLine());
                    this.topFileValues.add(new TPMMSTuple(firstLine, occurrence, index));
                }
            }

        }

        private void merge(List<Path> files, Path to) throws IOException {
            this.init(files);
            this.output.open(to);

            String previousValue = null;
            long occurrence = 0L;

            while (!this.topFileValues.isEmpty()) {
                TPMMSTuple current = this.topFileValues.poll();
                if (previousValue != null && !previousValue.equals(current.getValue())) {
                    this.output.write(previousValue);
                    this.output.write(String.valueOf(occurrence));
                    occurrence = 0L;
                }
                occurrence += current.getOccurrence();
                previousValue = current.getValue();

                String nextValue = this.readers[current.getReaderNumber()].readLine();
                if (nextValue != null) {
                    long nextOccurrence = Long.parseLong(this.readers[current.getReaderNumber()].readLine());
                    current.setValue(nextValue);
                    current.setOccurrence(nextOccurrence);
                    this.topFileValues.add(current);
                }
            }
            this.output.close();
            this.closeReaders();

        }

        private void closeReaders() throws IOException {
            BufferedReader[] readers = this.readers;

            for (BufferedReader reader : readers) {
                if (reader != null) {
                    reader.close();
                }
            }

        }
    }

    private static class Execution {
        private final TPMMSConfiguration configuration;
        private final TPMMS.Output output;
        private final Path origin;
        private final long maxMemoryUsage;
        private final Map<String, Long> values;
        private final List<Path> spilledFiles;
        private int totalValues;
        private int valuesSinceLastMemoryCheck;

        private Execution(TPMMSConfiguration configuration, Path origin, TPMMS.Output output) {
            this.values = new TreeMap<>();
            this.spilledFiles = new ArrayList<>();
            this.totalValues = 0;
            this.valuesSinceLastMemoryCheck = 0;
            this.configuration = configuration;
            this.output = output;
            this.origin = origin;
            this.maxMemoryUsage = getMaxMemoryUsage(configuration);
        }

        private static long getMaxMemoryUsage(TPMMSConfiguration configuration) {
            long available = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
            return (long) ((double) available * ((double) configuration.getMaxMemoryUsagePercentage() / 100.0D));
        }

        private void uniqueAndSort() throws IOException {
            this.writeSpillFiles();
            if (this.spilledFiles.isEmpty()) {
                this.writeOutput();
            } else {
                if (!this.values.isEmpty()) {
                    this.writeSpillFile();
                }

                (new TPMMS.Merger(this.output)).merge(this.spilledFiles, this.origin);
            }

            this.removeSpillFiles();
        }

        private void writeSpillFiles() throws IOException {
            BufferedReader reader = Files.newBufferedReader(this.origin);

            String line;
            while ((line = reader.readLine()) != null && !this.isInputLimitExceeded()) {
                if (!values.containsKey(line)) {
                    values.put(line, 0L);
                }
                this.values.put(line, values.get(line) + 1);
                this.maybeWriteSpillFile();
            }

            reader.close();

        }

        private boolean isInputLimitExceeded() {
            ++this.totalValues;
            return this.configuration.getInputRowLimit() > 0 && this.totalValues > this.configuration.getInputRowLimit();
        }

        private void maybeWriteSpillFile() throws IOException {
            ++this.valuesSinceLastMemoryCheck;
            if (this.valuesSinceLastMemoryCheck > this.configuration.getMemoryCheckInterval() && this.shouldWriteSpillFile()) {
                this.valuesSinceLastMemoryCheck = 0;
                this.writeSpillFile();
            }

        }

        private boolean shouldWriteSpillFile() {
            return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > this.maxMemoryUsage;
        }

        private void writeSpillFile() throws IOException {
            Path target = Paths.get(this.origin + "#" + this.spilledFiles.size());
            this.write(target, this.values);
            this.spilledFiles.add(target);
            this.values.clear();
            System.gc();
        }

        private void write(Path path, Map<String, Long> values) throws IOException {
            BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            Iterator<Long> valueIterator = values.values().iterator();
            for (String key : values.keySet()) {
                writer.write(key);
                writer.newLine();
                writer.write(valueIterator.next().toString());
                writer.newLine();
            }
            writer.flush();
            writer.close();
        }

        private void writeOutput() throws IOException {
            this.output.open(this.origin);

            Iterator<Long> valueIterator = this.values.values().iterator();

            for (String key : values.keySet()) {
                this.output.write(key);
                this.output.write(valueIterator.next().toString());
            }
            this.output.close();
        }

        private void removeSpillFiles() {
            spilledFiles.forEach(x -> {
                try {
                    Files.delete(x);
                } catch (IOException e) {
                    System.out.println("Unable to delete file: " + x);
                    e.printStackTrace();
                }
            });

            this.spilledFiles.clear();
        }
    }
}
