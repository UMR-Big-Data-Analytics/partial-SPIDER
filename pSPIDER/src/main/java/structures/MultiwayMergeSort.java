package structures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

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

public record MultiwayMergeSort(runner.Config configuration) {

    public void uniqueAndSort(Path path) throws IOException {
        this.uniqueAndSort(path, new MultiwayMergeSort.DefaultOutput());
    }

    public void uniqueAndSort(Path path, MultiwayMergeSort.Output output) throws IOException {
        (new MultiwayMergeSort.Execution(configuration, path, output)).uniqueAndSort();
    }

    public interface Output extends Closeable {
        void open(Path var1) throws IOException;

        void write(String var1) throws IOException;
    }

    private static class DefaultOutput implements MultiwayMergeSort.Output {
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
        private final MultiwayMergeSort.Output output;
        private PriorityQueue<Entry> topFileValues;
        private BufferedReader[] readers;

        @ConstructorProperties({"output"})
        public Merger(MultiwayMergeSort.Output output) {
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
                    this.topFileValues.add(new Entry(firstLine, occurrence, index));
                }
            }

        }

        private void merge(List<Path> files, Path to) throws IOException {
            this.init(files);
            this.output.open(to);

            String previousValue = null;
            long occurrence = 0L;

            while (!this.topFileValues.isEmpty()) {
                Entry current = this.topFileValues.poll();
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
        private final MultiwayMergeSort.Output output;
        private final Path origin;
        private final long maxMemoryUsage;
        private final Map<String, Long> values;
        private final List<Path> spilledFiles;
        private final int memoryCheckFrequency;
        private int valuesSinceLastMemoryCheck;
        private final Logger logger;

        private Execution(Config configuration, Path origin, Output output) {
            this.values = new TreeMap<>();
            this.spilledFiles = new ArrayList<>();
            this.valuesSinceLastMemoryCheck = 0;
            this.output = output;
            this.origin = origin;
            this.memoryCheckFrequency = configuration.memoryCheckFrequency;
            this.maxMemoryUsage = getMaxMemoryUsage(configuration.maxMemoryPercent);
            this.logger = LoggerFactory.getLogger(Execution.class);

            logger.debug("Max Memory Usage set to: " + maxMemoryUsage + "bytes");
        }

        private static long getMaxMemoryUsage(int maxMemoryPercent) {
            long available = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
            return (long) ((double) available * ((double) maxMemoryPercent / 100.0D));
        }

        private void uniqueAndSort() throws IOException {
            this.writeSpillFiles();
            if (this.spilledFiles.isEmpty()) {
                this.writeOutput();
            } else {
                if (!this.values.isEmpty()) {
                    this.writeSpillFile();
                }

                (new MultiwayMergeSort.Merger(this.output)).merge(this.spilledFiles, this.origin);
            }

            this.removeSpillFiles();
        }

        private void writeSpillFiles() throws IOException {
            BufferedReader reader = Files.newBufferedReader(this.origin);

            String line;
            while ((line = reader.readLine()) != null) {
                if (!values.containsKey(line)) {
                    values.put(line, 0L);
                }
                this.values.put(line, values.get(line) + 1);
                this.maybeWriteSpillFile();
            }

            reader.close();

        }


        private void maybeWriteSpillFile() throws IOException {
            ++this.valuesSinceLastMemoryCheck;
            if (this.valuesSinceLastMemoryCheck > this.memoryCheckFrequency && this.shouldWriteSpillFile()) {
                this.valuesSinceLastMemoryCheck = 0;
                this.writeSpillFile();
            }

        }

        private boolean shouldWriteSpillFile() {
            return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > this.maxMemoryUsage;
        }

        private void writeSpillFile() throws IOException {
            logger.info("Spilling Attribute " + this.origin + " #" + this.spilledFiles.size());
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
