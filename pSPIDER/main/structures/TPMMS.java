package structures;

import de.metanome.util.TPMMSConfiguration;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
            this.writer.close();
        }
    }

    private static class Merger {
        private final TPMMS.Output output;
        private ObjectHeapPriorityQueue<TPMMSTuple> values;
        private BufferedReader[] readers;

        private void init(List<Path> files) throws IOException {
            this.values = new ObjectHeapPriorityQueue(files.size());
            this.readers = new BufferedReader[files.size()];

            for(int index = 0; index < files.size(); ++index) {
                BufferedReader reader = Files.newBufferedReader((Path)files.get(index));
                this.readers[index] = reader;
                String firstLine = reader.readLine();
                if (firstLine != null) {
                    this.values.enqueue(new TPMMSTuple(firstLine, index));
                }
            }

        }

        private void merge(List<Path> files, Path to) throws IOException {
            this.init(files);
            this.output.open(to);

            try {
                String previousValue = null;

                while(!this.values.isEmpty()) {
                    TPMMSTuple current = (TPMMSTuple)this.values.dequeue();
                    if (previousValue == null || !previousValue.equals(current.getValue())) {
                        this.output.write(current.getValue());
                    }

                    previousValue = current.getValue();
                    String nextValue = this.readers[current.getReaderNumber()].readLine();
                    if (nextValue != null) {
                        current.setValue(nextValue);
                        this.values.enqueue(current);
                    }
                }
            } finally {
                this.output.close();
                this.closeReaders();
            }

        }

        private void closeReaders() throws IOException {
            BufferedReader[] var1 = this.readers;
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
                BufferedReader reader = var1[var3];
                if (reader != null) {
                    reader.close();
                }
            }

        }

        @ConstructorProperties({"output"})
        public Merger(TPMMS.Output output) {
            this.output = output;
        }
    }

    private static class Execution {
        private final TPMMSConfiguration configuration;
        private final TPMMS.Output output;
        private final Path origin;
        private final long maxMemoryUsage;
        private final SortedSet<String> values;
        private final List<Path> spilledFiles;
        private int totalValues;
        private int valuesSinceLastMemoryCheck;

        private Execution(TPMMSConfiguration configuration, Path origin, TPMMS.Output output) {
            this.values = new TreeSet();
            this.spilledFiles = new ArrayList();
            this.totalValues = 0;
            this.valuesSinceLastMemoryCheck = 0;
            this.configuration = configuration;
            this.output = output;
            this.origin = origin;
            this.maxMemoryUsage = getMaxMemoryUsage(configuration);
        }

        private static long getMaxMemoryUsage(TPMMSConfiguration configuration) {
            long available = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
            return (long)((double)available * ((double)configuration.getMaxMemoryUsagePercentage() / 100.0D));
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
            Throwable var2 = null;

            try {
                String line;
                try {
                    while((line = reader.readLine()) != null && !this.isInputLimitExceeded()) {
                        this.values.add(line);
                        this.maybeWriteSpillFile();
                    }
                } catch (Throwable var11) {
                    var2 = var11;
                    throw var11;
                }
            } finally {
                if (reader != null) {
                    if (var2 != null) {
                        try {
                            reader.close();
                        } catch (Throwable var10) {
                            var2.addSuppressed(var10);
                        }
                    } else {
                        reader.close();
                    }
                }

            }

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

        private void write(Path path, Set<String> values) throws IOException {
            BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Throwable var4 = null;

            try {
                Iterator var5 = values.iterator();

                while(var5.hasNext()) {
                    String value = (String)var5.next();
                    writer.write(value);
                    writer.newLine();
                }

                writer.flush();
            } catch (Throwable var14) {
                var4 = var14;
                throw var14;
            } finally {
                if (writer != null) {
                    if (var4 != null) {
                        try {
                            writer.close();
                        } catch (Throwable var13) {
                            var4.addSuppressed(var13);
                        }
                    } else {
                        writer.close();
                    }
                }

            }
        }

        private void writeOutput() throws IOException {
            this.output.open(this.origin);

            try {
                Iterator var1 = this.values.iterator();

                while(var1.hasNext()) {
                    String value = (String)var1.next();
                    this.output.write(value);
                }
            } finally {
                this.output.close();
            }

        }

        private void removeSpillFiles() throws IOException {
            Iterator var1 = this.spilledFiles.iterator();

            while(var1.hasNext()) {
                Path spill = (Path)var1.next();
                Files.delete(spill);
            }

            this.spilledFiles.clear();
        }
    }

    public interface Output extends Closeable {
        void open(Path var1) throws IOException;

        void write(String var1) throws IOException;
    }
}
