package structures;

import io.Merger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

public class MultiwayMergeSort {

    private final Path origin;
    private final long maxMemoryUsage;
    private final Map<String, Long> values;
    private final List<Path> spilledFiles;
    private final int memoryCheckFrequency;
    private final Logger logger;
    private final Attribute attribute;
    private final int stringLimit;
    private int valuesSinceLastMemoryCheck;

    public MultiwayMergeSort(Config config, Attribute attribute, int stringLimit) {
        this.values = new HashMap<>((int) (stringLimit*1.05));
        this.attribute = attribute;
        this.spilledFiles = new ArrayList<>();
        this.valuesSinceLastMemoryCheck = 0;
        this.origin = attribute.getPath();
        this.memoryCheckFrequency = config.memoryCheckFrequency;
        this.maxMemoryUsage = getMaxMemoryUsage(config.maxMemoryPercent);
        this.logger = LoggerFactory.getLogger(MultiwayMergeSort.class);
        this.stringLimit = stringLimit;
    }

    private static long getMaxMemoryUsage(int maxMemoryPercent) {
        long available = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        return (long) ((double) available * ((double) maxMemoryPercent / 100.0D));
    }

    public void sort() throws IOException {
        logger.info("Starting sort for: " + attribute.getId());
        long sTime = System.currentTimeMillis();

        this.writeSpillFiles();
        if (this.spilledFiles.isEmpty()) {
            attribute.setUniqueSize(this.values.size());
            this.write(origin);
        } else {
            if (!this.values.isEmpty()) {
                this.writeSpillFile();
            }
            Merger spilledMerger = new Merger();
            spilledMerger.merge(this.spilledFiles, this.origin, attribute);
        }

        attribute.spilledFiles = spilledFiles.size();
        this.removeSpillFiles();

        logger.info("Finished sort for: " + attribute.getId() + ". Took: " + (System.currentTimeMillis() - sTime));
    }

    private void writeSpillFiles() throws IOException {
        BufferedReader reader = Files.newBufferedReader(this.origin);

        String line;
        while ((line = reader.readLine()) != null) {
            if (1L == this.values.compute(line, (k, v) -> v == null ? 1L : v+1L)) {
                this.maybeWriteSpillFile();
            }
        }

        reader.close();
    }


    private void maybeWriteSpillFile() throws IOException {
        ++this.valuesSinceLastMemoryCheck;
        if (this.valuesSinceLastMemoryCheck > this.stringLimit) {
            this.valuesSinceLastMemoryCheck = 0;
            this.writeSpillFile();
        }

    }

    private void writeSpillFile() throws IOException {
        logger.info("Spilling Attribute " + this.origin + "#" + this.spilledFiles.size());
        Path target = Paths.get(this.origin + "#" + this.spilledFiles.size());
        this.write(target);
        this.spilledFiles.add(target);
        this.values.clear();
    }

    private void write(Path path) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        for (String key : values.keySet().stream().sorted().toList()) {
            writer.write(key);
            writer.newLine();
            writer.write(values.get(key).toString());
            writer.newLine();
        }
        writer.flush();
        writer.close();
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
