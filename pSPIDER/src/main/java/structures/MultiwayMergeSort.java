package structures;

import io.Merger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class MultiwayMergeSort {

    private final Path origin;
    private final Map<String, Long> values;
    private final List<Path> spilledFiles;
    private final Logger logger;
    private final Attribute attribute;
    private final int maxMapSize;
    private int valuesSinceLastSpill;

    public MultiwayMergeSort(Config config, Attribute attribute, int stringLimit) {
        this.values = new HashMap<>((int) (stringLimit*1.05));
        this.attribute = attribute;
        this.spilledFiles = new ArrayList<>();
        this.valuesSinceLastSpill = 0;
        this.origin = attribute.getPath();
        this.logger = LoggerFactory.getLogger(MultiwayMergeSort.class);
        this.maxMapSize = config.maxMemory;
    }

    public void sort() throws IOException {
        logger.debug("Starting sort for: " + attribute.getId());
        long sTime = System.currentTimeMillis();

        // one file is created when merging
        attribute.spilledFiles = 1;

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

        attribute.spilledFiles += spilledFiles.size();
        this.removeSpillFiles();

        logger.debug("Finished sort for: " + attribute.getId() + ". Took: " + (System.currentTimeMillis() - sTime));
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
        ++this.valuesSinceLastSpill;
        if (this.valuesSinceLastSpill > this.maxMapSize) {
            this.valuesSinceLastSpill = 0;
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
