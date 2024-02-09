package core;

import io.MultiMergeRunner;
import io.RelationalFileInput;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.ExternalRepository;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class Spider {

    private final Config config;
    private final ExternalRepository externalRepository;
    private final Logger logger;
    private Attribute[] attributeIndex;
    private PriorityQueue<Attribute> priorityQueue;


    public Spider(Config config) {
        this.config = config;
        this.logger = LoggerFactory.getLogger(Spider.class);
        externalRepository = new ExternalRepository();
    }


    public void execute() throws IOException, InterruptedException {
        logger.info("Starting Execution");
        List<RelationalFileInput> tables = this.config.getFileInputs();
        initializeAttributes(tables);

        createAttributes(tables);

        enqueueAttributes();

        initializePINDs();
        calculateInclusionDependencies();
        int unaryINDs = collectResults();
        System.out.println("Unary INDs: " + unaryINDs);
        shutdown();
    }

    /**
     * Fetches the number of attributes and prepares the index as well as the priority queue
     *
     * @param tables Input Files
     */
    private void initializeAttributes(final List<RelationalFileInput> tables) {
        logger.info("Initializing Attributes");
        long sTime = System.currentTimeMillis();

        int numAttributes = getTotalColumnCount(tables);
        logger.info("Found " + numAttributes + " attributes");
        attributeIndex = new Attribute[numAttributes];
        priorityQueue = new ObjectHeapPriorityQueue<>(numAttributes, this::compareAttributes);

        logger.info("Finished Initializing Attributes. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    /**
     * Iterates over all tables and creates a file for each attribute. Closes the input readers.
     *
     * @param tables Input Files
     * @throws IOException If a file could not be found
     */
    private void createAttributes(List<RelationalFileInput> tables) throws IOException {
        logger.info("Creating attribute files");
        long sTime = System.currentTimeMillis();

        int id = 0;

        for (RelationalFileInput table : tables) {
            int tableCount = 0;
            for (Path attributePath : externalRepository.store(table)) {
                attributeIndex[id] = new Attribute(
                        id,
                        attributePath,
                        table.relationName(),
                        externalRepository.tableLength,
                        table.headerLine.get(tableCount),
                        externalRepository.nullCounts[tableCount]);
                id++;
                tableCount++;
            }
        }


        for (RelationalFileInput table : tables) {
            table.close();
        }
        logger.info("Finished creating attribute Files. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    private void enqueueAttributes() throws InterruptedException {

        Queue<Attribute> attributeQueue = new ArrayDeque<>(Arrays.asList(attributeIndex));
        MultiMergeRunner[] multiMergeRunners = new MultiMergeRunner[config.numThreads];
        for (int i = 0; i < config.numThreads; i++) {
            multiMergeRunners[i] = new MultiMergeRunner(attributeQueue, config);
            multiMergeRunners[i].start();
        }
        for (int i = 0; i < config.numThreads; i++) {
            multiMergeRunners[i].join();
        }

        for (final Attribute attribute : attributeIndex) {
            if (attribute.getReadPointer().hasNext()) {
                priorityQueue.enqueue(attribute);
            } else {
                // The attribute is null in every entry
                // Equality will never get here, since null is considered a value
                if (config.nullHandling == Config.NullHandling.INEQUALITY) {
                    // Inequality: Every Null is different form every other null
                    // An attribute that only consists of null can not form any pIND regardless of the threshold.
                    attribute.getDependent().clear();
                    attribute.getReferenced().clear();
                } else {
                    // Subset: Attribute references everything
                    // Another Pure-Null attribute could still be a reference

                    // Foreign: Like Subset but referenced can not include null -> handled below
                    attribute.getDependent().clear();
                }
            }
        }


        // Handle Foreign constraints
        if (config.nullHandling == Config.NullHandling.FOREIGN) {
            for (Attribute attribute : attributeIndex) {
                if (attribute.getNullCount() > 0L) {
                    for (Attribute depAttribute : attributeIndex) {
                        depAttribute.removeReferenced(attribute.getId());
                    }
                }
            }
        }
    }

    private void initializePINDs() {
        final IntSet allIds = allIds();
        for (final Attribute attribute : attributeIndex) {
            attribute.addDependent(allIds);
            attribute.removeDependent(attribute.getId());
            attribute.addReferenced(allIds);
            attribute.removeReferenced(attribute.getId());
        }
    }

    private IntSet allIds() {
        final IntSet ids = new IntOpenHashSet(attributeIndex.length);
        for (int index = 0; index < attributeIndex.length; ++index) {
            ids.add(index);
        }
        return ids;
    }

    private int getTotalColumnCount(final List<RelationalFileInput> tables) {
        return tables.stream().mapToInt(RelationalFileInput::numberOfColumns).sum();
    }

    private int compareAttributes(final Attribute a1, final Attribute a2) {
        if (a1.getCurrentValue() == null && a2.getCurrentValue() == null) {
            return Integer.compare(a1.getId(), a2.getId());
        }

        if (a1.getCurrentValue() == null) {
            return 1;
        }

        if (a2.getCurrentValue() == null) {
            return -1;
        }

        final int order = a1.getCurrentValue().compareTo(a2.getCurrentValue());
        if (order == 0) {
            return Integer.compare(a1.getId(), a2.getId());
        }
        return order;
    }

    private void calculateInclusionDependencies() {
        long sTime = System.currentTimeMillis();
        logger.info("Start pIND calculation");

        Map<Integer, Long> topAttributes = new HashMap<>();
        while (!priorityQueue.isEmpty()) {

            final Attribute firstAttribute = priorityQueue.dequeue();
            topAttributes.put(firstAttribute.getId(), firstAttribute.getCurrentOccurrences());
            while (!priorityQueue.isEmpty() && priorityQueue.first().equals(firstAttribute)) {
                Attribute sameGroupAttribute = priorityQueue.dequeue();
                topAttributes.put(sameGroupAttribute.getId(), sameGroupAttribute.getCurrentOccurrences());
            }

            for (int topAttribute : topAttributes.keySet()) {
                attributeIndex[topAttribute].intersectReferenced(topAttributes.keySet(), attributeIndex);
            }

            for (int topAttribute : topAttributes.keySet()) {
                final Attribute attribute = attributeIndex[topAttribute];
                if (attribute.nextValue() && !attribute.isFinished()) {
                    priorityQueue.enqueue(attribute);
                }
            }

            topAttributes.clear();
        }
        logger.info("Finished pIND calculation. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    private int collectResults() throws IOException {
        int numUnary = 0;
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(".\\results\\INDS.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert bw != null;
        for (final Attribute dep : attributeIndex) {

            if (dep.getReferenced().isEmpty()) {
                continue;
            }

            for (final int refId : dep.getReferenced().keySet()) {
                numUnary++;
                final Attribute ref = attributeIndex[refId];

                bw.write(dep.getTableName() + " " + dep.getColumnName());
                bw.write(" < ");
                bw.write(ref.getTableName() + " " + ref.getColumnName());
                bw.write('\n');

            }
        }
        bw.flush();
        bw.close();
        return numUnary;
    }

    private void shutdown() throws IOException {
        for (final Attribute attribute : attributeIndex) {
            attribute.close();
        }

    }
}
