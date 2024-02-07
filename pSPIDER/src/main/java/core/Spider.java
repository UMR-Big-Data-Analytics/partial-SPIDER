package core;

import io.ReadPointer;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Spider {

    private final Config config;
    private final ExternalRepository externalRepository;
    private final double threshold;
    private Attribute[] attributeIndex;
    private PriorityQueue<Attribute> priorityQueue;
    private final Logger logger;


    public Spider(Config config) {
        this.config = config;
        this.threshold = config.threshold;
        this.logger = LoggerFactory.getLogger(Spider.class);
        externalRepository = new ExternalRepository();
    }


    public void execute() throws IOException {
        logger.info("Starting Execution");
        initializeAttributes(this.config.getFileInputs());
        calculateInclusionDependencies();
        int unaryINDs = collectResults();
        System.out.println("Unary INDs: " + unaryINDs);
        shutdown();
    }

    private void initializeAttributes(final List<RelationalFileInput> tables) throws IOException {
        logger.info("Initializing Attributes");
        long sTime = System.currentTimeMillis();

        final int columnCount = getTotalColumnCount(tables);
        attributeIndex = new Attribute[columnCount];
        priorityQueue = new ObjectHeapPriorityQueue<>(columnCount, this::compareAttributes);
        createAndEnqueueAttributes(tables);
        initializePINDs();

        logger.info("Finished Initializing Attributes. Took: " + (System.currentTimeMillis() - sTime) + "ms");
        for (RelationalFileInput table : tables) {
            table.close();
        }
    }

    private void createAndEnqueueAttributes(final List<RelationalFileInput> tables) throws IOException {

        int attributeId = 0;
        for (RelationalFileInput table : tables) {
            long sTime = System.currentTimeMillis();
            logger.info("Starting table " + table.relationName());

            final Attribute[] attributes = getAttributes(table, attributeId);
            attributeId += attributes.length;

            for (final Attribute attribute : attributes) {
                attributeIndex[attribute.getId()] = attribute;
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
            logger.info("Finished table " + table.relationName() + ". Took: " + (System.currentTimeMillis() - sTime) + "ms");

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

    private Attribute[] getAttributes(final RelationalFileInput table, int startIndex) throws IOException {

        final ReadPointer[] readPointers = externalRepository.uniqueAndSort(config, table);
        final Attribute[] attributes = new Attribute[table.numberOfColumns()];
        long allowedViolations = (long) ((1.0 - threshold) * externalRepository.tableLength);
        for (int index = 0; index < readPointers.length; ++index) {
            attributes[index] = new Attribute(
                    startIndex++,
                    table.relationName(),
                    table.headerLine.get(index),
                    allowedViolations,
                    externalRepository.nullCounts[index],
                    readPointers[index]
            );
        }
        return attributes;
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
            while (!priorityQueue.isEmpty() && sameValue(priorityQueue.first(), firstAttribute)) {
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

    private boolean sameValue(final Attribute a1, final Attribute a2) {
        return Objects.equals(a1.getCurrentValue(), a2.getCurrentValue());
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
