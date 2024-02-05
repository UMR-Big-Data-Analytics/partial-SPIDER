package core;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import io.ReadPointer;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import runner.SpiderConfiguration;
import structures.Attribute;
import structures.ExternalRepository;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Spider {


    private final TableInfoFactory tableInfoFactory;
    private final ExternalRepository externalRepository;

    private SpiderConfiguration configuration;
    private Attribute[] attributeIndex;
    private PriorityQueue<Attribute> priorityQueue;
    private double threshold;


    public Spider() {
        tableInfoFactory = new TableInfoFactory();
        externalRepository = new ExternalRepository();
    }


    public void execute(final SpiderConfiguration configuration, double threshold) throws AlgorithmExecutionException, IOException {
        this.configuration = configuration;
        this.threshold = threshold;
        final List<TableInfo> table = tableInfoFactory.create(configuration.getRelationalInputGenerators(), configuration.getTableInputGenerators());
        initializeAttributes(table);
        calculateInclusionDependencies();
        int uINDS = collectResults();
        System.out.println("Unary INDs: " + uINDS);
        shutdown();
    }

    private void initializeAttributes(final List<TableInfo> tables) throws AlgorithmExecutionException {

        final int columnCount = getTotalColumnCount(tables);
        attributeIndex = new Attribute[columnCount];
        priorityQueue = new ObjectHeapPriorityQueue<>(columnCount, this::compareAttributes);
        createAndEnqueueAttributes(tables);
        initializeRoles();
    }

    private void createAndEnqueueAttributes(final List<TableInfo> tables) throws AlgorithmExecutionException {

        int attributeId = 0;
        List<Attribute> nullAttributes = new ArrayList<>();
        for (final TableInfo table : tables) {
            final Attribute[] attributes = getAttributes(table, attributeId);
            attributeId += attributes.length;

            for (final Attribute attribute : attributes) {
                attributeIndex[attribute.getId()] = attribute;
                if (attribute.getReadPointer().hasNext()) {
                    // Has next value: always process.
                    priorityQueue.enqueue(attribute);
                } else {
                    // this attribute carries only null values
                    nullAttributes.add(attribute);
                }
            }
        }

        // this is the NULL is subset interpretation
        for (Attribute nullAttribute : nullAttributes) {
            nullAttribute.getDependent().clear();
            for (Attribute attribute : attributeIndex) {
                attribute.removeReferenced(nullAttribute.getId());
            }
        }
    }

    private Attribute[] getAttributes(final TableInfo table, int startIndex) throws AlgorithmExecutionException {

        final ReadPointer[] readPointers = externalRepository.uniqueAndSort(configuration, table);
        final Attribute[] attributes = new Attribute[table.getColumnCount()];
        long allowedViolations = (long) ((1.0 - threshold) * externalRepository.tableLength);
        for (int index = 0; index < readPointers.length; ++index) {
            attributes[index] = new Attribute(startIndex++, table.getTableName(), table.getColumnNames().get(index), allowedViolations, readPointers[index]);
        }
        return attributes;
    }

    private void initializeRoles() {
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

    private int getTotalColumnCount(final List<TableInfo> tables) {
        return tables.stream().mapToInt(TableInfo::getColumnCount).sum();
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

    private void shutdown() throws AlgorithmExecutionException {
        try {
            for (final Attribute attribute : attributeIndex) {
                attribute.close();
            }
        } catch (final IOException e) {
            throw new AlgorithmExecutionException("failed to close attribute", e);
        }
    }
}
