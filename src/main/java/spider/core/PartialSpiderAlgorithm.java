package spider.core;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.RelaxedInclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.RelaxedInclusionDependency;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import spider.io.RelationalInputWrapper;
import spider.io.RepositoryRunner;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import spider.structures.Attribute;
import spider.structures.MultiwayMergeSort;
import spider.utils.DuplicateHandling;
import spider.utils.NullHandling;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

public class PartialSpiderAlgorithm {

    public double threshold = 1.0;
    public DefaultFileInputGenerator[] fileInputGenerator = null;
    public String databaseName;
    public String[] tableNames;
    public String executionName = "pSPIDER";
    public String resultFolder = "F:\\statistics";
    public String tempFolder = "F:\\temp";

    public int numThreads = Runtime.getRuntime().availableProcessors();

    public DuplicateHandling duplicateHandling = DuplicateHandling.AWARE;
    public NullHandling nullHandling = NullHandling.SUBSET;

    public int maxMemory = 3_000_000;

    private Attribute[] attributeIndex;
    private PriorityQueue<Attribute> priorityQueue;

    RelaxedInclusionDependencyResultReceiver resultReceiver;

    public void execute() throws IOException, AlgorithmExecutionException {
        //logger.info("Starting Execution");
        List<RelationalInputWrapper> tables = this.getFileInputs();

        initializeAttributes(tables);

        long initializingTime = System.currentTimeMillis();
        try {
            createAttributes(tables);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        initializingTime = System.currentTimeMillis() - initializingTime;

        long enqueueTime = System.currentTimeMillis();
        enqueueAttributes();
        enqueueTime = System.currentTimeMillis() - enqueueTime;

        long pINDInitialization = System.currentTimeMillis();
        initializePINDs();
        pINDInitialization = System.currentTimeMillis() - pINDInitialization;

        long pINDCalculation = System.currentTimeMillis();
        calculateInclusionDependencies();
        pINDCalculation = System.currentTimeMillis() - pINDCalculation;

        output();
        //collectResults(initializingTime, enqueueTime, pINDInitialization, pINDCalculation);
        shutdown();
    }

    private List<RelationalInputWrapper> getFileInputs() throws InputGenerationException {
        List<RelationalInputWrapper> inputs = new ArrayList<>();
        int totalColumns = 0;
        for (int i = 0; i < fileInputGenerator.length; i++) {
            inputs.add(new RelationalInputWrapper(totalColumns, tableNames[i], fileInputGenerator[i].generateNewCopy()));
            totalColumns += inputs.get(i).numberOfColumns();
        }
        return inputs;
    }

    /**
     * Fetches the number of attributes and prepares the index as well as the priority queue
     *
     * @param tables Input Files
     */
    private void initializeAttributes(final List<RelationalInputWrapper> tables) {
        //logger.info("Initializing Attributes");
        long sTime = System.currentTimeMillis();

        int numAttributes = getTotalColumnCount(tables);
        //logger.info("Found " + numAttributes + " attributes");
        attributeIndex = new Attribute[numAttributes];
        priorityQueue = new PriorityQueue<>(numAttributes, this::compareAttributes);

        //logger.info("Finished Initializing Attributes. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    /**
     * Iterates over all tables and creates a file for each attribute. Closes the input readers.
     *
     * @param tables Input Files
     */
    private void createAttributes(List<RelationalInputWrapper> tables) throws InterruptedException {
        //logger.info("Creating attribute files");
        long sTime = System.currentTimeMillis();

        Queue<RelationalInputWrapper> inputQueue = new ArrayDeque<>(tables);
        RepositoryRunner[] repositoryRunners = new RepositoryRunner[numThreads];
        for (int i = 0; i < numThreads; i++) {
            repositoryRunners[i] = new RepositoryRunner(inputQueue, attributeIndex, tempFolder);
            repositoryRunners[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            repositoryRunners[i].join();
        }

        //logger.info("Finished creating attribute Files. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    private void enqueueAttributes() throws IOException {

        Queue<Attribute> attributeQueue = Arrays.stream(attributeIndex).sorted(Attribute::compareBySize).collect(Collectors.toCollection(ArrayDeque::new));
        System.gc();
        MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long available = memoryUsage.getMax() - memoryUsage.getUsed();
        // we estimate 400 Bytes per String including overhead
        long threadStringLimit = available / (this.numThreads*400L);
        this.maxMemory = (int) threadStringLimit;

        attributeQueue.parallelStream().forEach(attribute -> {
            int maxSize = (int) Math.min(attribute.getSize(), this.maxMemory);
            MultiwayMergeSort multiwayMergeSort = new MultiwayMergeSort(maxMemory, attribute, maxSize);
            try {
                multiwayMergeSort.sort();
            } catch (IOException e) {
                e.printStackTrace();
            }
            attribute.calculateViolations(threshold, duplicateHandling);
        });

        for (final Attribute attribute : attributeIndex) {
            attribute.open();
            if (attribute.getReadPointer().hasNext()) {
                priorityQueue.add(attribute);
            } else {
                // The attribute is null in every entry
                // Equality will never get here, since null is considered a value
                if (this.nullHandling == NullHandling.INEQUALITY) {
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
        if (this.nullHandling == NullHandling.FOREIGN) {
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

    private int getTotalColumnCount(final List<RelationalInputWrapper> tables) {
        return tables.stream().mapToInt(RelationalInputWrapper::numberOfColumns).sum();
    }

    private int compareAttributes(final Attribute a1, final Attribute a2) {
        if (a1.getCurrentValue() == null && a2.getCurrentValue() == null) {
            return 0;
        }

        if (a1.getCurrentValue() == null) {
            return 1;
        }

        if (a2.getCurrentValue() == null) {
            return -1;
        }

        return a1.getCurrentValue().compareTo(a2.getCurrentValue());
    }

    private void calculateInclusionDependencies() {
        long sTime = System.currentTimeMillis();
        //logger.info("Start pIND calculation");

        Map<Integer, Long> topAttributes = new HashMap<>();
        while (!priorityQueue.isEmpty()) {

            final Attribute firstAttribute = priorityQueue.poll();
            topAttributes.put(firstAttribute.getId(), firstAttribute.getCurrentOccurrences());
            while (!priorityQueue.isEmpty() && priorityQueue.peek().equals(firstAttribute)) {
                Attribute sameGroupAttribute = priorityQueue.poll();
                topAttributes.put(sameGroupAttribute.getId(), sameGroupAttribute.getCurrentOccurrences());
            }

            for (int topAttribute : topAttributes.keySet()) {
                attributeIndex[topAttribute].intersectReferenced(topAttributes.keySet(), attributeIndex, duplicateHandling);
            }

            if (topAttributes.size() == 1 && !priorityQueue.isEmpty()) {
                String nextVal = priorityQueue.peek().getCurrentValue();
                while (firstAttribute.nextValue() && !firstAttribute.isFinished()) {
                    if (firstAttribute.getCurrentValue().compareTo(nextVal) >= 0) {
                        priorityQueue.add(firstAttribute);
                        break;
                    }
                }
            } else {

                for (int topAttribute : topAttributes.keySet()) {
                    final Attribute attribute = attributeIndex[topAttribute];
                    if (attribute.nextValue() && !attribute.isFinished()) {
                        priorityQueue.add(attribute);
                    }
                }
            }

            topAttributes.clear();
        }
        //logger.info("Finished pIND calculation. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    private void collectResults(long init, long enqueue, long pINDCreation, long pINDValidation) throws IOException {
        int numUnary = 0;


        BufferedWriter bw = new BufferedWriter(new FileWriter(resultFolder + File.separator + executionName + "_pINDs.txt"));

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


        bw = Files.newBufferedWriter(Path.of(resultFolder + File.separator + executionName + "_" + databaseName + "_" + (System.currentTimeMillis()/1000) + ".json"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

        // TODO: log num spills
        //bw = new BufferedWriter(new FileWriter(".\\results\\" + config.executionName + "_" + (System.currentTimeMillis()/1000) + ".json"));
        // build a json file
        bw.write('{');
        bw.write("\"database\": \"" + databaseName + "\",");
        long runtime = init + enqueue + pINDCreation + pINDValidation;
        bw.write("\"runtime\": \"" + runtime + "\",");
        bw.write("\"threads\": " + numThreads + ",");
        bw.write("\"pINDs\": " + numUnary + ",");
        bw.write("\"threshold\": " + threshold + ",");
        bw.write("\"nullHandling\": \"" + nullHandling + "\",");
        bw.write("\"duplicateHandling\": \"" + duplicateHandling + "\",");
        bw.write("\"initialization\": " + init + ",");
        bw.write("\"enqueue\": " + enqueue + ",");
        bw.write("\"pINDCreation\": " + pINDCreation + ",");
        bw.write("\"pINDValidation\": " + pINDValidation + ",");
        // the total of spilled files + the copied attribute file
        bw.write("\"spilledFiles\": " + (Arrays.stream(attributeIndex).mapToInt(Attribute::getSpilledFiles).sum() + attributeIndex.length));
        bw.write('}');
        bw.flush();
        bw.close();
    }

    private void output() {
        for (final Attribute dep : attributeIndex) {
            if (dep.getReferenced().isEmpty()) {
                continue;
            }
            for (final int refId : dep.getReferenced().keySet()) {
                final Attribute ref = attributeIndex[refId];

                if (resultReceiver != null) {
                    double measure = dep.getReferenced().get(refId);
                    try {
                        this.resultReceiver.receiveResult(
                                new RelaxedInclusionDependency(
                                        new ColumnPermutation(
                                                new ColumnIdentifier(dep.getTableName(), dep.getColumnName())),
                                        new ColumnPermutation(
                                                new ColumnIdentifier(ref.getTableName(), ref.getColumnName())), measure));
                    } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        }
    }



    private void shutdown() throws IOException {
        for (final Attribute attribute : attributeIndex) {
            attribute.close();
        }

    }
}
