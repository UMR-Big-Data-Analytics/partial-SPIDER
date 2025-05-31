package spider.structures;

import spider.io.ReadPointer;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import spider.utils.DuplicateHandling;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * An Attribute resembles a column. It manages its own dependent and referenced attributes.
 */
public class Attribute {


    private final int id;
    private final String tableName;
    private final String columnName;
    public int spilledFiles;

    private Map<Integer, Long> referenced;
    private IntSet dependent;

    private long size;
    private long uniqueSize;
    private long nullCount = 0L;
    private long violationsLeft;

    private ReadPointer readPointer;
    private Path path;

    private String currentValue;
    private Long currentOccurrences;

    public Attribute(int id, Path attributePath, String tableName, String columnName) {
        this.id = id;
        this.path = attributePath;
        this.tableName = tableName;
        this.columnName = columnName;
        this.dependent = new IntLinkedOpenHashSet(); //TODO: get rid of this
        this.referenced = new HashMap<>();
    }

    public void calculateViolations(double threshold, DuplicateHandling duplicateHandling) {
        if (duplicateHandling == DuplicateHandling.AWARE) {
            this.violationsLeft = (long) ((1.0 - threshold) * size);
        } else {
            this.violationsLeft = (long) ((1.0 - threshold) * uniqueSize);
        }
    }

    public boolean equals(Attribute other) {
        return currentValue.equals(other.currentValue);
    }

    public int compareBySize(Attribute other) {
        if (this.size > other.getSize()) {
            return -1;
        } else if (this.size == other.size) {
            return 0;
        } else {
            return 1;
        }
    }

    public long getSize() {
        return size;
    }

    /**
     * Adds all dependent ids from the given Set to the internal dependent set.
     *
     * @param dependent ids of attributes that should be added
     */
    public void addDependent(final IntSet dependent) {
        this.dependent.addAll(dependent);
    }

    /**
     * Removes all dependent ids from the given Set from the internal dependent set.
     *
     * @param dependent ids of attributes that should be removed
     */
    public void removeDependent(final int dependent) {
        this.dependent.remove(dependent);
    }

    /**
     * Adds all referenced ids from the given Set to the internal referenced set.
     *
     * @param referenced ids of attributes that should be added
     */
    public void addReferenced(final IntSet referenced) {
        referenced.forEach(x -> this.referenced.put(x, violationsLeft));
    }

    /**
     * Removes all referenced ids from the given Set from the internal referenced set.
     *
     * @param referenced ids of attributes that should be removed
     */
    public void removeReferenced(final int referenced) {
        this.referenced.remove(referenced);
    }

    /**
     * Updates the internal variables currentValue and currentOccurrences
     *
     * @return True if there was a next value to load, false otherwise
     */
    public boolean nextValue() {
        if (readPointer.hasNext()) {
            this.currentValue = readPointer.next();
            if (readPointer.hasNext()) {
                this.currentOccurrences = Long.valueOf(readPointer.next());
                return true;
            }
        }
        return false;
    }

    /**
     * Updates the dependant and referenced sets using the attributes that share a value.
     *
     * @param attributes     Set of attribute ids, which share some value
     * @param attributeIndex The index that stores all attributes
     */
    public void intersectReferenced(Set<Integer> attributes, final Attribute[] attributeIndex, DuplicateHandling duplicateHandling) {
        Iterator<Integer> referencedIterator = referenced.keySet().iterator();
        while (referencedIterator.hasNext()) {
            final int ref = referencedIterator.next();
            if (attributes.contains(ref)) {
                continue;
            }

            long updated_violations;
            // v won't be null since we iterate the key set
            if (duplicateHandling == DuplicateHandling.UNAWARE) {
                updated_violations = referenced.compute(ref, (k, v) -> v - currentOccurrences);
            } else {
                updated_violations = referenced.compute(ref, (k, v) -> v - 1);
            }

            if (updated_violations < 0L) {
                referencedIterator.remove();
                attributeIndex[ref].removeDependent(id);
            }
        }
    }

    /**
     * An Attribute is considered as finished, if it is not referenced by any other and is not dependent on any
     * other attribute. Since it is irrelevant for pIND discovery now, it can be removed from the attribute queue.
     */
    public boolean isFinished() {
        return referenced.isEmpty() && dependent.isEmpty();
    }

    /**
     * Closes the reader connected to the attribute.
     *
     * @throws IOException if the reader fails to close
     */
    public void close() throws IOException {
        readPointer.close();
        Files.delete(readPointer.path);
    }

    public void open() throws IOException {
        this.readPointer = new ReadPointer(path);
        this.currentValue = readPointer.getCurrentValue();
        if (readPointer.hasNext()) this.currentOccurrences = Long.parseLong(readPointer.next());
    }

    public void incNullCount() {
        this.nullCount++;
    }

    public Path getPath() {
        return path;
    }

    public int getId() {
        return id;
    }

    public void setUniqueSize(long size) {
        this.uniqueSize = size;
    }

    public int getSpilledFiles() {
        return spilledFiles;
    }

    public Map<Integer, Long> getReferenced() {
        return referenced;
    }

    public IntSet getDependent() {
        return dependent;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public long getUniqueSize() {
        return uniqueSize;
    }

    public long getNullCount() {
        return nullCount;
    }

    public long getViolationsLeft() {
        return violationsLeft;
    }

    public ReadPointer getReadPointer() {
        return readPointer;
    }

    public String getCurrentValue() {
        return currentValue;
    }

    public Long getCurrentOccurrences() {
        return currentOccurrences;
    }

    public void setSize(long tableSize) {
        this.size = tableSize;
    }
}
