package structures;

import io.ReadPointer;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import lombok.Data;
import runner.Config;

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
@Data
public class Attribute {


    private final int id;
    private final String tableName;
    private final String columnName;

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

    public void calculateViolations(Config config) {
        if (config.duplicateHandling == Config.DuplicateHandling.AWARE) {
            this.violationsLeft = (long) ((1.0 - config.threshold) * size);
        } else {
            this.violationsLeft = (long) ((1.0 - config.threshold) * uniqueSize);
        }
    }

    public boolean equals(Attribute other) {
        return currentValue.equals(other.currentValue);
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
    public void intersectReferenced(Set<Integer> attributes, final Attribute[] attributeIndex) {
        Iterator<Integer> referencedIterator = referenced.keySet().iterator();
        while (referencedIterator.hasNext()) {
            final int ref = referencedIterator.next();
            if (attributes.contains(ref)) {
                continue;
            }

            referenced.put(ref, referenced.get(ref) - currentOccurrences);

            if (referenced.get(ref) < 0L) {
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
}
