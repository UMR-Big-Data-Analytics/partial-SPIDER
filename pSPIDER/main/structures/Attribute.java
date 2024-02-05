package structures;

import io.ReadPointer;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import lombok.Data;

@Data

public class Attribute {


    private final int id;
    private final String tableName;
    private final String columnName;
    private final Map<Integer, Long> referenced;
    private final IntSet dependent;
    private final ReadPointer readPointer;
    private long violationsLeft;
    private String currentValue;
    private Long currentOccurrences;

    /**
     * An Attribute resembles a column. It manages its own dependent and referenced attributes.
     */
    public Attribute(int id, String tableName, String columnName, long violationsLeft, ReadPointer readPointer) {

        this.id = id;
        this.readPointer = readPointer;
        this.tableName = tableName;
        this.columnName = columnName;
        this.violationsLeft = violationsLeft;
        this.dependent = new IntLinkedOpenHashSet();
        this.referenced = new HashMap<>();
        this.currentValue = readPointer.getCurrentValue();
        if (readPointer.hasNext()) {
            this.currentOccurrences = Long.parseLong(readPointer.next());
        }
    }

    /**
     * Adds all dependent ids from the given Set to the internal dependent set.
     * @param dependent ids of attributes that should be added
     */
    public void addDependent(final IntSet dependent) {
        this.dependent.addAll(dependent);
    }

    /**
     * Removes all dependent ids from the given Set from the internal dependent set.
     * @param dependent ids of attributes that should be removed
     */
    public void removeDependent(final int dependent) {
        this.dependent.remove(dependent);
    }

    /**
     * Adds all referenced ids from the given Set to the internal referenced set.
     * @param referenced ids of attributes that should be added
     */
    public void addReferenced(final IntSet referenced) {
        referenced.forEach(x -> this.referenced.put(x, violationsLeft));
    }

    /**
     * Removes all referenced ids from the given Set from the internal referenced set.
     * @param referenced ids of attributes that should be removed
     */
    public void removeReferenced(final int referenced) {
        this.referenced.remove(referenced);
    }

    /**
     * Updates the internal variables currentValue and currentOccurrences
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
     * @param attributes Set of attribute ids, which share some value
     * @param attributeIndex The index that stores all attributes
     */
    public void intersectReferenced(Set<Integer> attributes, final Attribute[] attributeIndex) {
        // TODO: adjust to respect pINDs
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
     * @throws IOException if the reader fails to close
     */
    public void close() throws IOException {
        readPointer.close();
    }
}
