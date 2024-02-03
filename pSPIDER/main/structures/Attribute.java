package structures;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.util.Set;

import lombok.Data;

@Data

public class Attribute {


    private final int id;
    private final String tableName;
    private final String columnName;
    private final IntSet referenced;
    private final IntSet dependent;
    private final ReadPointer readPointer;
    private String currentValue;
    private Long currentOccurrences;

    /**
     * An Attribute resembles a column. It manages its own dependent and referenced attributes.
     */
    public Attribute(int id, String tableName, String columnName, ReadPointer readPointer) {

        this.id = id;
        this.readPointer = readPointer;
        this.tableName = tableName;
        this.columnName = columnName;
        this.dependent = new IntLinkedOpenHashSet();
        this.referenced = new IntLinkedOpenHashSet();
        this.currentValue = readPointer.getCurrentValue();
        this.currentOccurrences = Long.parseLong(readPointer.next());
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
        this.referenced.addAll(referenced);
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
        final IntIterator referencedIterator = referenced.iterator();
        while (referencedIterator.hasNext()) {
            final int ref = referencedIterator.nextInt();
            if (attributes.contains(ref)) {
                continue;
            }

            referencedIterator.remove();
            attributeIndex[ref].removeDependent(id);
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
