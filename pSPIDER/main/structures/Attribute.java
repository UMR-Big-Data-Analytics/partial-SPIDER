package structures;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.util.Set;

import lombok.Data;

@Data
public
class Attribute {

    private final int id;
    private final String tableName;
    private final String columnName;
    private final IntSet referenced;
    private final IntSet dependent;
    private final ReadPointer readPointer;
    private String currentValue;
    private Long currentOccurrences;

    public Attribute(final int id, final String tableName, final String columnName,
                     final ReadPointer readPointer) {

        this.id = id;
        this.readPointer = readPointer;
        this.tableName = tableName;
        this.columnName = columnName;
        dependent = new IntLinkedOpenHashSet();
        referenced = new IntLinkedOpenHashSet();
        this.currentValue = readPointer.getCurrentValue();
        this.currentOccurrences = Long.parseLong(readPointer.next());
    }

    public void addDependent(final IntSet dependent) {
        this.dependent.addAll(dependent);
    }

    public void removeDependent(final int dependent) {
        this.dependent.remove(dependent);
    }

    public void addReferenced(final IntSet referenced) {
        this.referenced.addAll(referenced);
    }

    public void removeReferenced(final int referenced) {
        this.referenced.remove(referenced);
    }

    public String getCurrentValue() {
        return currentValue;
    }

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

    public void intersectReferenced(Set<Integer> attributes, final Attribute[] attributeIndex) {
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

    public boolean isFinished() {
        return referenced.isEmpty() && dependent.isEmpty();
    }

    public void close() throws IOException {
        readPointer.close();
    }
}
