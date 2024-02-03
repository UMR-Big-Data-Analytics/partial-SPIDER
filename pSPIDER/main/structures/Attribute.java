package structures;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
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

    public Attribute(final int id, final String tableName, final String columnName,
                     final ReadPointer readPointer) {

        this.id = id;
        this.readPointer = readPointer;
        this.tableName = tableName;
        this.columnName = columnName;
        dependent = new IntLinkedOpenHashSet();
        referenced = new IntLinkedOpenHashSet();
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
        return readPointer.getCurrentValue();
    }

    public void nextValue() {
        if (readPointer.hasNext()) {
            readPointer.next();
        }
    }

    public void intersectReferenced(final IntSet attributes, final Attribute[] attributeIndex) {
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
        return !readPointer.hasNext() || (referenced.isEmpty() && dependent.isEmpty());
    }

    public void close() throws IOException {
        readPointer.close();
    }
}
