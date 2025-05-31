package spider.io;

import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;

import java.util.List;

public class RelationalInputWrapper {
    public int tableOffset;
    public String relationName;
    public String[] headerLine;
    RelationalInput input;

    public RelationalInputWrapper(int tableOffset, String relationName, RelationalInput input) {
        this.tableOffset = tableOffset;
        this.relationName = relationName;
        this.headerLine = input.columnNames().toArray(new String[0]);
        this.input = input;
    }

    public int numberOfColumns() {
        return headerLine.length;
    }

    public boolean hasNext() throws InputIterationException {
        return input.hasNext();
    }

    public List<String> next() throws InputIterationException {
        return input.next();
    }

    public void close() throws Exception {
        input.close();
    }
}
