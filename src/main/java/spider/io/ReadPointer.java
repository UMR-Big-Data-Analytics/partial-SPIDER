package spider.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

public class ReadPointer implements Iterator<String> {

    private final BufferedReader reader;
    private String currentValue;
    public final Path path;

    public ReadPointer(Path path) throws IOException {
        this.path = path;
        this.reader = new BufferedReader(new FileReader(String.valueOf(path)));
        currentValue = reader.readLine();
    }

    public String getCurrentValue() {
        return currentValue;
    }

    public boolean hasNext() {
        return currentValue != null;
    }

    public String next() {
        if (currentValue == null) {
            return null;
        }
        try {
            currentValue = reader.readLine();
            return currentValue;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        reader.close();
    }

}
