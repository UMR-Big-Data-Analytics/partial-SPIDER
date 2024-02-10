package io;

import structures.Attribute;
import structures.Entry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.PriorityQueue;

public class Merger {
    private PriorityQueue<Entry> headValues;
    private BufferedReader[] readers;

    private void init(List<Path> files) throws IOException {
        this.headValues = new PriorityQueue<>(files.size());
        this.readers = new BufferedReader[files.size()];

        for (int index = 0; index < files.size(); ++index) {
            BufferedReader reader = Files.newBufferedReader(files.get(index));
            this.readers[index] = reader;
            String firstLine = reader.readLine();
            if (firstLine != null) {
                long occurrence = Long.parseLong(reader.readLine());
                this.headValues.add(new Entry(firstLine, occurrence, index));
            }
        }

    }

    public void merge(List<Path> files, Path to, Attribute attribute) throws IOException {
        this.init(files);
        BufferedWriter output = Files.newBufferedWriter(to);

        String previousValue = null;
        long occurrence = 0L;

        while (!this.headValues.isEmpty()) {
            Entry current = this.headValues.poll();
            if (previousValue != null && !previousValue.equals(current.getValue())) {
                writeValue(attribute, output, previousValue, occurrence);
                occurrence = 0L;
            }
            occurrence += current.getOccurrence();
            previousValue = current.getValue();

            String nextValue = this.readers[current.getReaderNumber()].readLine();
            if (nextValue != null) {
                updateHeadValues(current, nextValue);
            }
        }
        output.flush();
        output.close();
        closeReaders();

    }

    private void updateHeadValues(Entry current, String nextValue) throws IOException {
        long nextOccurrence = Long.parseLong(this.readers[current.getReaderNumber()].readLine());
        current.setValue(nextValue);
        current.setOccurrence(nextOccurrence);
        this.headValues.add(current);
    }

    private void writeValue(Attribute attribute, BufferedWriter output, String previousValue, long occurrence) throws IOException {
        output.write(previousValue);
        output.newLine();
        output.write(String.valueOf(occurrence));
        output.newLine();
        attribute.setUniqueSize(attribute.getUniqueSize() + 1L);
    }

    private void closeReaders() throws IOException {
        BufferedReader[] readers = this.readers;

        for (BufferedReader reader : readers) {
            if (reader != null) {
                reader.close();
            }
        }

    }
}
