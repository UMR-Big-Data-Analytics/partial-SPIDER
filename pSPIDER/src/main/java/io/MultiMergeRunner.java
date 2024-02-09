package io;

import runner.Config;
import structures.Attribute;
import structures.MultiwayMergeSort;

import java.io.IOException;
import java.util.Queue;

public class MultiMergeRunner extends Thread {

    private final Config config;
    private final Queue<Attribute> attributeQueue;

    public MultiMergeRunner(Queue<Attribute> attributeQueue, Config config) {
        this.attributeQueue = attributeQueue;
        this.config = config;
    }
    public void run() {
        MultiwayMergeSort multiwayMergeSort = new MultiwayMergeSort(config);
        while (!attributeQueue.isEmpty()) {
            Attribute attribute = attributeQueue.poll();
            if (attribute == null) continue;
            try {
                multiwayMergeSort.uniqueAndSort(attribute);
            } catch (IOException e) {
                e.printStackTrace();
            }
            attribute.calculateViolations(config);
            try {
                attribute.open();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
