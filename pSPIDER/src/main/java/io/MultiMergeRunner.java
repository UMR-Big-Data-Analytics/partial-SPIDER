package io;

import runner.Config;
import structures.Attribute;
import structures.MultiwayMergeSort;

import java.io.IOException;
import java.util.Queue;

/**
 * Runner that executes a Multiway MergeSort over a queue of attributes
 */
public class MultiMergeRunner extends Thread {

    private final Config config;
    private final Queue<Attribute> attributeQueue;

    /**
     * @param attributeQueue A Queue of attributes which still need to be processed
     * @param config         The configuration that should be used for the MultiwayMergeSort execution
     */
    public MultiMergeRunner(Queue<Attribute> attributeQueue, Config config) {
        this.attributeQueue = attributeQueue;
        this.config = config;
    }

    /**
     * While there are attributes left to be processed, the Thread polls the next in queue and processes it.
     */
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
