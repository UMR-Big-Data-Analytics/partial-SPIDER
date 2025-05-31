package spider.io;

import spider.structures.Attribute;
import spider.structures.MultiwayMergeSort;
import spider.utils.DuplicateHandling;

import java.io.IOException;
import java.util.Queue;

/**
 * Runner that executes a Multiway MergeSort over a queue of attributes
 */
public class MultiMergeRunner extends Thread {

    private final Queue<Attribute> attributeQueue;
    private int maxMemory;
    private double threshold;
    private DuplicateHandling duplicateHandling;

    /**
     * @param attributeQueue A Queue of attributes which still need to be processed
     * @param config         The configuration that should be used for the MultiwayMergeSort execution
     */
    public MultiMergeRunner(Queue<Attribute> attributeQueue, int maxMemory, double threshold, DuplicateHandling duplicateHandling) {
        this.attributeQueue = attributeQueue;
        this.maxMemory = maxMemory;
        this.threshold = threshold;
        this.duplicateHandling = duplicateHandling;
    }

    /**
     * While there are attributes left to be processed, the Thread polls the next in queue and processes it.
     */
    public void run() {
        while (!attributeQueue.isEmpty()) {
            Attribute attribute = attributeQueue.poll();
            if (attribute == null) continue;
            int maxSize = (int) Math.min(attribute.getSize(), maxMemory);
            MultiwayMergeSort multiwayMergeSort = new MultiwayMergeSort(maxMemory, attribute, maxSize);
            try {
                multiwayMergeSort.sort();
            } catch (IOException e) {
                e.printStackTrace();
            }
            attribute.calculateViolations(threshold, duplicateHandling);
        }
    }
}
