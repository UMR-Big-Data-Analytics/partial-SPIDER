package runner;

import core.Spider;

import java.io.IOException;

// TODO: Null handling options
// TODO: include some prints for progress tracking

public class Runner {
    public static void main(String[] args) throws IOException, InterruptedException {

        Config.Dataset[] datasets = new Config.Dataset[]{Config.Dataset.TPCH_1};
        double[] thresholds = new double[]{1.0, 0.99, 0.95, 0.9};
        int[] threads = new int[]{1, 2, 4, 6, 8};
        int[] maxMemory = new int[]{90, 75, 50, 25, 10, 5};

        for (Config.Dataset dataset : datasets) {
            for (int thread : threads) {
                for (int memory : maxMemory) {
                    Config config = new Config(dataset, 1);
                    config.maxMemoryPercent = memory;
                    config.numThreads = thread;
                    config.executionName = dataset + "_" + memory + "_" + thread;

                    Spider spider = new Spider(config);

                    long startTime = System.currentTimeMillis();
                    spider.execute();
                    System.out.println("(" + config.executionName + ") took: " + (System.currentTimeMillis() - startTime) + "ms");
                }
            }
        }


    }
}
