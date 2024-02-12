package runner;

import core.Spider;

import java.io.IOException;

// TODO: Null handling options
// TODO: include some prints for progress tracking

public class Runner {
    public static void main(String[] args) throws IOException, InterruptedException {

        Config.Dataset[] datasets = new Config.Dataset[]{Config.Dataset.TPCH_1};
        int[] threads = new int[]{12};
        int[] maxMemory = new int[]{1_000_000, 400_000, 200_000, 100_000, 50_000, 10_000};

        for (Config.Dataset dataset : datasets) {
            for (int thread : threads) {
                for (int memory : maxMemory) {
                    for (int i = 1; i <= 5; i++) {
                        Config config = new Config(dataset, 1);
                        config.maxMemoryPercent = memory;
                        config.numThreads = thread;
                        config.executionName = dataset + "_" + memory + "_" + thread + "_" + i;

                        System.out.print("(" + config.executionName + ")");

                        Spider spider = new Spider(config);

                        long startTime = System.currentTimeMillis();
                        spider.execute();
                        System.out.println(" took: " + (System.currentTimeMillis() - startTime) + "ms");
                    }
                }
            }
        }


    }
}
