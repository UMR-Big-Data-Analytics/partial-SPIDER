package runner;

import core.Spider;

import java.io.IOException;

// TODO: Null handling options
// TODO: include some prints for progress tracking

public class Runner {
    public static void main(String[] args) throws IOException, InterruptedException {

        Config config = new Config(Config.Dataset.TPCH_1, 1);

        Spider spider = new Spider(config);

        long startTime = System.currentTimeMillis();
        spider.execute();
        System.out.println("Execution Took: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
