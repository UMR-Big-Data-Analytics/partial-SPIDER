package runner;

import core.Spider;
import de.metanome.algorithm_integration.AlgorithmExecutionException;

import java.io.IOException;

// TODO: Get rid of three config Classes -> everything into one
// TODO: Null handling options
// TODO: include some prints for progress tracking

public class Runner {
    public static void main(String[] args) throws AlgorithmExecutionException, IOException {

        Config config = new Config(Config.Algorithm.SPIDER, Config.Dataset.DATA_GOV, 1);

        Spider spider = new Spider(config);
        long startTime = System.currentTimeMillis();
        spider.execute();
        System.out.println("Execution Took: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
