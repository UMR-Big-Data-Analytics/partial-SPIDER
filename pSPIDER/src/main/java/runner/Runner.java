package runner;

import core.Spider;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import io.RelationalFileInput;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO: Get rid of three config Classes -> everything into one
// TODO: Null handling options
// TODO: include some prints for progress tracking

public class Runner {
    public static void main(String[] args) throws AlgorithmExecutionException, IOException {

        Config config = new Config(Config.Algorithm.SPIDER, Config.Dataset.DATA_GOV, 1);

        List<RelationalFileInput> fileInputGenerators = new ArrayList<>(config.tableNames.length);
        for (int i = 0; i < config.tableNames.length; i++) {
            String relationName = config.databaseName + "." + config.tableNames[i];
            String relationPath = config.inputFolderPath + config.databaseName + File.separator +
                    config.tableNames[i] + config.inputFileEnding;
            fileInputGenerators.add(new RelationalFileInput(relationName, relationPath, config));
        }

        Spider spider = new Spider(config);
        long startTime = System.currentTimeMillis();
        spider.execute(fileInputGenerators, config.threshold);
        System.out.println("Execution Took: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
