package runner;

import core.Spider;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.util.TPMMSConfiguration;
import io.DefaultFileInputGenerator;

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

        List<FileInputGenerator> fileInputGenerators = new ArrayList<>(config.tableNames.length);
        for (int i = 0; i < config.tableNames.length; i++) {
            fileInputGenerators.add(
                    new DefaultFileInputGenerator(
                            new ConfigurationSettingFileInput(
                                    config.inputFolderPath +
                                            config.databaseName +
                                            File.separator +
                                            config.tableNames[i] +
                                            config.inputFileEnding,
                                    true,
                                    config.inputFileSeparator,
                                    config.inputFileQuoteChar,
                                    config.inputFileEscape,
                                    config.inputFileStrictQuotes,
                                    config.inputFileIgnoreLeadingWhiteSpace,
                                    config.inputFileSkipLines,
                                    config.inputFileHasHeader,
                                    config.inputFileSkipDifferingLines,
                                    config.inputFileNullString
                            )
                    )
            );
        }

        SpiderConfiguration.SpiderConfigurationBuilder builder = SpiderConfiguration.builder();
        builder.relationalInputGenerators(fileInputGenerators);

        TPMMSConfiguration multiwayMergeConfig = new TPMMSConfiguration(-1,50,1000);
        SpiderConfiguration spiderConfiguration = builder.tpmmsConfiguration(multiwayMergeConfig).build();

        Spider spider = new Spider();
        long startTime = System.currentTimeMillis();
        spider.execute(spiderConfiguration, config.threshold);
        System.out.println("Execution Took: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
