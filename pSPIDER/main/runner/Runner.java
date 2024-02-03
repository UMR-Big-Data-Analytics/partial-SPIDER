package runner;

import core.Spider;
import core.SpiderFileAlgorithm;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.util.TPMMSConfiguration;
import io.DefaultFileInputGenerator;

import java.io.File;
import java.util.Collections;

public class Runner {
    public static void main(String[] args) throws AlgorithmExecutionException {

        Config config = new Config(Config.Algorithm.SPIDER, Config.Dataset.UEFA);

        FileInputGenerator[] fileInputGenerators = new FileInputGenerator[config.tableNames.length];
        for (int i = 0; i < config.tableNames.length; i++) {
            fileInputGenerators[i] = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(config.inputFolderPath + config.databaseName + File.separator + config.tableNames[i] + config.inputFileEnding, true, config.inputFileSeparator, config.inputFileQuoteChar, config.inputFileEscape, config.inputFileStrictQuotes, config.inputFileIgnoreLeadingWhiteSpace, config.inputFileSkipLines, config.inputFileHasHeader, config.inputFileSkipDifferingLines, config.inputFileNullString));
        }

        SpiderFileAlgorithm spiderFileAlgorithm = new SpiderFileAlgorithm();
        spiderFileAlgorithm.setRelationalInputConfigurationValue(ConfigurationKey.TABLE.name(), fileInputGenerators);

        spiderFileAlgorithm.execute();
        /*
        Spider spider = new Spider();
        SpiderConfiguration conf = new SpiderConfiguration(
                true,
                true,
                true,
                "",
                ',',
                null,
                null,
                TPMMSConfiguration.withDefaults(),
                Collections.emptyList(),
                Collections.emptyList()
        );
        spider.execute(conf);

         */
    }
}
