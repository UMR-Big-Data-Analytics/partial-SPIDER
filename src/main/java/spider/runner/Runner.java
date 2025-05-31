package spider.runner;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import spider.core.PartialSpider;
import spider.utils.FileUtils;

import java.io.File;
import java.io.IOException;

public class Runner {

    static String DATA_FOLDER = "F:\\metaserve\\io\\data\\";
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            Config config = new Config(1.0d);
            config.inputFileHasHeader = true;
            config.fileEnding = ".csv";
            config.separator = ',';
            config.folderPath = DATA_FOLDER;
            config.setDataset(DATA_FOLDER + "Cars");
            long time = System.currentTimeMillis();
            executePSPIDER(config);
            System.out.println("Runtime: " + (System.currentTimeMillis() - time) + " ms");
        } else {
            // Parse arguments
            String datasetName = args[0];
            char separator = args[1].charAt(0);
            String fileEnding = String.valueOf(args[2]);
            boolean inputFileHasHeader = Boolean.parseBoolean(args[3]);
            double threshold = Double.parseDouble(args[4]);
            int NUMBER_OF_FILES = Integer.parseInt(args[11]);

            // Create and configure the Config object
            Config config = new Config(threshold);
            config.folderPath = DATA_FOLDER;
            config.inputFileHasHeader = inputFileHasHeader;
            config.fileEnding = fileEnding;
            config.separator = separator;
            if(datasetName.equals("Musicbrainz")) {
                config.fileEnding = "";
                config.inputFileNullString = "\\N";
                config.quoteChar = '\0';
            }
            config.setDataset(DATA_FOLDER + datasetName);

            long time = System.currentTimeMillis();
            executePSPIDER(config);
            System.out.println("Runtime: " + (System.currentTimeMillis() - time) + " ms");
        }

    }

    public static void executePSPIDER(Config conf) {
        try {

            DefaultFileInputGenerator[] fileInputGenerators = new DefaultFileInputGenerator[conf.tableNames.length];
            for (int i = 0; i < conf.tableNames.length; i++) {
                File file = new File(conf.folderPath + conf.databaseName + File.separator + conf.tableNames[i] + conf.fileEnding);
                fileInputGenerators[i] = new DefaultFileInputGenerator(file, conf.toConfigurationSettingFileInput(file.getName()));
            }
            PartialSpider binderFile = new PartialSpider();
            binderFile.setRelationalInputConfigurationValue(PartialSpider.Identifier.INPUT_FILES.name(), fileInputGenerators);
            binderFile.setStringConfigurationValue(PartialSpider.Identifier.THRESHOLD.name(), "1.0");

            long time = System.currentTimeMillis();
            binderFile.execute();
            time = System.currentTimeMillis() - time;

            if (conf.writeResults) {
                FileUtils.writeToFile(binderFile + "\r\n\r\n" + "Runtime: " + time + "\r\n\r\n" + conf, conf.resultFolder + File.separator + conf.getResultName());
            }
        } catch (IOException | AlgorithmExecutionException e) {
            e.printStackTrace();
        }
    }

}
