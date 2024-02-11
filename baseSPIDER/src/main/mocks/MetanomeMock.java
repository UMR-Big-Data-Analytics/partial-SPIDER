package main.mocks;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.*;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import main.FileUtils;
import main.Spider;
import main.SpiderAlgorithm;
import main.SpiderFileAlgorithm;
import main.config.Config;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class MetanomeMock {

    public static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator[] relationalInputGenerators) throws InputGenerationException, AlgorithmConfigurationException {
        List<ColumnIdentifier> acceptedColumns = new ArrayList<>();
        for (RelationalInputGenerator relationalInputGenerator : relationalInputGenerators) {
            RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
            String tableName = relationalInput.relationName();
            for (String columnName : relationalInput.columnNames())
                acceptedColumns.add(new ColumnIdentifier(tableName, columnName));
        }
        return acceptedColumns;
    }

    public static void executeSPIDER(Config conf) {
        try {
            SpiderAlgorithm spider;

            FileInputGenerator[] fileInputGenerators = new FileInputGenerator[conf.tableNames.length];
            ResultCache resultReceiver = new ResultCache("MetanomeMock", null);
            for (int i = 0; i < conf.tableNames.length; i++) {
                System.out.println(conf.inputFolderPath + conf.databaseName + File.separator + conf.tableNames[i] + conf.inputFileEnding);
                fileInputGenerators[i] = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                        conf.inputFolderPath + conf.databaseName + File.separator + conf.tableNames[i] + conf.inputFileEnding, true,
                        conf.inputFileSeparator, conf.inputFileQuotechar, conf.inputFileEscape, conf.inputFileStrictQuotes,
                        conf.inputFileIgnoreLeadingWhiteSpace, conf.inputFileSkipLines, conf.inputFileHasHeader, conf.inputFileSkipDifferingLines, conf.inputFileNullString));
            }
            SpiderFileAlgorithm spiderFile = new SpiderFileAlgorithm();
            spiderFile.setRelationalInputConfigurationValue("TABLE", fileInputGenerators);
            spiderFile.setIntegerConfigurationValue("INPUT_ROW_LIMIT", Integer.valueOf(conf.inputRowLimit));
            spiderFile.setIntegerConfigurationValue("MAX_MEMORY_USAGE_PERCENTAGE", Integer.valueOf(conf.maxMemoryPercentage));
            spiderFile.setIntegerConfigurationValue("MEMORY_CHECK_INTERVAL", Integer.valueOf(conf.memoryCheckInterval));

            spiderFile.setResultReceiver(resultReceiver);
            spider = spiderFile;


            long time = System.currentTimeMillis();
            spider.execute();
            time = System.currentTimeMillis() - time;

            if (conf.writeResults) {
                FileUtils.writeToFile(spider.toString() + "\r\n\r\n" + "Runtime: " + time + "\r\n\r\n" + conf.toString(), conf.measurementsFolderPath + File.separator + conf.statisticsFileName);
                FileUtils.writeToFile(format(resultReceiver.fetchNewResults()), conf.measurementsFolderPath + File.separator + conf.resultFileName);
            }
        } catch (AlgorithmExecutionException | IOException e) {
            e.printStackTrace();
        }
	}

    private static String format(List<Result> results) {
        HashMap<String, List<String>> ref2Deps = new HashMap<String, List<String>>();

        for (Result result : results) {
            InclusionDependency ind = (InclusionDependency) result;

            StringBuilder refBuilder = new StringBuilder("(");
            Iterator<ColumnIdentifier> refIterator = ind.getReferenced().getColumnIdentifiers().iterator();
            while (refIterator.hasNext()) {
                refBuilder.append(refIterator.next().toString());
                if (refIterator.hasNext())
                    refBuilder.append(",");
                else
                    refBuilder.append(")");
            }
            String ref = refBuilder.toString();

            StringBuilder depBuilder = new StringBuilder("(");
            Iterator<ColumnIdentifier> depIterator = ind.getDependant().getColumnIdentifiers().iterator();
            while (depIterator.hasNext()) {
                depBuilder.append(depIterator.next().toString());
                if (depIterator.hasNext())
                    depBuilder.append(",");
                else
                    depBuilder.append(")");
            }
            String dep = depBuilder.toString();

            if (!ref2Deps.containsKey(ref))
                ref2Deps.put(ref, new ArrayList<String>());
            ref2Deps.get(ref).add(dep);
        }

        StringBuilder builder = new StringBuilder();
        ArrayList<String> referenced = new ArrayList<String>(ref2Deps.keySet());
        Collections.sort(referenced);
        for (String ref : referenced) {
            List<String> dependants = ref2Deps.get(ref);
            Collections.sort(dependants);

            if (!dependants.isEmpty())
                builder.append(ref + " > ");
            for (String dependant : dependants)
                builder.append(dependant + "  ");
            if (!dependants.isEmpty())
                builder.append("\r\n");
        }
        return builder.toString();
    }
}
