package spider.core;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.*;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.RelaxedInclusionDependencyResultReceiver;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import spider.utils.CollectionUtils;
import spider.utils.DuplicateHandling;
import spider.utils.FileUtils;
import spider.utils.NullHandling;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class PartialSpider extends PartialSpiderAlgorithm implements RelaxedInclusionDependencyAlgorithm, RelationalInputParameterAlgorithm, StringParameterAlgorithm {

    @Override
    public void execute() throws AlgorithmExecutionException {
        try {
            super.execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> configs = new ArrayList<ConfigurationRequirement<?>>(5);
        configs.add(new ConfigurationRequirementRelationalInput(PartialSpider.Identifier.INPUT_FILES.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));

        ConfigurationRequirementString threshold = new ConfigurationRequirementString(Identifier.THRESHOLD.name());
        String[] thresholdDefault = new String[1];
        thresholdDefault[0] = String.valueOf(this.threshold);
        threshold.setDefaultValues(thresholdDefault);
        threshold.setRequired(true);
        configs.add(threshold);

        ConfigurationRequirementString nullH = new ConfigurationRequirementString(
                Identifier.NULL_HANDLING.name());
        nullH.setDefaultValues(new String[]{"SUBSET"});
        nullH.setRequired(true);
        configs.add(nullH);

        ConfigurationRequirementString dupH = new ConfigurationRequirementString(
                Identifier.DUPLICATE_HANDLING.name());
        dupH.setDefaultValues(new String[]{"AWARE"});
        dupH.setRequired(true);
        configs.add(dupH);

        return configs;
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
        if (PartialSpider.Identifier.INPUT_FILES.name().equals(identifier)) {
            this.fileInputGenerator = (DefaultFileInputGenerator[]) values;

            this.tableNames = new String[fileInputGenerator.length];
            for (int i = 0; i < fileInputGenerator.length; i++) {
                try (RelationalInput input = fileInputGenerator[i].generateNewCopy()){
                    this.tableNames[i] = input.relationName();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(fileInputGenerator, ","));
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) throws IllegalArgumentException {
        if (Identifier.THRESHOLD.name().equals(identifier)) {
            isIllegalArgument(values);
            this.threshold = Double.parseDouble(values[0]);
        } else if (Identifier.NULL_HANDLING.name().equals(identifier)) {
            this.nullHandling = NullHandling.valueOf(values[0]);
        } else if (Identifier.DUPLICATE_HANDLING.name().equals(identifier)) {
            this.duplicateHandling = DuplicateHandling.valueOf(values[0]);

        } else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
    }

    private void isIllegalArgument(String[] values) {
        if ("".equals(values[0]) || " ".equals(values[0]) || "/".equals(values[0]) || "\\".equals(values[0]) || File.separator.equals(values[0]) || FileUtils.isRoot(new File(values[0])))
            throw new IllegalArgumentException(Identifier.TEMP_FOLDER_PATH + " must not be \"" + values[0] + "\"");
    }

    protected void handleUnknownConfiguration(String identifier, String value) throws IllegalArgumentException {
        throw new IllegalArgumentException("Unknown configuration: " + identifier + " -> " + value);
    }

    @Override
    public void setResultReceiver(RelaxedInclusionDependencyResultReceiver resultReceiver) {
        this.resultReceiver = resultReceiver;
    }


    @Override
    public String getAuthors() {
        return "Jakob L. Müller, Marcian Seeger and Thorsten Papenbrock";
    }

    @Override
    public String getDescription() {
        return "Partial Divide and Conquer-based IND discovery";
    }

    public enum Identifier {
        INPUT_FILES, INPUT_ROW_LIMIT, TEMP_FOLDER_PATH, CLEAN_TEMP, FILTER_KEY_FOREIGN_KEYS, NUM_BUCKETS_PER_COLUMN, MEMORY_CHECK_FREQUENCY, MAX_MEMORY_USAGE_PERCENTAGE, THRESHOLD, NULL_HANDLING, DUPLICATE_HANDLING
    }
}
