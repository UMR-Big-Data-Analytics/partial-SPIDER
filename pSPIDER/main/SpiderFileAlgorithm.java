import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import java.util.ArrayList;
import java.util.List;

public class SpiderFileAlgorithm extends SpiderAlgorithm implements
    RelationalInputParameterAlgorithm {

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.addAll(file());
    requirements.addAll(common());
    return requirements;
  }

  private List<ConfigurationRequirement<?>> file() {
    return List.of(new ConfigurationRequirementRelationalInput(ConfigurationKey.TABLE.name(),
            ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));
  }

  @Override
  public void setRelationalInputConfigurationValue(String identifier,
      RelationalInputGenerator... values)
      throws AlgorithmConfigurationException {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      builder.relationalInputGenerators(asList(values));
    } else {
      handleUnknownConfiguration(identifier, values);
    }
  }

}
