package main;

import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.backend.algorithm_execution.TempFileGenerator;
import de.metanome.util.TPMMSConfiguration;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SpiderConfiguration {

  private boolean processEmptyColumns;
  private FileGenerator tempFileGenerator;
  private InclusionDependencyResultReceiver resultReceiver;

  private TPMMSConfiguration tpmmsConfiguration;
  @Singular
  private List<TableInputGenerator> tableInputGenerators;
  @Singular
  private List<RelationalInputGenerator> relationalInputGenerators;

  public static SpiderConfiguration withDefaults() {
    SpiderConfiguration builder = null;
    try {
      builder = builder()
          .processEmptyColumns(true)
          .tempFileGenerator(new TempFileGenerator())
          .resultReceiver(null)
          .tpmmsConfiguration(TPMMSConfiguration.withDefaults())
          .tableInputGenerators(Collections.emptyList())
          .relationalInputGenerators(Collections.emptyList())
          .build();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return builder;
  }
}
