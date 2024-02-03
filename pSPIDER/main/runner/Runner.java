package runner;

import core.Spider;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.util.TPMMSConfiguration;

import java.util.Collections;

public class Runner {
    public static void main(String[] args) throws AlgorithmExecutionException {
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
    }
}
