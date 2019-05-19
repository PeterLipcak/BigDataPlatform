package big.data.analysis;

import big.data.analysis.property.DatasetStorageProperties;
import big.data.analysis.property.FlinkProperties;
import big.data.analysis.property.IngestionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;


/**
 * @author Peter Lipcak, Masaryk University
 */
@SpringBootApplication
@EnableConfigurationProperties({
		DatasetStorageProperties.class,
		IngestionProperties.class,
		FlinkProperties.class
})
public class BigDataAnalysisPlatformApplication {

	public static void main(String[] args) {
		SpringApplication.run(BigDataAnalysisPlatformApplication.class, args);
	}

}
