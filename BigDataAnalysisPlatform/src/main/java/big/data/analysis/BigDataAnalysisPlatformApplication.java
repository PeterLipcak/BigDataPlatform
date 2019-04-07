package big.data.analysis;

import big.data.analysis.property.DatasetStorageProperties;
import big.data.analysis.property.IngestionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
		DatasetStorageProperties.class,
		IngestionProperties.class
})
public class BigDataAnalysisPlatformApplication {

	public static void main(String[] args) {
		SpringApplication.run(BigDataAnalysisPlatformApplication.class, args);
	}

}
