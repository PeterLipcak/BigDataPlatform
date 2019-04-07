package big.data.analysis.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingestion")
@Data
public class IngestionProperties {
    private String jarFile;
}