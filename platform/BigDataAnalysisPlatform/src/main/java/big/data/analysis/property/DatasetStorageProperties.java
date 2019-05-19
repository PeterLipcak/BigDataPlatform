package big.data.analysis.property;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dataset")
@Data
public class DatasetStorageProperties {
    private String uploadLocalDir;
    private String uploadHdfsDir;
}
