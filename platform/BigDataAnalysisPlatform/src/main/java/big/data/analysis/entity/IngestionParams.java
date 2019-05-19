package big.data.analysis.entity;

import lombok.Data;
import lombok.ToString;

/**
 * @author Peter Lipcak, Masaryk University
 */
@Data
@ToString
public class IngestionParams {

    private Integer densificationType;
    private String datasetName;
    private String topic;
    private Integer recordsPerSecond;
    private Integer densificationCount;
    private String[] interpolators;
    private String interpolationId;

}
