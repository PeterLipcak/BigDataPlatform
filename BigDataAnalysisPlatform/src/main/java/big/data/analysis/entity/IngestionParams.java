package big.data.analysis.entity;

import lombok.Data;

@Data
public class IngestionParams {

    private Integer densificationType;
    private String datasetName;
    private String topic;
    private Integer recordsPerSecond;
    private Integer multiplicationCount;

}
