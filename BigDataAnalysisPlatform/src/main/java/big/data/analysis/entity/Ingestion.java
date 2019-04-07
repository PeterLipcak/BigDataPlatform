package big.data.analysis.entity;

import lombok.Builder;
import lombok.Data;
import org.joda.time.DateTime;

import java.util.List;

@Data
@Builder
public class Ingestion {
    private String datasetName;
    private DateTime startTime;
    private transient Process process;
    private String status;
    private long recordsProcessed;
}