package big.data.analysis.entity;

import lombok.Builder;
import lombok.Data;
import org.joda.time.DateTime;

import java.util.List;

/**
 * @author Peter Lipcak, Masaryk University
 */
@Data
@Builder
public class Ingestion {
    private String id;
    private String datasetName;
    private int ingestionType;
    private long startTime;
    private transient Process process;
    private String status;
    private long recordsProcessed;
}