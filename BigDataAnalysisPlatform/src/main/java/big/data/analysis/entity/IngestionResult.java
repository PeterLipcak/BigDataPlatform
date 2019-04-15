package big.data.analysis.entity;

import lombok.Data;
import org.joda.time.Duration;

@Data
public class IngestionResult {

    private long recordsSent = 0;
    private String status;
    private Duration duration;

}
