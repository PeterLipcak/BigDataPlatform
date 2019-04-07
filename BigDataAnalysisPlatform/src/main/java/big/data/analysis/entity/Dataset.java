package big.data.analysis.entity;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class Dataset {
    private String datasetName;
    private long size;
    private List<String> preview;
}
