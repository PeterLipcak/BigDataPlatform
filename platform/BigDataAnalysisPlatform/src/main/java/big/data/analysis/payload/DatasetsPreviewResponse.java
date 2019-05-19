package big.data.analysis.payload;

import big.data.analysis.entity.Dataset;
import lombok.Data;

import java.util.List;

/**
 * @author Peter Lipcak, Masaryk University
 */
@Data
public class DatasetsPreviewResponse {
    private List<Dataset> datasets;

    public DatasetsPreviewResponse(List<Dataset> datasets){
        this.datasets = datasets;
    }
}
