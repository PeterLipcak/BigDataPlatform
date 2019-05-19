package big.data.analysis.payload;

import big.data.analysis.entity.Ingestion;
import lombok.Data;

import java.util.List;

/**
 * @author Peter Lipcak, Masaryk University
 */
@Data
public class IngestionsResponse {
    private List<Ingestion> ingestions;

    public IngestionsResponse(List<Ingestion> ingestions){
        this.ingestions = ingestions;
    }
}