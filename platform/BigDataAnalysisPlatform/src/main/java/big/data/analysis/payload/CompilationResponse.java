package big.data.analysis.payload;

import lombok.Builder;
import lombok.Data;

/**
 * @author Peter Lipcak, Masaryk University
 */
@Data
@Builder
public class CompilationResponse {

    private String compilationOutput;
    private boolean success;

}
