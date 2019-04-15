package big.data.analysis.payload;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CompilationResponse {

    private String compilationOutput;
    private boolean success;

}
