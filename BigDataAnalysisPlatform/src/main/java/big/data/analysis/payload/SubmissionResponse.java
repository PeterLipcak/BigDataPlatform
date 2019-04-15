package big.data.analysis.payload;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SubmissionResponse {

    private CompilationResponse compilationResult;
    private String submissionOutput;
    private boolean compilationSuccess;
    private boolean submissionSuccess;

}