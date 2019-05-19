package big.data.analysis.payload;

import lombok.Data;

/**
 * @author Peter Lipcak, Masaryk University
 */
@Data
public class DeleteResponse {
    private String fileName;
    private String message;

    public DeleteResponse(String fileName) {
        this.fileName = fileName;
        this.message = fileName + " successfully deleted";
    }
}