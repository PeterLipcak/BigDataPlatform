package big.data.analysis.payload;

import lombok.Data;

/**
 * @author Peter Lipcak, Masaryk University
 */
@Data
public class MessageResponse {
    private String message;

    public MessageResponse(String message) {
        this.message = message;
    }
}