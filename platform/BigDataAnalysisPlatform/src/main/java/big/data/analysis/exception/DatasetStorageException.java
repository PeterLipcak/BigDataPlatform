package big.data.analysis.exception;

/**
 * @author Peter Lipcak, Masaryk University
 */
public class DatasetStorageException extends RuntimeException {
    public DatasetStorageException(String message) {
        super(message);
    }
    public DatasetStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
