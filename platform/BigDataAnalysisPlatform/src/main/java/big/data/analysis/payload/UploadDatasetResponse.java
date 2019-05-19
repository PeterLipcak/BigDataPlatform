package big.data.analysis.payload;

import lombok.Data;

@Data
public class UploadDatasetResponse {
    private String fileName;
    private String fileType;
    private long size;

    public UploadDatasetResponse(String fileName, String fileType, long size) {
        this.fileName = fileName;
        this.fileType = fileType;
        this.size = size;
    }
}
