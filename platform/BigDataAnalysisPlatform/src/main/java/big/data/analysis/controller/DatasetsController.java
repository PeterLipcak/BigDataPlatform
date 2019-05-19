package big.data.analysis.controller;


import big.data.analysis.entity.Dataset;
import big.data.analysis.exception.DatasetStorageException;
import big.data.analysis.payload.DatasetsPreviewResponse;
import big.data.analysis.payload.DeleteResponse;
import big.data.analysis.payload.UploadDatasetResponse;
import big.data.analysis.service.DatasetsStorageService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class DatasetsController {

    private static final Logger logger = LoggerFactory.getLogger(DatasetsController.class);

    @Autowired
    private DatasetsStorageService datasetStorageService;

    @CrossOrigin
    @PostMapping("/uploadFile")
    public ResponseEntity uploadFile(@RequestParam("filepond") MultipartFile file) {
        String fileName = datasetStorageService.storeDatasetLocally(file);

        boolean uploadedToHdfs = false;
        try{
            datasetStorageService.uploadDatasetToHdfs(fileName);
            uploadedToHdfs = true;
        }catch (DatasetStorageException e){
            e.printStackTrace();
        }

        logger.info("uploadedToHdfs " + uploadedToHdfs);
        String contentType = "application/json";
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        if(fileName == null || !uploadedToHdfs){
            String response = "Unable to upload dataset";
            String json = gson.toJson(response);
            logger.info(json);
            return ResponseEntity.status(400)
                    .contentType(MediaType.parseMediaType(contentType))
                    .body(json);
        }

        UploadDatasetResponse udr = new UploadDatasetResponse(fileName, file.getContentType(), file.getSize());

//        return udr;
//        String response = "File: " + fileName + " successfully uploaded";
        String json = gson.toJson(udr);
        logger.info(json);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }

    @CrossOrigin
    @PostMapping("/uploadMultipleFiles")
    public List<UploadDatasetResponse> uploadMultipleFiles(@RequestParam("files") MultipartFile[] files) {
        logger.info("files " + files.toString());
//        return Arrays.asList(files)
//                .stream()
//                .map(file -> uploadFile(file))
//                .collect(Collectors.toList());
        return null;
    }

    @CrossOrigin
    @GetMapping("/datasets")
    public ResponseEntity<String> previewDatasets() {
        List<Dataset> datasets = datasetStorageService.getDatasetPreviews();
        String contentType = "application/json";

        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        DatasetsPreviewResponse datasetsPreviewResponse = new DatasetsPreviewResponse(datasets);
        String json = gson.toJson(datasetsPreviewResponse);
        logger.info(json);

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }

    @CrossOrigin
    @DeleteMapping("/datasets")
    public ResponseEntity<String> deleteDataset(@RequestParam("dataset") String dataset) {

        boolean deletedFromHdfs = false;
        try{
            datasetStorageService.deleteDatasetFromHdfs(dataset);
            deletedFromHdfs = true;
        }catch (DatasetStorageException e){
            e.printStackTrace();
        }

        String contentType = "application/json";
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        if(!deletedFromHdfs){
            String response = "Unable to delete dataset";
            String json = gson.toJson(response);
            logger.info(json);
            return ResponseEntity.status(400)
                    .contentType(MediaType.parseMediaType(contentType))
                    .body(json);
        }

        DeleteResponse deleteResponse = new DeleteResponse(dataset);
        String json = gson.toJson(deleteResponse);
        logger.info(json);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }

}
