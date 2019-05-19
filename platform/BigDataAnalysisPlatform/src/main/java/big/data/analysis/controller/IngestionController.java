package big.data.analysis.controller;


import big.data.analysis.entity.Ingestion;
import big.data.analysis.entity.IngestionParams;
import big.data.analysis.payload.IngestionsResponse;
import big.data.analysis.payload.MessageResponse;
import big.data.analysis.service.IngestionService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

/**
 * Rest controller for ingestion management
 * @author Peter Lipcak, Masaryk University
 */
@RestController
public class IngestionController {

    private static final Logger logger = LoggerFactory.getLogger(IngestionController.class);

    @Autowired
    private IngestionService ingestionService;

    /**
     * Submits the ingestion based on ingestion parameters
     * @param ingestionParams ingestion details (densification, etc.)
     * @return submission result as JSON
     */
    @CrossOrigin
    @PostMapping("/ingestion")
    public ResponseEntity<String> ingestion(@RequestBody IngestionParams ingestionParams) {

        boolean successfullIngestion = false;
        try {
            ingestionService.runIngestion(ingestionParams);
            successfullIngestion = true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        String contentType = "application/json";

        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        if(!successfullIngestion){
            MessageResponse successResponse = new MessageResponse("Unsuccessfull ingestion submission.");
            String json = gson.toJson(successResponse);
            logger.info(json);

            return ResponseEntity.badRequest()
                    .contentType(MediaType.parseMediaType(contentType))
                    .body(json);
        }

        MessageResponse successResponse = new MessageResponse("Successfull ingestion submission.");
        String json = gson.toJson(successResponse);
        logger.info(json);

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }

    /**
     * Cancels running ingestion
     * @param id of ingestion to be canceled
     * @return cancelling result as JSON
     */
    @CrossOrigin
    @DeleteMapping("/ingestion")
    public ResponseEntity<String> cancelIngestion(@RequestParam("id") String id) {
        ingestionService.cancelIngestion(id);

        String contentType = "application/json";
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        MessageResponse messageResponse = new MessageResponse("Ingestion successfully canceled.");
        String json = gson.toJson(messageResponse);
        logger.info(json);

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }


    /**
     * Gets ingestions that had beed submitted
     * @return JSON with ingestions details
     */
    @CrossOrigin
    @GetMapping("/ingestions")
    public ResponseEntity<String> runningIngestion() {

        List<Ingestion> ingestions = ingestionService.getIngestionData();

        String contentType = "application/json";
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        IngestionsResponse successResponse = new IngestionsResponse(ingestions);
        String json = gson.toJson(successResponse);
        logger.info(json);

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }



}
