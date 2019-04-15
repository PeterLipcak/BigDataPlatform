package big.data.analysis.controller;

import big.data.analysis.entity.Dataset;
import big.data.analysis.entity.JavaProject;
import big.data.analysis.exception.CompilationException;
import big.data.analysis.exception.DatasetStorageException;
import big.data.analysis.exception.FlinkSubmissionException;
import big.data.analysis.payload.*;
import big.data.analysis.service.DatasetsStorageService;
import big.data.analysis.service.ProcessingCodeService;
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
public class ProcessingCodeController {

    private static final Logger logger = LoggerFactory.getLogger(ProcessingCodeController.class);

    @Autowired
    private ProcessingCodeService processingCodeService;

    @CrossOrigin
    @PostMapping("/saveCode")
    public ResponseEntity saveCode(@RequestBody JavaProject javaProject) {
        boolean saved = false;
        try {
            processingCodeService.saveCode(javaProject);
            saved = true;
        } catch (IOException e) {
            e.printStackTrace();
        }


        String contentType = "application/json";
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        if(!saved){
            MessageResponse messageResponse = new MessageResponse("Failed to save project.");
            String json = gson.toJson(messageResponse);
            logger.info(json);
            return ResponseEntity.badRequest()
                    .contentType(MediaType.parseMediaType(contentType))
                    .body(json);
        }

        MessageResponse messageResponse = new MessageResponse("Project successfully saved.");

        String json = gson.toJson(messageResponse);
        logger.info(json);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }

    @CrossOrigin
    @PostMapping("/compileCode")
    public ResponseEntity compileCode(@RequestBody JavaProject javaProject) {
        boolean compiled = false;
        String compilationOutput = null;
        try {
            compilationOutput = processingCodeService.compileCode(javaProject,false);
            compiled = true;
        } catch (IOException e) {
            e.printStackTrace();
            return getMessageResponseEntity("Server error", false);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return getMessageResponseEntity("Server error", false);
        } catch (CompilationException e) {
            e.printStackTrace();
            compilationOutput = e.getMessage();
        }


        String contentType = "application/json";
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        CompilationResponse compilationResponse = CompilationResponse.builder().success(compiled).compilationOutput(compilationOutput).build();

        if(!compiled){
            String json = gson.toJson(compilationResponse);
            logger.info(json);
            return ResponseEntity.badRequest()
                    .contentType(MediaType.parseMediaType(contentType))
                    .body(json);
        }

        String json = gson.toJson(compilationResponse);
        logger.info(json);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }


    @CrossOrigin
    @PostMapping("/submitCode")
    public ResponseEntity submitCode(@RequestBody JavaProject javaProject) {
        boolean submitted = false;
        boolean compiled = false;
        String submissionOutput = null;

        CompilationResponse compilationResponse = null;

        try {
            submissionOutput = processingCodeService.submitCode(javaProject);
            submitted = true;
            compiled = true;
        } catch (IOException e) {
            e.printStackTrace();
            return getMessageResponseEntity("Server error", false);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return getMessageResponseEntity("Server error", false);
        } catch (CompilationException e) {
            e.printStackTrace();
            compilationResponse = CompilationResponse.builder().success(false).compilationOutput(e.getMessage()).build();
        } catch (FlinkSubmissionException e) {
            e.printStackTrace();
            submissionOutput = e.getMessage();
            compiled = true;
        }

        String contentType = "application/json";
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();

        SubmissionResponse submissionResponse = SubmissionResponse
                .builder()
                .compilationResult(compilationResponse)
                .submissionOutput(submissionOutput)
                .compilationSuccess(compiled)
                .submissionSuccess(submitted)
                .build();

        String json = gson.toJson(submissionResponse);
        logger.info(json);

        if(!compiled || !submitted){
            return ResponseEntity.badRequest()
                    .contentType(MediaType.parseMediaType(contentType))
                    .body(json);
        }

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .body(json);
    }



    public ResponseEntity getMessageResponseEntity(String message, boolean success){
        String contentType = "application/json";
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();
        String json = gson.toJson(message);


        if(success){
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType(contentType))
                    .body(json);
        }else{
            return ResponseEntity.badRequest()
                    .contentType(MediaType.parseMediaType(contentType))
                    .body(json);
        }

    }

}
