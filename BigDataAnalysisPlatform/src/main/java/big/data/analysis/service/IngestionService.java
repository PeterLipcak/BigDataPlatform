package big.data.analysis.service;

import big.data.analysis.entity.Ingestion;
import big.data.analysis.entity.IngestionParams;
import big.data.analysis.exception.DatasetStorageException;
import big.data.analysis.property.DatasetStorageProperties;
import big.data.analysis.property.IngestionProperties;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Service
public class IngestionService {

    private static final Logger logger = LoggerFactory.getLogger(IngestionService.class);

    private List<Ingestion> executedIngestions = new ArrayList<>();

    private final String hdfsStorageDatasetLocation;
    private final String jarFileLocation;

    @Autowired
    public IngestionService(DatasetStorageProperties datasetStorageProperties, IngestionProperties ingestionProperties) {
        this.hdfsStorageDatasetLocation = datasetStorageProperties.getUploadHdfsDir();
        this.jarFileLocation = ingestionProperties.getJarFile();
    }


    public void runIngestion(IngestionParams ingestionParams) throws IOException {
        ProcessBuilder builder = new ProcessBuilder();
        switch(ingestionParams.getDensificationType()){
            case 1: {
                builder.command("java"
                        ,"-cp"
                        ,jarFileLocation
                        ,"Main"
                        ,ingestionParams.getTopic()
                        ,hdfsStorageDatasetLocation + ingestionParams.getDatasetName()
                        ,ingestionParams.getRecordsPerSecond().toString()
                );
                Process process = builder.start();
                Ingestion ingestion = Ingestion.builder().datasetName(ingestionParams.getDatasetName()).process(process).startTime(new DateTime()).build();
                executedIngestions.add(ingestion);
                break;
            }
            case 2: {
                builder.command("java"
                        ,"-cp"
                        ,jarFileLocation
                        ,"Main"
                        ,ingestionParams.getTopic()
                        ,hdfsStorageDatasetLocation + ingestionParams.getDatasetName()
                        ,ingestionParams.getRecordsPerSecond().toString()
                        ,ingestionParams.getMultiplicationCount().toString()
                );
                logger.info(builder.toString());
                Process process = builder.start();
                Ingestion ingestion = Ingestion.builder().datasetName(ingestionParams.getDatasetName()).process(process).startTime(new DateTime()).build();
                executedIngestions.add(ingestion);
                break;
            }
        }
    }

    public List<Ingestion> getIngestionData(){
        List<Ingestion> ingestions = new ArrayList<>();

        for(Ingestion ingestion : executedIngestions){
            if(ingestion.getProcess().isAlive()){
                ingestion.setStatus("RUNNING");
            }else{
                ingestion.setStatus("FINISHED");
            }

            ingestions.add(ingestion);
        }

        return ingestions;
    }



}
