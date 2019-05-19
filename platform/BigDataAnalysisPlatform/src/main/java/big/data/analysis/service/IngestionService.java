package big.data.analysis.service;

import big.data.analysis.entity.Ingestion;
import big.data.analysis.entity.IngestionParams;
import big.data.analysis.property.DatasetStorageProperties;
import big.data.analysis.property.IngestionProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

/**
 * Service responsible for managing ingestions
 * @author Peter Lipcak, Masaryk University
 */
@Service
public class IngestionService {

    private static final Logger logger = LoggerFactory.getLogger(IngestionService.class);

    private static final int DENSIFICATION_NONE = 0;
    private static final int DENSIFICATION_MULTIPLICATION = 1;
    private static final int DENSIFICATION_INTERPOLATION = 2;

    private List<Ingestion> executedIngestions = new ArrayList<>();

    private final String hdfsStorageDatasetLocation;
    private final String jarFileLocation;

    @Autowired
    public IngestionService(DatasetStorageProperties datasetStorageProperties, IngestionProperties ingestionProperties) {
        this.hdfsStorageDatasetLocation = datasetStorageProperties.getUploadHdfsDir();
        this.jarFileLocation = ingestionProperties.getJarFile();
    }

    /**
     * Creates process running Ingestion Manager with possible densification
     * @param ingestionParams ingestion parameters
     * @throws IOException
     */
    public void runIngestion(IngestionParams ingestionParams) throws IOException {
        logger.info(ingestionParams.toString());

        //Create process running ingestion manager program
        ProcessBuilder builder = new ProcessBuilder();
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-jar");
        command.add(jarFileLocation);
        command.add("-t");
        command.add(ingestionParams.getTopic());
        command.add("-p");
        command.add(hdfsStorageDatasetLocation + ingestionParams.getDatasetName());
        command.add("-rps");
        command.add(ingestionParams.getRecordsPerSecond().toString());

        switch(ingestionParams.getDensificationType()){
            case DENSIFICATION_NONE:
                command.add("-dt");
                command.add(String.valueOf(DENSIFICATION_NONE));
                break;
            case DENSIFICATION_MULTIPLICATION:
                command.add("-dt");
                command.add(String.valueOf(DENSIFICATION_MULTIPLICATION));
                command.add("-dc");
                command.add(String.valueOf(ingestionParams.getDensificationCount()));
                break;
            case DENSIFICATION_INTERPOLATION:
                command.add("-dt");
                command.add(String.valueOf(DENSIFICATION_INTERPOLATION));
                command.add("-dc");
                command.add(String.valueOf(ingestionParams.getDensificationCount()));
                command.add("-i");
                for(String interpolator : ingestionParams.getInterpolators()){
                    command.add("\"" + interpolator + "\"");
                }
                if(ingestionParams.getInterpolationId() != null && ingestionParams.getInterpolationId().length() > 0){
                    command.add("-id");
                    command.add(ingestionParams.getInterpolationId());
                }
                break;
            default:
                break;
        }

        for(String cmd : command){
            logger.info(cmd);
        }

        //Execute the program
        builder.command(command);
        Process process = builder.start();
        Ingestion ingestion = Ingestion
                .builder()
                .id(UUID.randomUUID().toString())
                .datasetName(ingestionParams.getDatasetName())
                .process(process)
                .startTime(new Date().getTime())
                .ingestionType(ingestionParams.getDensificationType())
                .build();

        //Adds ingestion to the list of submitted ingestions
        executedIngestions.add(ingestion);
    }

    /**
     * Gets information regarding ingestions (e.g. status RUNNING/FAILED/FINISHED, date, dataset name)
     * @return ingestion details
     */
    public List<Ingestion> getIngestionData(){
        List<Ingestion> ingestions = new ArrayList<>();

        for(Ingestion ingestion : executedIngestions){
            if(ingestion.getProcess().isAlive()){
                ingestion.setStatus("RUNNING");
            }else if(ingestion.getProcess().exitValue() == 0){
                logger.info("exit value: " + ingestion.getProcess().exitValue());
                ingestion.setStatus("FINISHED");
            }else {
                logger.info("exit value: " + ingestion.getProcess().exitValue());
                ingestion.setStatus("FAILED");
            }

            ingestions.add(ingestion);
        }

        return ingestions;
    }

    /**
     * Cancels running ingestion
     * @param id of ingestion
     */
    public void cancelIngestion(String id){
        for(Ingestion ingestion : executedIngestions){
            if(ingestion.getId().equals(id)){
                ingestion.getProcess().destroy();
            }
        }
    }



}
