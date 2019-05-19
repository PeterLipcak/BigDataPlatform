package big.data.analysis.ingestion;

import big.data.analysis.entity.IngestionResult;
import big.data.analysis.utils.EnvironmentVariablesHelper;
import big.data.analysis.utils.KafkaHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

//NOT WORKING
public class IngestionCallable implements Callable<IngestionResult> {

    private String kafkaUri;
    private String zookeeperUri;
    private String hdfsUri;
    private String topic;
    private int limitRecordsPerSecond;
    private Path path;
    private Map<String,String> lastRecords = new HashMap<>();
    private int recordsSent = 0;
    private int allRecordsSent = 0;
    private int densificationCount = 1;
    private DateTime lastTimestamp;
    private DensificationType densificationType;

    public IngestionCallable(String topic, Path hdfsPath, DensificationType densificationType, Integer densificationCount){
        this.topic = topic;
        this.path = hdfsPath;
        this.densificationType = densificationType;
        this.densificationCount = densificationCount;
        initEnvironmentVariables();
    }

    @Override
    public IngestionResult call() throws Exception {
        return null;
    }

    private void readAndSendFileToKafka(FileSystem fs, Path path) throws IOException, InterruptedException {
        InputStream is = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Producer<String, String> producer = KafkaHelper.getKafkaProducer();

        String line;
        while ((line = br.readLine()) != null) {
            regulateSpeed();

            IDensificator densificator = new MultiplierDensificator(densificationCount);
            String[] recordSplits = line.split(",");
            String lastRecord = lastRecords.get(recordSplits[0]);
            lastRecords.put(recordSplits[0],line);

            List<String> recordsToIngest = densificator.densify(lastRecord,line);

            for(String record : recordsToIngest){
                producer.send(new ProducerRecord<>(topic,Long.toString(System.currentTimeMillis()), record));
                recordsSent++;
            }
        }
    }

    private void regulateSpeed() throws InterruptedException {
        if(recordsSent>limitRecordsPerSecond){
            DateTime newTimestamp = new DateTime();
            Duration duration = new Duration(lastTimestamp,newTimestamp);

            if(duration.getStandardSeconds()<1){
                Thread.sleep((1000L-duration.getMillis()) * 2);
            }

            lastTimestamp = newTimestamp;
            allRecordsSent += recordsSent;
            recordsSent = 0;

            System.out.println(allRecordsSent);
        }
    }

    public void initEnvironmentVariables(){
        kafkaUri = EnvironmentVariablesHelper.getKafkaIpPort();
        zookeeperUri = EnvironmentVariablesHelper.getZookeeperIpPort();
        hdfsUri = EnvironmentVariablesHelper.getHdfsIpPort();
    }

}
