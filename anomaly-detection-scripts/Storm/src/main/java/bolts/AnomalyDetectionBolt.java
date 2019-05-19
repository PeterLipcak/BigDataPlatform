package bolts;

import entities.Consumption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import util.EnvironmentVariablesHelper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;


/**
 * Implementation of bolt that filters anomalies
 *
 * @author Peter Lipcak, Masaryk University
 */
public class AnomalyDetectionBolt extends BaseBasicBolt {

    private Map<String, Double> expectedConsumptions;
    private String hdfsUri;

    /**
     * Loads prediction model from HDFS and creates a map of composite ids and expected consumptions
     * @param stormConf Storm variable - not used
     * @param context Storm variable - not used
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            //Set up hdfs configuration
            hdfsUri = EnvironmentVariablesHelper.getHdfsIpPort();
            Configuration hdfsConf = new Configuration();
            hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
            hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
            hdfsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            hdfsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            hdfsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
            //Get file system of hdfs
            FileSystem fs = FileSystem.get(new URI(hdfsUri + "/datasets/predictions.csv"), hdfsConf);
            //File or directory is accepted
            FileStatus[] fileStatus = fs.listStatus(new Path(hdfsUri + "/datasets/predictions.csv"));

            //Iterate over files in directory or file and append expected consumptions to hashmap
            final Map<String, Double> expectedConsumptions = new HashMap<>();
            for (FileStatus status : fileStatus) {
                InputStream is = fs.open(status.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    try {
                        String[] splits = line.split(",");
                        expectedConsumptions.put(splits[0], Double.parseDouble(splits[1]));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }

            this.expectedConsumptions = expectedConsumptions;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method is executed for each incoming record.
     * It compares consumption values with expeted values
     * and emits anomalies when the value exceeds the expected one
     * @param tuple incoming record
     * @param collector emits anomalies
     */
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            Consumption consumption = new Consumption(tuple.getStringByField("value"));
            Double expectedConsumption = expectedConsumptions.get(consumption.getCompositeId());

            //Comapre consumption value with expeted value and emit if exceeded
            if (expectedConsumption != null && expectedConsumption < consumption.getConsumption()) {
                collector.emit(new Values(consumption.getCompositeId(), consumption.getCompositeId()+ ", consumption=" + consumption + " expected=" + expectedConsumption));
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));
    }
}
