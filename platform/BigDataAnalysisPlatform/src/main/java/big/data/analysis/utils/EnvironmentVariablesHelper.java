package big.data.analysis.utils;

public class EnvironmentVariablesHelper {

    public static String get(String envVariable){
        return System.getenv(envVariable);
    }

    public static String getHdfsIpPort(){
        String hdfsIP = System.getenv("HDFS_IP_PORT");
        return hdfsIP != null ? "hdfs://" + hdfsIP : "hdfs://localhost:9000";
    }

    public static String getKafkaIpPort(){
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        return kafkaIP != null ? kafkaIP : "localhost:9092";
    }

    public static String getZookeeperIpPort(){
        String zookeeperIP = System.getenv("ZOOKEEPER_IP_PORT");
        return zookeeperIP != null ? zookeeperIP : "localhost:2181";
    }
}
