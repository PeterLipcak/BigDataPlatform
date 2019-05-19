package util;

/**
 * Helper for environment variables with default values
 *
 * @author Peter Lipcak, Masaryk University
 */
public class EnvironmentVariablesHelper {

    public static String getKafkaIpPort() {
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        return kafkaIP != null ? kafkaIP : "localhost:9092";
    }

    public static String getZookeeperIpPort() {
        String zookeeperIP = System.getenv("ZOOKEEPER_IP_PORT");
        return zookeeperIP != null ? zookeeperIP : "localhost:2181";
    }

    public static String getHdfsIpPort() {
        String hdfsIP = System.getenv("HDFS_IP_PORT");
        return hdfsIP != null ? "hdfs://" + hdfsIP : "hdfs://localhost:9000";
    }

    public static String getWebHdfsIpPort() {
        String hdfsIP = System.getenv("WEB_HDFS_IP_PORT");
        return hdfsIP != null ? "webhdfs://" + hdfsIP : "hdfs://localhost:50070";
    }

    public static String getSparkIpPort() {
        String sparkIP = System.getenv("SPARK_IP_PORT");
        return sparkIP != null ? "spark://" + sparkIP : "local";
    }
}
