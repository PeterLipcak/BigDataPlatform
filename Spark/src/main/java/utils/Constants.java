package utils;

public final class Constants {

    private Constants() {
        // restrict instantiation
    }

    public static final String TIMER_TOPIC = "timer";
    public static final String METRICS_DATA_TOPIC = "consumptions";
    public static final String ANOMALIES_DATA_TOPIC = "anomalies";

    public static final String ANOMALIES_TABLE_NAME = "anomalies";
    public static final String CONSUMPTIONS_TABLE_NAME = "consumptions";
    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

}