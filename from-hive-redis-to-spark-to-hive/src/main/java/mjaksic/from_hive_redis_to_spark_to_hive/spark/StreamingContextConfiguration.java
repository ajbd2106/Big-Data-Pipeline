package mjaksic.from_hive_redis_to_spark_to_hive.spark;

public class StreamingContextConfiguration {
    public String app_name;
    public String master; //-> TODO submit the value through spark-submit
    public long batch_millisecond_interval;

    public StreamingContextConfiguration(String app_name, String master, long batch_millisecond_interval) {
        this.app_name = app_name;
        this.master = master;
        this.batch_millisecond_interval = batch_millisecond_interval;
    }
}
