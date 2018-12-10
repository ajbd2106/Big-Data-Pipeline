package mjaksic.from_hive_redis_to_spark_to_hive.closing;

/**
 * Configures when a program should be closed gracefully.
 */
public class KafkaAndHiveCloserConfiguration {
    public int max_empty;

    public KafkaAndHiveCloserConfiguration(int max_empty) {
        this.max_empty = max_empty;
    }
}
