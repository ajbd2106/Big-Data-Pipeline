package mjaksic.from_hive_to_kafka.closing;

/**
 * Configures when a program should be closed gracefully.
 */
public class KafkaAndHiveCloserConfiguration {
    public int max_empty;

    public KafkaAndHiveCloserConfiguration(int max_empty) {
        this.max_empty = max_empty;
    }
}
