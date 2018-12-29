package mjaksic.Kafka_To_Hive.closing;

/**
 * Configures when to close the program.
 */
public class KafkaAndHiveCloserConfiguration {
    public int max_empty;

    public KafkaAndHiveCloserConfiguration(int max_empty) {
        this.max_empty = max_empty;
    }
}
