package mjaksic.Kafka_To_Hive.hive.config;

/**
 * Configures when to commit data to the database.
 */
public class CommitControlConfiguration {
    public int max_writes_per_commit;

    public CommitControlConfiguration(int max_writes_per_commit) {
        this.max_writes_per_commit = max_writes_per_commit;
    }
}