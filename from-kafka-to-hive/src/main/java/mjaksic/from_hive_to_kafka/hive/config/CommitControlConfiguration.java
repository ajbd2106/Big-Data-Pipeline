package mjaksic.from_hive_to_kafka.hive.config;

/**
 * Configures when data should be committed to the database.
 */
public class CommitControlConfiguration {
    public int max_writes_per_commit;

    public CommitControlConfiguration(int max_writes_per_commit) {
        this.max_writes_per_commit = max_writes_per_commit;
    }
}