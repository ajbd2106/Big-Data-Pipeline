package mjaksic.Kafka_To_Hive.hive.config;

/**
 * Configures the Hive Transaction Batch.
 * @see <a href="https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hive/hcatalog/streaming/TransactionBatch.html">Hive Transaction Batch</a>
 */
public class TransactionBatchConfiguration{
    public int max_commits_per_transaction;

    public TransactionBatchConfiguration(int max_commits_per_transaction) {
        this.max_commits_per_transaction = max_commits_per_transaction;
    }
}