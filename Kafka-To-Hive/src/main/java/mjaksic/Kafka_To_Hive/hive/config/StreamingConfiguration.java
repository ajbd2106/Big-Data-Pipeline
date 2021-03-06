package mjaksic.Kafka_To_Hive.hive.config;

/**
 * Wraps up configuration classes required for streaming data into Apache Hive.
 */
public class StreamingConfiguration {
    public EndPointConfiguration end_point_config;
    public StreamingConnectionConfiguration streaming_config;
    public CSVWriterConfiguration csv_writer_config;
    public TransactionBatchConfiguration transaction_config;
    public CommitControlConfiguration commit_control_config;

    /**
     *
     * @param end_point_config See the link below.
     * @param streaming_config See the link below.
     * @param csv_writer_config See the link below.
     * @param transaction_config See the link below.
     * @param commit_control_config See the link below.
     * @see EndPointConfiguration
     * @see StreamingConnectionConfiguration
     * @see CSVWriterConfiguration
     * @see TransactionBatchConfiguration
     * @see CommitControlConfiguration
     */
    public StreamingConfiguration(EndPointConfiguration end_point_config, StreamingConnectionConfiguration streaming_config, CSVWriterConfiguration csv_writer_config, TransactionBatchConfiguration transaction_config, CommitControlConfiguration commit_control_config) {
        this.end_point_config = end_point_config;
        this.streaming_config = streaming_config;
        this.csv_writer_config = csv_writer_config;
        this.transaction_config = transaction_config;
        this.commit_control_config = commit_control_config;
    }
}