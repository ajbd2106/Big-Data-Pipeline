package mjaksic.from_hive_to_kafka.hive.streaming;

import org.apache.hive.hcatalog.streaming.*;

import mjaksic.from_hive_to_kafka.hive.config.MetaStoreClientWrapperConfiguration;
import mjaksic.from_hive_to_kafka.hive.config.StreamingConfiguration;
import mjaksic.from_hive_to_kafka.hive.metastore.MetaStoreClientWrapper;

import java.io.Closeable;
import java.util.List;


/**
 * Streams data into an (unpartitioned) Hive table.
 * Hides how Hive transactions work.
 */
public class Streamer implements Closeable {
    private static final int exception_exit_code = -4;

    private StreamingConfiguration streaming_config;

    private HiveEndPoint end_point;
    private StreamingConnection connection;
    private DelimitedInputWriter csv_writer;
    private TransactionBatch transaction_batch;

    private int writes_per_commit_counter = 0;

    /**
     *
     * @param streaming_config See a link below.
     * @see bigdata.hive.config.StreamingConfiguration
     */
    public Streamer(StreamingConfiguration streaming_config) { //TODO, may need to be a singleton per each Hive table
        SetStreamingConfiguration(streaming_config);

        SetAllStreamingInstances();

        OpenNewTransaction();
    }

    private void SetStreamingConfiguration(StreamingConfiguration streaming_config) {
        this.streaming_config = streaming_config;
    }

    private void SetAllStreamingInstances() {
        SetHiveEndPoint();
        SetStreamingConnection();
        SetCSVWriter();
        SetTransactionBatch();
    }

    private void SetHiveEndPoint() {
        HiveEndPoint end_point = new HiveEndPoint(this.streaming_config.end_point_config.hive_metastore_uri,
                this.streaming_config.end_point_config.table_in_database.getDatabase_name(),
                this.streaming_config.end_point_config.table_in_database.getTable_name(),
                this.streaming_config.end_point_config.partition_values);

        this.end_point = end_point;
        ReportActionWasASuccess("End point creation");
    }

    /**
     * Attempts to create a Streaming Connection.
     */
    private void SetStreamingConnection() {
        try {
            StreamingConnection connection = this.end_point.newConnection(this.streaming_config.streaming_config.is_create_partitions,
                    this.streaming_config.streaming_config.custom_configuration);

            this.connection = connection;
            ReportActionWasASuccess("Streaming Connection creation");

        } catch (ConnectionError | InterruptedException | ImpersonationFailed | PartitionCreationFailed | InvalidTable | InvalidPartition connectionError) {
            connectionError.printStackTrace();
            ShutDownAfterException();
        }
    }

    /**
     * Attempts to create a CSV Writer.
     */
    private void SetCSVWriter() {
        try {
            String[] table_column_names = GetTableColumnNames();
            DelimitedInputWriter csv_writer = new DelimitedInputWriter(table_column_names,
                    this.streaming_config.csv_writer_config.delimiter,
                    this.end_point,
                    this.streaming_config.csv_writer_config.custom_configuration,
                    this.streaming_config.csv_writer_config.serde_separator);

            this.csv_writer = csv_writer;
            ReportActionWasASuccess("CSV writer creation");

        } catch (ClassNotFoundException | StreamingException e) {
            e.printStackTrace();
            CloseStreamingConnection();
            ShutDownAfterException();
        }
    }

    private String[] GetTableColumnNames() {
        MetaStoreClientWrapperConfiguration config = new MetaStoreClientWrapperConfiguration(this.streaming_config.end_point_config.hive_metastore_uri);
        MetaStoreClientWrapper metastore = new MetaStoreClientWrapper(config);

        List<String> table_column_names = metastore.GetNamesOfColumns(this.streaming_config.end_point_config.table_in_database);
        metastore.close();
        
        return TransformToArray(table_column_names);
    }

    private String[] TransformToArray(List<String> strings) {
        String[] array = new String[strings.size()];
        for (int i = 0; i < strings.size(); i++) {
            array[i] = strings.get(i);
        }
        return array;
    }

    /**
     * Attempts to create a Transaction Batch.
     */
    private void SetTransactionBatch() {
        try {
            TransactionBatch transaction_batch = this.connection.fetchTransactionBatch(this.streaming_config.transaction_config.max_commits_per_transaction,
                    this.csv_writer);

            this.transaction_batch = transaction_batch;
            ReportActionWasASuccess("Transaction Batch creation");

        } catch (StreamingException | InterruptedException e) {
            e.printStackTrace();
            CloseStreamingConnection();
            ShutDownAfterException();
        }
    }



    /**
     *
     * @param csv_strings All strings must have values ordered is the same way as they are displayed in the Hive metastore.
     */
    public void TransactMessages(List<String> csv_strings) {
        Record record;
        for (String string : csv_strings) {
            record = new Record(string);
            StreamRecordToHive(record);
        }
    }

    private void StreamRecordToHive(Record record) {
        if (IsWritesPerTransactionReached()) {
            CommitTransaction();
            OpenNewTransaction();
        }
        WriteRecordToHive(record);
    }

    private boolean IsWritesPerTransactionReached() {
        int max_writes_per_commit = this.streaming_config.commit_control_config.max_writes_per_commit;
        if (this.writes_per_commit_counter >= max_writes_per_commit) {
            System.out.println("Writes per transaction limit reached. (" + max_writes_per_commit + ")");
            return true;
        }
        return false;
    }


    private void ResetWriteCounter() {
        this.writes_per_commit_counter = 0;
    }


    private void OpenNewTransaction() {
        if (!IsReachedExpectedNumberOfCommits()) {
            BeginNextTransactionInTransactionBatch();
        } else {
            RefreshTransactionBatch();
            BeginNextTransactionInTransactionBatch();
        }
        ReportActionWasASuccess("Opening a transaction");
    }

    private boolean IsReachedExpectedNumberOfCommits() {
        if (GetRemainingCommits() > 0) {
            System.out.println("You can still perform " + GetRemainingCommits() + " commits.");
            return false;
        }
        System.out.println("Commits per transaction limit reached. (" + this.streaming_config.transaction_config.max_commits_per_transaction + ")");
        return true;
    }

    private int GetRemainingCommits() {
        return this.transaction_batch.remainingTransactions();
    }

    /**
     * Attempts to begin a new transaction in Transaction Batch.
     */
    private void BeginNextTransactionInTransactionBatch() {
        try {
            this.transaction_batch.beginNextTransaction();
        } catch (StreamingException | InterruptedException e) {
            e.printStackTrace();
            StopStreamingAndShutDown();
        }
    }

    private void RefreshTransactionBatch() {
        SetTransactionBatch();
    }


    private void WriteRecordToHive(Record record) {
        WriteToHive(record);
        IncrementWritesPerCommit();
    }

    /**
     * Attempts to write to Hive.
     * @param record Simple record.
     */
    private void WriteToHive(Record record) {
        try {
            this.transaction_batch.write(record.byte_record);
        } catch (StreamingException | InterruptedException e) {
            e.printStackTrace();
            StopStreamingAndShutDown();
        }
    }

    private void IncrementWritesPerCommit() {
        this.writes_per_commit_counter++;
    }



    private void StopStreamingAndShutDown() {
        StopStreaming();
        ShutDownAfterException();
    }

    /**
     * Attempts to stop streaming.
     */
    public void StopStreaming() {
        FlushWriter();
        CommitTransaction();
        CloseAll();
        ReportActionWasASuccess("Hive Streamer shutdown");
    }

    /**
     * Attempts to flush the writer.
     */
    private void FlushWriter() {
        try {
            csv_writer.flush();
        } catch (StreamingIOFailure streamingIOFailure) {
            streamingIOFailure.printStackTrace();
        }
    }

    /**
     * Attempts to commit a transaction.
     */
    private void CommitTransaction() {
        try {
            this.transaction_batch.commit();
            ResetWriteCounter();
        } catch (StreamingException | InterruptedException e) {
            e.printStackTrace();
            CloseAll();
            ShutDownAfterException();
        }
    }



    private void CloseAll() {
        CloseTransaction();
        CloseStreamingConnection();
    }

    /**
     * Attempts to close the Transaction Batch.
     */
    private void CloseTransaction() {
        try {
            this.transaction_batch.close();
        } catch (StreamingException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Closes the Streaming Connection.
     */
    private void CloseStreamingConnection() {
        this.connection.close();
    }


    private void ReportActionWasASuccess(String message) {
        System.out.println(message + " was a success!");
    }

    private void ShutDownAfterException() {
        System.out.println("Shutting down after an exception.");
        System.exit(Streamer.exception_exit_code);
    }


    @Override
    protected void finalize() throws Throwable {
        StopStreaming();
        super.finalize();
    }

    /**
     * You may want to use try-with-resources.
     */
    @Override
    public void close() {
        StopStreaming();
    }
}