package mjaksic.from_hive_to_kafka.runners;

import org.apache.kafka.common.serialization.StringDeserializer;

import mjaksic.from_hive_to_kafka.closing.KafkaAndHiveCloser;
import mjaksic.from_hive_to_kafka.closing.KafkaAndHiveCloserConfiguration;
import mjaksic.from_hive_to_kafka.hive.config.CSVWriterConfiguration;
import mjaksic.from_hive_to_kafka.hive.config.CommitControlConfiguration;
import mjaksic.from_hive_to_kafka.hive.config.EndPointConfiguration;
import mjaksic.from_hive_to_kafka.hive.config.StreamingConfiguration;
import mjaksic.from_hive_to_kafka.hive.config.StreamingConnectionConfiguration;
import mjaksic.from_hive_to_kafka.hive.config.TableInDatabase;
import mjaksic.from_hive_to_kafka.hive.config.TransactionBatchConfiguration;
import mjaksic.from_hive_to_kafka.hive.streaming.Parser;
import mjaksic.from_hive_to_kafka.hive.streaming.Streamer;
import mjaksic.from_hive_to_kafka.kafka.Consumer;
import mjaksic.from_hive_to_kafka.kafka.ConsumerConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Creates and executes instances of other classes.
 * Can move around 3 300 000 bytes from Kafka to Hive.
 */
public class FromKafkaToHiveTransporter { //TODO move properties/configs to a file

    public static void main(String[] args) {
        List<String> bootstrap_servers = new ArrayList<>();
        bootstrap_servers.add("999.999.99.99:9094");
        List<String> topics = new ArrayList<>();
        topics.add("alarms");
        ConsumerConfiguration consumer_config = new ConsumerConfiguration(StringDeserializer.class.getName(),
                StringDeserializer.class.getName(),
                bootstrap_servers,
                "hive_consumers",
                topics,
                2000);



        TableInDatabase table_in_database = new TableInDatabase("default", "unpartitioned_alarms_raw");
        EndPointConfiguration end_point_config = new EndPointConfiguration("thrift://sandbox-hdp.hortonworks.com:9083",
                table_in_database,
                null);
        StreamingConnectionConfiguration streaming_connection_config = new StreamingConnectionConfiguration(true,
                null);
        CSVWriterConfiguration csv_writer_config = new CSVWriterConfiguration(",",
                null,
                ',');
        TransactionBatchConfiguration transaction_batch_config = new TransactionBatchConfiguration(200);
        CommitControlConfiguration commit_control_config = new CommitControlConfiguration(10000);
        StreamingConfiguration hive_streaming_config = new StreamingConfiguration(end_point_config,
                streaming_connection_config,
                csv_writer_config,
                transaction_batch_config,
                commit_control_config);



        KafkaAndHiveCloserConfiguration stopper_config = new KafkaAndHiveCloserConfiguration(10);



        try (Consumer consumer = new Consumer(consumer_config)) {

            try (Streamer hive_streamer = new Streamer(hive_streaming_config);) {
                Parser hive_alarms_parser = new Parser();
                KafkaAndHiveCloser stopper = new KafkaAndHiveCloser(stopper_config);

                while(true) {
                    List<Map<String, String>> records = consumer.GetJSONRecords();

                    for (Map<String, String> map:records) {
                        List<String> messages = hive_alarms_parser.AlarmsRawToCSV(map);
                        hive_streamer.TransactMessages(messages);
                    }

                    if (stopper.IsStoppingConditionReached(records)) {
                        break;
                    }
                }
            }
        }
    }


}