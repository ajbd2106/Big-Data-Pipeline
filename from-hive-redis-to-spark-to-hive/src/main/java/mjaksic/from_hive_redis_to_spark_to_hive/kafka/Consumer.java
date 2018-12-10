package mjaksic.from_hive_redis_to_spark_to_hive.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.util.*;

import mjaksic.from_hive_redis_to_spark_to_hive.json.Parser;;

/**
 * Reads (consumes) records from the subscribed topics and partitions.
 */
public class Consumer implements Closeable {
    private ConsumerConfiguration config;
    private KafkaConsumer<String, String> consumer;

    /**
     *
     * @param config See a link below.
     * @see bigdata.kafka.ConsumerConfiguration
     */
    public Consumer(ConsumerConfiguration config) {
        SetConsumerConfig(config);

        SetConsumer();
        SubscribeToTopics();
    }

    private void SetConsumerConfig(ConsumerConfiguration config){
        this.config = config;
    }

    private void SetConsumer() {
        Properties consumer_properties = CreateConsumerProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumer_properties);

        this.consumer = consumer;
    }

    private Properties CreateConsumerProperties() {
        Properties consumer_properties = new Properties();
        consumer_properties.put("key.deserializer", this.config.key_deserializer);
        consumer_properties.put("value.deserializer", this.config.value_deserializer);
        consumer_properties.put("bootstrap.servers", this.config.bootstrap_servers);
        consumer_properties.put("group.id", this.config.group_id);

        return consumer_properties;
    }

    private void SubscribeToTopics() {
        this.consumer.subscribe(this.config.topics);
    }



    /**
     *
     * @return JSON data from the subscribed topics and partitions.
     */
    public List<Map<String, String>> GetJSONRecords() {
        ConsumerRecords<String, String> records = GetRecordsFromKafka();
        List<String> values = ExtractValuesFromRecords(records);

        List<Map<String, String>> parsed_records = Parser.FlatJSONToMap(values);

        return parsed_records;
    }

    /**
     *
     * @return Empty map container or a group of records from subscribed topics and partitions.
     * @see <a href="https://kafka.apache.org/11/javadoc/org/apache/kafka/clients/consumer/ConsumerRecord.html">Kafka Consumer Record</a>
     */
    private ConsumerRecords<String, String> GetRecordsFromKafka() {
        ConsumerRecords<String, String> records = this.consumer.poll(this.config.poll_timeout_milliseconds);

        return records;
    }

    /**
     *
     * @param records Records polled from Kafka.
     * @return Values.
     */
    private List<String> ExtractValuesFromRecords(ConsumerRecords<String, String> records){
        ArrayList<String> strings = new ArrayList<>();
        for (ConsumerRecord<String, String> record:records) {
            strings.add(record.value());
        }
        return strings;
    }


    /**
     * You may want to use try-with-resources.
     */
    @Override
    public void close() {
        StopConsumer();
    }

    @Override
    protected void finalize() throws Throwable {
        StopConsumer();
        super.finalize();
    }

    private void StopConsumer() {
        consumer.close();
        System.out.println("Consumer closed gracefully.");
    }
}