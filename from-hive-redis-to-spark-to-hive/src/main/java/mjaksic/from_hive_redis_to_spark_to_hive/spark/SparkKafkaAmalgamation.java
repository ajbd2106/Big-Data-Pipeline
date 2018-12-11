package mjaksic.from_hive_redis_to_spark_to_hive.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.HashMap;
import java.util.Map;

/**
 * Connects to Kafka, consumes data and sends it thought to Spark streaming. An experimental class.
 */
public class SparkKafkaAmalgamation {
    private StreamingContextConfiguration spark_streaming_config = null;
    private ConsumerConfiguration kafka_consumer_config = null;

    private JavaStreamingContext streaming_context = null;
    private JavaInputDStream<ConsumerRecord<String, String>> kafka_dstream = null;

    public SparkKafkaAmalgamation(StreamingContextConfiguration spark_streaming_config, ConsumerConfiguration kafka_consumer_config) {
        SetConfig(spark_streaming_config, kafka_consumer_config);

        SetStreamingContext();
        SetKafkaDStream();

    }

    private void SetConfig(StreamingContextConfiguration spark_streaming_config, ConsumerConfiguration kafka_consumer_config){
        this.spark_streaming_config = spark_streaming_config;
        this.kafka_consumer_config = kafka_consumer_config;
    }

    private void SetStreamingContext(){
        SparkConf config = new SparkConf().setAppName(this.spark_streaming_config.app_name).setMaster(this.spark_streaming_config.master);
        JavaStreamingContext streaming_context = new JavaStreamingContext(config, new Duration(this.spark_streaming_config.batch_millisecond_interval));

        this.streaming_context = streaming_context;
    }

    private void SetKafkaDStream(){
        Map<String, Object> consumer_properties = CreateKafkaConsumerProperties();
        JavaInputDStream<ConsumerRecord<String, String>> kafka_dstream = KafkaUtils.createDirectStream(this.streaming_context, //-> Java friendly input stream; has start/stop
                LocationStrategies.PreferConsistent(), //use LocationStrategies.PreferConsistent() in most cases
                ConsumerStrategies.Subscribe(this.kafka_consumer_config.topics, consumer_properties)); //-> preferred way of subscribing; this is a static way

        this.kafka_dstream = kafka_dstream;
    }

    private Map<String, Object> CreateKafkaConsumerProperties() {
        Map<String, Object> consumer_properties = new HashMap<>();
        consumer_properties.put("auto.offset.reset", this.kafka_consumer_config.auto_offset_reset);
        consumer_properties.put("bootstrap.servers", this.kafka_consumer_config.bootstrap_servers);
        consumer_properties.put("group.id", this.kafka_consumer_config.group_id);
        consumer_properties.put("enable.auto.commit", this.kafka_consumer_config.enable_auto_commit);
        consumer_properties.put("key.deserializer", this.kafka_consumer_config.key_deserializer);
        consumer_properties.put("value.deserializer", this.kafka_consumer_config.value_deserializer);

        return consumer_properties;
    }


    public void Run() throws InterruptedException {
        ApplyRulesToStream();
        streaming_context.start();
        streaming_context.awaitTermination();
        System.out.println("Spark terminated");
    }

    private void ApplyRulesToStream() {

        kafka_dstream.map(record -> record.value()).print();

    }
}
