package mjaksic.from_kafka_to_hive.runners;

import mjaksic.from_kafka_to_hive.spark.SparkHiveAmalgamation;
import mjaksic.from_kafka_to_hive.spark.SparkHiveSessionConfiguration;

/**
 * Creates and executes instances of other classes.
 */
public class SparkPlayground {

    public static void main(String[] args) {
        /*SparkStreamingContextConfiguration spark_stream_config = new SparkStreamingContextConfiguration("name_of_the_cluster",
                "local[2]",
                1000);

        ArrayList<String> bootstrap_servers = new ArrayList<>();
        bootstrap_servers.add("999.999.99.99:9094");
        ArrayList<String> topics = new ArrayList<>();
        topics.add("alarms");
        KafkaDataConsumerConfiguration consumer_config = new KafkaDataConsumerConfiguration(StringDeserializer.class.getName(),
                StringDeserializer.class.getName(),
                bootstrap_servers,
                "spark_playground",
                topics,
                1000,
                "earliest",
                "false");

        SparkKafka apache_spark = new SparkKafka(spark_stream_config, consumer_config);
        apache_spark.Run();*/



        SparkHiveSessionConfiguration spark_hive_config = new SparkHiveSessionConfiguration("local[2]",
                "my_little_spark_hive_app",
                "/apps/hive/warehouse",
                "thrift://sandbox-hdp.hortonworks.com:9083",
                "nonstrict");



        try (SparkHiveAmalgamation spark_hive = new SparkHiveAmalgamation(spark_hive_config);) {
            spark_hive.Run();
        }
    }
}
