package mjaksic.from_hive_redis_to_spark_to_hive.kafka;

import java.util.List;

/**
 * Configures the Kafka Consumer.
 * @see <a href="http://kafka.apache.org/documentation/#consumerconfigs">Kafka Consumer Config</a>
 */
public class ConsumerConfiguration {
    /* Required or encouraged */
    public String key_deserializer;
    public String value_deserializer;
    public List<String> bootstrap_servers;
    public String group_id;

    public List<String> topics;
    public long poll_timeout_milliseconds;

    /* Optional */
    public String auto_offset_reset = "latest";
    public String enable_auto_commit = "true";

    public ConsumerConfiguration(String key_deserializer, String value_deserializer, List<String> bootstrap_servers, String group_id, List<String> topics, long poll_timeout) {
        this.key_deserializer = key_deserializer;
        this.value_deserializer = value_deserializer;
        this.bootstrap_servers = bootstrap_servers;
        this.group_id = group_id;
        this.topics = topics;
        this.poll_timeout_milliseconds = poll_timeout;
    }

    public ConsumerConfiguration(String key_deserializer, String value_deserializer, List<String> bootstrap_servers, String group_id, List<String> topics, long poll_timeout_miliseconds, String auto_offset_reset, String enable_auto_commit) {
        this.key_deserializer = key_deserializer;
        this.value_deserializer = value_deserializer;
        this.bootstrap_servers = bootstrap_servers;
        this.group_id = group_id;
        this.topics = topics;
        this.poll_timeout_milliseconds = poll_timeout_miliseconds;
        this.auto_offset_reset = auto_offset_reset;
        this.enable_auto_commit = enable_auto_commit;
    }
}