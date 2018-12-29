package mjaksic.Kafka_To_Hive.hive.config;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Configures the Hive Streaming Connection.
 * @see <a href="https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hive/hcatalog/streaming/StreamingConnection.html">Hive Streaming Connection</a>
 */
public class StreamingConnectionConfiguration {
    public boolean is_create_partitions;
    public HiveConf custom_configuration;

    /**
     *
     * @param is_create_partitions Is Hive allowed to create partitions if they don't already exist? Very often a "true".
     * @param custom_configuration Can be null. See the link below.
     * @see <a href="https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hadoop/hive/conf/HiveConf.html">Hive Conf</a>
     */
    public StreamingConnectionConfiguration(boolean is_create_partitions, HiveConf custom_configuration) {
        this.is_create_partitions = is_create_partitions;
        this.custom_configuration = custom_configuration;
    }
}