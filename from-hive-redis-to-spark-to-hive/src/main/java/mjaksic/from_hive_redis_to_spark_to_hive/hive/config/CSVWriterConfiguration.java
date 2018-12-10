package mjaksic.from_hive_redis_to_spark_to_hive.hive.config;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Configures the Hive Delimited Input Writer.
 * @see <a href="https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hive/hcatalog/streaming/DelimitedInputWriter.html">Hive Delimited Input Writer</a>
 */
public class CSVWriterConfiguration {
    public String delimiter;
    public HiveConf custom_configuration;
    public char serde_separator;

    /**
     *
     * @param delimiter Very often ",".
     * @param custom_configuration Can be null.
     * @param serde_separator Very often ",".
     * @see <a href="https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hadoop/hive/conf/HiveConf.html">Hive Conf</a>
     * @see <a href="https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hive/hcatalog/streaming/DelimitedInputWriter.html#DelimitedInputWriter-java.lang.String:A-java.lang.String-org.apache.hive.hcatalog.streaming.HiveEndPoint-org.apache.hadoop.hive.conf.HiveConf-char-org.apache.hive.hcatalog.streaming.StreamingConnection-">serdeSeparator</a>
     */
    public CSVWriterConfiguration(String delimiter, HiveConf custom_configuration, char serde_separator) {
        this.delimiter = delimiter;
        this.custom_configuration = custom_configuration;
        this.serde_separator = serde_separator;
    }
}