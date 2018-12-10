package mjaksic.from_hive_redis_to_spark_to_hive.hive.config;

import java.util.List;

/**
 * Configures the Hive End Point.
 * @see <a href="https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hive/hcatalog/streaming/HiveEndPoint.html">Hive End Point</a>
 */
public class EndPointConfiguration {
    public String hive_metastore_uri;
    public TableInDatabase table_in_database;
    public List<String> partition_values;

    /**
     *
     * @param hive_metastore_uri Hive metastore URI. See a link below.
     * @param table_in_database Table in a database name. See a link below.
     * @param partition_values Can be null. See a link below.
     * @see "Look up hive.metastore.uris in /path/to/hive/.../hive-site.xml"
     * @see TableInDatabase
     * @see <a href="https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest#StreamingDataIngest-Example%E2%80%93Non-secureMode">Example of partition value use; Hive Version < 3.0.0</a>
     * @see <a href="https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest+V2#StreamingDataIngestV2-Example">Example of partition value use; Hive Version >= 3.0.0</a>
     */
    public EndPointConfiguration(String hive_metastore_uri, TableInDatabase table_in_database, List<String> partition_values){
        this.hive_metastore_uri = hive_metastore_uri;
        this.table_in_database = table_in_database;
        this.partition_values = partition_values;
    }
}