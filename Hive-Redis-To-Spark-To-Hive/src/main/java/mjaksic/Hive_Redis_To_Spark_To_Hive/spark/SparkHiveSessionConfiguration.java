package mjaksic.Hive_Redis_To_Spark_To_Hive.spark;

/**
 * Configures Spark Session with Hive support.
 */
public class SparkHiveSessionConfiguration {
    /* Spark config */
    public String master;
    public String app_name;
    /* Hive config */
    public String spark_sql_warehouse_dir;
    public String hive_metastore_uris;
    public String hive_exec_dynamic_partition_mode;

    /**
     *
     * @param master See the link below.
     * @param app_name Application name.
     * @param spark_sql_warehouse_dir Location of the Hive metastore. See the link below.
     * @param hive_metastore_uris  See the link below.
     * @param hive_exec_dynamic_partition_mode "nonstrict" if you want to insert data into Hive.
     * @see <a href="https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/SparkSession.html">Spark Session</a>
     * @see <a href="https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/SparkSession.Builder.html#master-java.lang.String-">Master</a>
     * @see "Look up spark_sql_warehouse_dir under hive.metastore.warehouse.dir in /path/to/hive/.../hive-site.xml"
     * @see "Look up hive.metastore.uris in /path/to/hive/.../hive-site.xml"
     */
    public SparkHiveSessionConfiguration(String master, String app_name, String spark_sql_warehouse_dir, String hive_metastore_uris, String hive_exec_dynamic_partition_mode) {
        this.master = master;
        this.app_name = app_name;
        this.spark_sql_warehouse_dir = spark_sql_warehouse_dir;
        this.hive_metastore_uris = hive_metastore_uris;
        this.hive_exec_dynamic_partition_mode = hive_exec_dynamic_partition_mode;
    }
}
