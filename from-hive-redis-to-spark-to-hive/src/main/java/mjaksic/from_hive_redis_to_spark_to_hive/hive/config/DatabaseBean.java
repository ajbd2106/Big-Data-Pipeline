package mjaksic.from_hive_redis_to_spark_to_hive.hive.config;

/**
 * Convenience class. Explains how to look up the database name.
 */
public class DatabaseBean {
    private String database_name;


    public DatabaseBean() {

    }

    public String getDatabase_name() {
        return database_name;
    }

    /**
     *
     * @param database_name Hive database name. See a link below.
     * @see "Look up Hive database name in /path/to/hive/bin/beeline by first executing "!connect jdbc:hive2://localhost:10000", then "!tables" and looking up TABLE_SCHEM column."
     * @see <a href="https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-Beeline%E2%80%93CommandLineShell">Hive Beeline Command Line Interface</a>
     */
    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }
}
