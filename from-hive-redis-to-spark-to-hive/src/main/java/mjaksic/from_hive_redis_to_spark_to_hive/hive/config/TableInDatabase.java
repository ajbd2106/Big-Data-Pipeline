package mjaksic.from_hive_redis_to_spark_to_hive.hive.config;

/**
 * Convenience class. Explains how to look up the table name.
 */
public class TableInDatabase {
    private DatabaseBean database;
    private String table_name;

    /**
     *
     * @param database_name Hive database name. See a link below.
     * @param table_name Hive table name. See a link below.
     * @see DatabaseBean
     * @see "Look up Hive table name in the same way as Hive database name but read out TABLE_NAME column."
     */
    public TableInDatabase(String database_name, String table_name) {
        this.database = new DatabaseBean();
        this.database.setDatabase_name(database_name);
        this.table_name = table_name;
    }

    public String getDatabase_name() {
        return database.getDatabase_name();
    }

    public String getTable_name() {
        return table_name;
    }
}
