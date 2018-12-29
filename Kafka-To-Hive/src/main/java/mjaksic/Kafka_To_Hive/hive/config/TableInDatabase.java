package mjaksic.Kafka_To_Hive.hive.config;

/**
 * Convenience class. It details how to look up a table name.
 */
public class TableInDatabase {
    private DatabaseBean database;
    private String table_name;

    /**
     *
     * @param database_name A Hive database name. See the link below.
     * @param table_name A Hive table name. See the link below.
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
