package mjaksic.Hive_Redis_To_Spark_To_Hive.sql;

public class SelectConstructor {
    public static final String sql_sentinel = "-1";

    public static String ConstructSQLString(SelectConfiguration config) {
        String sql_string = "SELECT DISTINCT " + config.select + " FROM " + config.from;

        if (!config.where.equals(SelectConstructor.getSql_sentinel())) {
            sql_string = sql_string + " WHERE " + config.where;
        }
        if (!config.group_by.equals(SelectConstructor.getSql_sentinel())) {
            sql_string = sql_string + " GROUP BY " + config.group_by;
        }
        if (!config.order_by.equals(SelectConstructor.getSql_sentinel())) {
            sql_string = sql_string + " ORDER BY " + config.order_by;
        }
        if (!config.limit.equals(SelectConstructor.getSql_sentinel())) {
            sql_string = sql_string + " LIMIT " + config.limit;
        }

        return sql_string;
    }

    public static String getSql_sentinel() {
        return SelectConstructor.sql_sentinel;
    }
}
